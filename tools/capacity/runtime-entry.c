#define _GNU_SOURCE
#include <sys/mount.h>
#include <sys/prctl.h>
#include <sys/syscall.h>
#include <sys/statvfs.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <signal.h>
#include <linux/capability.h>
#include <linux/securebits.h>
#include <linux/seccomp.h>
#include <linux/filter.h>
#include <linux/audit.h>
#include <sched.h>
#include <fcntl.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#ifndef HARBOR_RUNTIME_PYTHON
#error Fixed Python interpreter is required
#endif
#ifndef HARBOR_RUNTIME_HELPER
#error Fixed extraction helper is required
#endif

static int text(const char *path, const char *value) {
  int fd = open(path, O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
  if (fd < 0) return -1;
  size_t size = strlen(value);
  int result = write(fd, value, size) == (ssize_t)size ? 0 : -1;
  close(fd); return result;
}

/* The filter is inherited by Node threads and subprocesses. New user
 * namespaces cannot recover a mount/chroot/setns escape after capabilities
 * are dropped; ordinary clone/thread creation remains available. */
static int seal(void) {
#if !defined(__x86_64__)
  return -1;
#else
  struct statvfs fs;
  if (statvfs("/", &fs) || !(fs.f_flag & ST_RDONLY)) return -1;
  for (int cap = 0; cap < 64; cap++)
    if (prctl(PR_CAPBSET_DROP, cap, 0, 0, 0) && errno != EINVAL) return -1;
  if (prctl(PR_SET_SECUREBITS, SECBIT_NOROOT | SECBIT_NOROOT_LOCKED |
      SECBIT_NO_SETUID_FIXUP | SECBIT_NO_SETUID_FIXUP_LOCKED, 0, 0, 0)) return -1;
  struct __user_cap_header_struct header = {_LINUX_CAPABILITY_VERSION_3, 0};
  struct __user_cap_data_struct capabilities[2] = {{0}, {0}};
  if (syscall(SYS_capset, &header, capabilities) ||
      prctl(PR_SET_NO_NEW_PRIVS, 1, 0, 0, 0)) return -1;
#define DENY(number) BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, number, 0, 1), \
                     BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ERRNO | EPERM)
  struct sock_filter instructions[] = {
    BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, arch)),
    BPF_JUMP(BPF_JMP | BPF_JEQ | BPF_K, AUDIT_ARCH_X86_64, 1, 0),
    BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
    BPF_STMT(BPF_LD | BPF_W | BPF_ABS, offsetof(struct seccomp_data, nr)),
    BPF_JUMP(BPF_JMP | BPF_JGE | BPF_K, 0x40000000U, 0, 1),
    BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_KILL_PROCESS),
    DENY(428), DENY(429), DENY(430), DENY(431), DENY(432), DENY(433), DENY(442),
    DENY(SYS_mount), DENY(SYS_umount2), DENY(SYS_pivot_root), DENY(SYS_chroot),
    DENY(SYS_unshare), DENY(SYS_setns), DENY(SYS_ptrace),
    DENY(SYS_open_by_handle_at), DENY(SYS_process_vm_writev),
    DENY(SYS_bpf), DENY(SYS_keyctl),
    BPF_STMT(BPF_RET | BPF_K, SECCOMP_RET_ALLOW)
  };
#undef DENY
  struct sock_fprog program = {(unsigned short)(sizeof(instructions) /
                                 sizeof(instructions[0])), instructions};
  return prctl(PR_SET_SECCOMP, SECCOMP_MODE_FILTER, &program);
#endif
}

/* Fixed root input only. FD7 is reserved so a caller cannot inject a grant;
 * FD5/6 remain available to existing diagnostic checkpoint wrappers. */
static int root_public_origin_grant(void) {
  close(7);
  int directory = open("/", O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
  if (directory < 0) return -1;
  const char *parts[] = {"etc", "harbor-capacity"};
  struct stat info;
  for (unsigned int at = 0; at < 2; at++) {
    if (fstat(directory, &info) || info.st_uid != 0 || (info.st_mode & 0022)) {
      close(directory); return -1;
    }
    int next = openat(directory, parts[at], O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    close(directory);
    if (next < 0) return errno == ENOENT ? 0 : -1;
    directory = next;
  }
  if (fstat(directory, &info) || info.st_uid != 0 || (info.st_mode & 0022)) {
    close(directory); return -1;
  }
  int file = openat(directory, "runtime.json", O_RDONLY | O_CLOEXEC | O_NOFOLLOW | O_NONBLOCK);
  close(directory);
  if (file < 0) return errno == ENOENT ? 0 : -1;
  if (fstat(file, &info) || !S_ISREG(info.st_mode) || info.st_uid != 0 ||
      (info.st_mode & 0022) || info.st_size < 1 || info.st_size > 65536) {
    close(file); return -1;
  }
  if (file != 7) { if (dup2(file, 7) < 0) { close(file); return -1; } close(file); }
  if (fcntl(7, F_SETFD, 0)) { close(7); return -1; }
  return 0;
}

static volatile sig_atomic_t broker_signal = 0;
static void broker_stop(int signal_number) { broker_signal = signal_number; }

int main(int argc, char **argv) {
  unsetenv("LD_PRELOAD"); unsetenv("LD_LIBRARY_PATH");
  unsetenv("PYTHONPATH"); unsetenv("PYTHONHOME");
  unsetenv("NODE_OPTIONS"); unsetenv("NODE_PATH");
  int restored = argc == 3 && !strcmp(argv[1], "--execute") &&
                 !strcmp(argv[2], "--restored-report");
  int broker = argc == 3 && !strcmp(argv[1], "--execute") &&
               !strcmp(argv[2], "--remote-broker");
  if ((argc == 2 && !strcmp(argv[1], "--execute")) || restored || broker) {
    if (seal() || chdir("/app")) return 126;
#ifdef SYS_close_range
    if (syscall(SYS_close_range, 4U, ~0U, 0U)) return 126;
#else
    return 126;
#endif
    if (broker) {
      if (getpid() != 1 || signal(SIGTERM, broker_stop) == SIG_ERR ||
          signal(SIGINT, broker_stop) == SIG_ERR) return 126;
      pid_t runner = fork(); if (runner < 0) return 126;
      if (runner) {
        int result = 126, main_exited = 0, forwarded = 0;
        for (;;) {
          if (broker_signal && !forwarded) {
            if (kill(runner, broker_signal) && errno != ESRCH) return 126;
            forwarded = 1;
          }
          int status = 0; pid_t reaped = waitpid(-1, &status, WNOHANG);
          if (reaped == runner) {
            main_exited = 1;
            result = WIFEXITED(status) ? WEXITSTATUS(status) :
                     WIFSIGNALED(status) ? 128 + WTERMSIG(status) : 126;
            /* This PID namespace contains only this owned execution tree. */
            if (kill(-1, SIGKILL) && errno != ESRCH) return 126;
          } else if (reaped < 0 && errno == ECHILD) return main_exited ? result : 126;
          else if (reaped < 0 && errno != EINTR) return 126;
          if (!reaped) usleep(10000);
        }
      }
      if (prctl(PR_SET_PDEATHSIG, SIGKILL) || getppid() != 1) return 126;
      signal(SIGTERM, SIG_DFL); signal(SIGINT, SIG_DFL);
    }
    char *const command[] = {"/usr/local/bin/node",
      "--import=/app/tools/capacity/runtime-source-loader.mjs",
      broker ? "/app/tools/capacity/remote-broker-runtime.mjs" :
      restored ? "/app/tools/capacity/restored-runtime-entry.mjs" :
                 "/app/tools/capacity/process-adapter.mjs",
      broker ? "--owned-capacity-remote-broker" :
      restored ? "--owned-restored-report" : "--owned-capacity-child", NULL};
    execv(command[0], command); return 126;
  }
  if (argc != 1 || root_public_origin_grant()) return 126;
  uid_t uid = getuid(); gid_t gid = getgid(); char mapping[96];
  if (unshare(CLONE_NEWUSER | CLONE_NEWNS)) return 126;
  if (text("/proc/self/setgroups", "deny\n") && errno != ENOENT) return 126;
  snprintf(mapping, sizeof(mapping), "0 %u 1\n", uid);
  if (text("/proc/self/uid_map", mapping)) return 126;
  snprintf(mapping, sizeof(mapping), "0 %u 1\n", gid);
  if (text("/proc/self/gid_map", mapping) ||
      mount(NULL, "/", NULL, MS_REC | MS_PRIVATE, NULL)) return 126;
  char *const command[] = {HARBOR_RUNTIME_PYTHON, "-I", HARBOR_RUNTIME_HELPER,
                          "--launch", NULL};
  execv(command[0], command); return 126;
}
