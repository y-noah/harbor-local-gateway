#define _GNU_SOURCE
#include <sys/prctl.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <limits.h>

#ifndef HARBOR_PREPARATION_NODE
#error Fixed Node executable is required
#endif
#ifndef HARBOR_PREPARATION_MODULE
#error Fixed preparation module is required
#endif

/* The subreaper flag survives exec, but is not inherited by forked children.
 * Orphaned PG backends therefore return to this exact Node owner. */
int main(int argc, char **argv) {
  (void)argv;
  if (argc != 1) return 126;
  char executable[PATH_MAX], number[32];
  ssize_t length = readlink("/proc/self/exe", executable, sizeof(executable) - 1);
  if (length <= 0 || length >= (ssize_t)sizeof(executable) - 1) return 126;
  executable[length] = '\0';
  char *last = strrchr(executable, '/');
  if (!last || strcmp(last + 1, "entry")) return 126;
  int binary = open(executable, O_RDONLY | O_NOFOLLOW);
  if (binary < 0) return 126;
  *last = '\0';
  int directory = open(executable, O_RDONLY | O_DIRECTORY | O_NOFOLLOW);
  struct stat d, b;
  if (directory < 0 || fstat(directory, &d) || fstat(binary, &b) ||
      !S_ISDIR(d.st_mode) || !S_ISREG(b.st_mode) ||
      d.st_uid != geteuid() || b.st_uid != geteuid() ||
      (d.st_mode & 0777) != 0700 || (b.st_mode & 0777) != 0700) return 126;
  int enabled = 0;
  if (prctl(PR_SET_CHILD_SUBREAPER, 1, 0, 0, 0) ||
      prctl(PR_GET_CHILD_SUBREAPER, &enabled, 0, 0, 0) || enabled != 1 ||
      fcntl(directory, F_SETFD, 0) || fcntl(binary, F_SETFD, 0)) return 126;
  snprintf(number, sizeof(number), "%d", directory);
  if (setenv("HARBOR_PREPARATION_ENTRY_DIRECTORY_FD", number, 1)) return 126;
  snprintf(number, sizeof(number), "%d", binary);
  if (setenv("HARBOR_PREPARATION_ENTRY_BINARY_FD", number, 1)) return 126;
  char *const command[] = {HARBOR_PREPARATION_NODE, HARBOR_PREPARATION_MODULE,
                          "--owned-capacity-preparation", NULL};
  execv(HARBOR_PREPARATION_NODE, command);
  return 126;
}
