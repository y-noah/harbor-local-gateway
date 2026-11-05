#define _GNU_SOURCE
#include <errno.h>
#include <fcntl.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/vfs.h>
#include <unistd.h>

/* Enter the owned cgroup before exec, so every descendant inherits its limits. */
static int fail(void) {
    static const char message[] = "capacity_resource_entry_failed\n";
    ssize_t written = write(STDERR_FILENO, message, sizeof(message) - 1);
    (void)written;
    return 126;
}

static int number(const char *text, uintmax_t *result) {
    char *end;
    if (!text[0] || strspn(text, "0123456789") != strlen(text)) return 0;
    errno = 0;
    *result = strtoumax(text, &end, 10);
    return errno == 0 && *end == '\0';
}

static int read_value(int directory, const char *name, char *buffer, size_t size) {
    int fd = openat(directory, name, O_RDONLY | O_CLOEXEC | O_NOFOLLOW);
    if (fd < 0) return 0;
    ssize_t count = read(fd, buffer, size - 1);
    int closed = close(fd);
    if (count < 0 || (size_t)count >= size - 1 || closed < 0) return 0;
    buffer[count] = '\0';
    return 1;
}

static int limits_match(int directory, uintmax_t expected_quota,
                        uintmax_t expected_period, uintmax_t expected_memory) {
    uintmax_t quota, period, memory;
    char buffer[128], extra;
    return read_value(directory, "cpu.max", buffer, sizeof(buffer)) &&
        sscanf(buffer, "%" SCNuMAX " %" SCNuMAX " %c", &quota, &period, &extra) == 2 &&
        quota == expected_quota && period == expected_period &&
        read_value(directory, "memory.max", buffer, sizeof(buffer)) &&
        sscanf(buffer, "%" SCNuMAX " %c", &memory, &extra) == 1 &&
        memory == expected_memory;
}

int main(int argc, char **argv) {
    uintmax_t device, inode, quota, period, memory;
    struct stat identity;
    struct statfs filesystem;
    char buffer[128];
    if (argc < 8 || argv[1][0] != '/' || argv[7][0] != '/' ||
        !number(argv[2], &device) || !number(argv[3], &inode) ||
        !number(argv[4], &quota) || quota < 1000 || quota > UINTMAX_C(9007199254740991) ||
        !number(argv[5], &period) || period != 100000 ||
        !number(argv[6], &memory) || memory == 0 || memory > UINTMAX_C(9007199254740991)) return fail();
    int directory = open(argv[1], O_RDONLY | O_DIRECTORY | O_CLOEXEC | O_NOFOLLOW);
    if (directory < 0 || fstat(directory, &identity) < 0 ||
        fstatfs(directory, &filesystem) < 0 || filesystem.f_type != 0x63677270 ||
        (uintmax_t)identity.st_dev != device || (uintmax_t)identity.st_ino != inode ||
        identity.st_uid != geteuid()) return fail();
    if (!limits_match(directory, quota, period, memory)) return fail();
    int membership = openat(directory, "cgroup.procs", O_WRONLY | O_CLOEXEC | O_NOFOLLOW);
    if (membership < 0) return fail();
    int length = snprintf(buffer, sizeof(buffer), "%ld\n", (long)getpid());
    if (length <= 0 || write(membership, buffer, (size_t)length) != length ||
        close(membership) < 0 || close(directory) < 0) return fail();
    execv(argv[7], &argv[7]);
    return fail();
}
