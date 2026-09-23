/*
 * fail_inject.c —— LD_PRELOAD 故障注入库（M5）
 *
 * 【用途】
 *   不改生产代码，只通过 LD_PRELOAD 覆盖 write / ftruncate / fsync，让第 N 次调用失败。
 *   用来验证存储层的"失败语义"：
 *     · fsync 失败        → 记录可能在页缓存但没落盘 → 对象必须进入 poisoned 状态
 *     · write 失败 + 回滚成功 → 属于【普通失败】：文件已回到写入前，对象仍然健康
 *     · write 失败 + 回滚也失败 → 提交结果不确定 → poisoned
 *
 * 【环境变量】
 *   KV_FAIL_WRITE_CALL=N      第 N 次 write()（1-based，只计对 fd>2 的调用）返回 -1/ENOSPC
 *   KV_FAIL_FTRUNCATE_CALL=N  第 N 次 ftruncate() 返回 -1/EIO
 *   KV_FAIL_FSYNC_CALL=N      第 N 次 fsync() 返回 -1/EIO
 *   KV_DELAY_FSYNC_MS=N       每次 fsync() 之前先睡 N 毫秒（模拟慢磁盘），不影响返回值
 *   不设置或设为 0 表示该注入关闭。
 *
 * 【为什么排除 fd <= 2】
 *   stdout/stderr 也是 write()，如果一起注入，测试程序的输出（PUT/GOOD 那些行）会被破坏，
 *   脚本就没法判断行为了。所以只对"数据文件"（fd > 2）计数。
 *
 * 【编译】
 *   gcc -shared -fPIC -O0 -g -o /tmp/kv_fail_inject.so tests/fail_inject.c -ldl
 *
 * 【用法】
 *   LD_PRELOAD=/tmp/kv_fail_inject.so KV_FAIL_FSYNC_CALL=3 ./build/crash_writer poison /tmp/x.log 3
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>

static int env_int(const char *name)
{
    const char *v = getenv(name);

    if(v == NULL || *v == '\0')
    {
        return 0;
    }

    return atoi(v);
}

/* 只统计对 fd > 2 的调用（避开 stdout/stderr） */
static int g_write_calls = 0;
static int g_ftruncate_calls = 0;
static int g_fsync_calls = 0;

ssize_t write(int fd,const void *buf,size_t count)
{
    static ssize_t (*real_write)(int,const void *,size_t) = NULL;

    if(real_write == NULL)
    {
        real_write = (ssize_t (*)(int,const void *,size_t))dlsym(RTLD_NEXT,"write");
    }

    if(fd > 2)
    {
        int fail_at = env_int("KV_FAIL_WRITE_CALL");
        g_write_calls++;

        if(fail_at > 0 && g_write_calls == fail_at)
        {
            errno = ENOSPC;      /* 模拟"磁盘满" */
            return -1;
        }
    }

    return real_write(fd,buf,count);
}

int ftruncate(int fd,off_t length)
{
    static int (*real_ftruncate)(int,off_t) = NULL;

    if(real_ftruncate == NULL)
    {
        real_ftruncate = (int (*)(int,off_t))dlsym(RTLD_NEXT,"ftruncate");
    }

    int fail_at = env_int("KV_FAIL_FTRUNCATE_CALL");
    g_ftruncate_calls++;

    if(fail_at > 0 && g_ftruncate_calls == fail_at)
    {
        errno = EIO;             /* 模拟"回滚也失败" */
        return -1;
    }

    return real_ftruncate(fd,length);
}

int fsync(int fd)
{
    static int (*real_fsync)(int) = NULL;

    if(real_fsync == NULL)
    {
        real_fsync = (int (*)(int))dlsym(RTLD_NEXT,"fsync");
    }

    /* KV_DELAY_FSYNC_MS：把 fsync 拖慢，用来验证"fsync 慢时事件循环还能不能服务"。
       放在真正 fsync 之前睡，效果等价于慢设备（返回值不变，只是变慢）。 */
    int delay_ms = env_int("KV_DELAY_FSYNC_MS");

    if(delay_ms > 0)
    {
        struct timespec ts;
        ts.tv_sec = delay_ms / 1000;
        ts.tv_nsec = (long)(delay_ms % 1000) * 1000000L;
        nanosleep(&ts,NULL);
    }

    int fail_at = env_int("KV_FAIL_FSYNC_CALL");
    g_fsync_calls++;

    if(fail_at > 0 && g_fsync_calls == fail_at)
    {
        errno = EIO;             /* 模拟"落盘失败" */
        return -1;
    }

    return real_fsync(fd);
}
