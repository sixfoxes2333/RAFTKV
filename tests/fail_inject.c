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
 *   KV_FAIL_PWRITE_CALL=N     第 N 次 pwrite()（只计对 fd>2 的调用）返回 -1/EIO
 *                             （Raft 日志用 pwrite 定位写，不经过 write()）
 *   KV_FAIL_PREAD_CALL=N      第 N 次 pread()（只计对 fd>2 的调用）返回 -1/EIO
 *   KV_SHORT_PWRITE=1         每次 pwrite() 最多只写 1 字节（验证循环拼装正确）
 *   KV_FAIL_FTRUNCATE_CALL=N  第 N 次 ftruncate() 返回 -1/EIO
 *   KV_FAIL_FSYNC_CALL=N      第 N 次 fsync() 返回 -1/EIO
 *   KV_FAIL_EPOLL_ADD=N      第 N 次 epoll_ctl(EPOLL_CTL_ADD) 返回 -1/ENOMEM
 *                             （用来验证"注册失败必须响亮退出"：服务器不许活着但不服务）
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
 *   LD_PRELOAD=/tmp/kv_fail_inject.so KV_FAIL_EPOLL_ADD=1 ./build/kvserver /tmp/x.db
 */
#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <sys/epoll.h>
#include <stdio.h>
#include <fcntl.h>

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
static int g_epoll_add_calls = 0;

static int g_pwrite_calls = 0;
static int g_pread_calls = 0;
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

/* Raft 日志用 pwrite/pread 做定位读写（不经过 write），所以单独挂钩子。
   KV_SHORT_PWRITE=1 时每次最多写 1 字节：用来验证 write_full 的循环拼接。 */
ssize_t pwrite(int fd,const void *buf,size_t count,off_t offset)
{
    static ssize_t (*real_pwrite)(int,const void *,size_t,off_t) = NULL;

    if(real_pwrite == NULL)
    {
        real_pwrite = (ssize_t (*)(int,const void *,size_t,off_t))dlsym(RTLD_NEXT,"pwrite");
    }

    if(fd > 2)
    {
        int fail_at = env_int("KV_FAIL_PWRITE_CALL");
        g_pwrite_calls++;

        if(fail_at > 0 && g_pwrite_calls == fail_at)
        {
            errno = EIO;             /* 模拟"写盘失败" */
            return -1;
        }

        if(env_int("KV_SHORT_PWRITE") > 0 && count > 1)
        {
            count = 1;
        }
    }

    return real_pwrite(fd,buf,count,offset);
}

ssize_t pread(int fd,void *buf,size_t count,off_t offset)
{
    static ssize_t (*real_pread)(int,void *,size_t,off_t) = NULL;

    if(real_pread == NULL)
    {
        real_pread = (ssize_t (*)(int,void *,size_t,off_t))dlsym(RTLD_NEXT,"pread");
    }

    if(fd > 2)
    {
        int fail_at = env_int("KV_FAIL_PREAD_CALL");
        g_pread_calls++;

        if(fail_at > 0 && g_pread_calls == fail_at)
        {
            errno = EIO;             /* 模拟"读盘失败" */
            return -1;
        }
    }

    return real_pread(fd,buf,count,offset);
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
    /* KV_COUNT_FSYNC=/path：记录每次真正落盘的时间戳，供测试观察。
       这里用 dlsym 拿到的真 write，绕过本文件自己的 write 包装（否则会污染 KV_FAIL_WRITE_CALL 计数）。 */
    const char *count_path = getenv("KV_COUNT_FSYNC");

    if(count_path != NULL && *count_path != '\0')
    {
        static ssize_t (*real_write_local)(int,const void *,size_t) = NULL;

        if(real_write_local == NULL)
        {
            real_write_local = (ssize_t (*)(int,const void *,size_t))dlsym(RTLD_NEXT,"write");
        }

        int cf = open(count_path,O_WRONLY | O_CREAT | O_APPEND,0644);

        if(cf >= 0)
        {
            struct timespec now;
            clock_gettime(CLOCK_MONOTONIC,&now);

            char line[64];
            int n = snprintf(line,sizeof(line),"%lld fd=%d\n",
                             (long long)now.tv_sec * 1000 + now.tv_nsec / 1000000,fd);

            if(real_write_local != NULL && n > 0)
            {
                (void)real_write_local(cf,line,(size_t)n);
            }

            close(cf);
        }
    }


    return real_fsync(fd);
}


/* KV_FAIL_EPOLL_ADD=N：第 N 次 epoll_ctl(EPOLL_CTL_ADD) 返回 -1/ENOMEM。
   注入的是【ADD】那一次注册，不是 epoll_wait。
   启动阶段的 ADD 顺序：第 1 次 = 监听 fd；第 2 次 = worker 的 eventfd。 */
int epoll_ctl(int epfd,int op,int fd,struct epoll_event *event)
{
    static int (*real_epoll_ctl)(int,int,int,struct epoll_event*) = NULL;

    if(real_epoll_ctl == NULL)
    {
        real_epoll_ctl = (int (*)(int,int,int,struct epoll_event*))dlsym(RTLD_NEXT,"epoll_ctl");
    }

    if(op == EPOLL_CTL_ADD)
    {
        int fail_at = env_int("KV_FAIL_EPOLL_ADD");
        g_epoll_add_calls++;

        if(fail_at > 0 && g_epoll_add_calls == fail_at)
        {
            errno = ENOMEM;      /* epoll watch 上限用尽时就是这个错误 */
            return -1;
        }
    }

    return real_epoll_ctl(epfd,op,fd,event);
}
