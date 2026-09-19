// crash_writer.cpp —— 崩溃注入用的存储读写程序（M5）
//
// 【为什么需要它】
//   kvserver 目前还是 echo（M6 才接存储），所以崩溃注入直接打存储层：
//   这个程序负责"写"，脚本负责"在写的过程中杀掉它 / 破坏文件"，验证由 verify 子命令完成。
//
// 【用法】
//   crash_writer write    <path> <n>       顺序写 n 条：k000000=v000000 ...（每条 put 都 fsync）
//   crash_writer verify   <path> <n>       重开后校验不变量（判定见下）
//   crash_writer truncate <path> <bytes>   从文件末尾截掉 bytes（模拟 torn tail / 断电残尾）
//   crash_writer flip     <path> <offset>  翻转指定偏移的 1 个 bit（模拟位翻转 / 磁盘损坏）
//   crash_writer size     <path>           打印文件字节数
//
// 【verify 的判定（退出码）】
//   0 = 恢复成功且不变量成立
//   2 = 存储拒绝启动（good() == false）—— 对"位翻转"场景是【预期结果】（fail closed）
//   1 = 不变量被破坏 → 真 bug
//
// 【不变量：存在的 key 必须是前缀】
//   写入是顺序的 k000000..k{n-1}，每条 put 都 fsync 成功后立刻更新 keydir。
//   所以任意时刻磁盘上的有效记录必然是前 m 条（0 ≤ m ≤ n）。
//   恢复之后必须满足：
//     · key_count ≤ n
//     · 存在的 key 恰好是 k000000..k{m-1}（不允许"跳号" / 空洞）
//     · 每个存在的 key 的值必须是 v{同样的编号}
//   违反任意一条 = 恢复逻辑有 bug（数据被删 / 错位 / 读出错值）。
//
// 【一个容易搞错的事实】
//   kill -9 单独【不会】写出半条记录：write() 到页缓存这个系统调用在内核里会执行完，
//   进程被杀不会把一次 write 撕成两半。真正能撕出"半条记录"的是机器崩溃/断电。
//   所以：
//     · kill -9 场景：文件必须始终能成功恢复（good() == true，前缀不变量成立）
//     · 截断场景：才是在模拟 torn tail，恢复要能自动截掉残尾
#include "storage/storage.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <iostream>
#include <string>
#include <unistd.h>

static std::string key_of(int i)
{
    char buf[32];
    std::snprintf(buf,sizeof(buf),"k%06d",i);
    return buf;
}

static std::string val_of(int i)
{
    char buf[32];
    std::snprintf(buf,sizeof(buf),"v%06d",i);
    return buf;
}

static long long file_size(const char *path)
{
    std::ifstream f(path,std::ios::binary | std::ios::ate);
    if(!f)
    {
        return -1;
    }
    return static_cast<long long>(f.tellg());
}

//顺序写入：每条 put 成功后打印一行（endl 会 flush，便于脚本判断"写到哪了"）
static int cmd_write(const char *path,int n)
{
    kv::Storage s(path);

    if(!s.good())
    {
        std::cerr << "write: storage not good" << std::endl;
        return 2;
    }

    for(int i = 0; i < n; i++)
    {
        if(!s.put(key_of(i),val_of(i)))
        {
            std::cerr << "write: put failed at i = " << i << std::endl;
            return 3;
        }

        std::cout << "put " << i << std::endl;
    }

    std::cout << "done " << n << std::endl;
    return 0;
}

static int cmd_verify(const char *path,int n)
{
    kv::Storage s(path);

    if(!s.good())
    {
        std::cout << "VERIFY: refused (good() == false)" << std::endl;
        return 2;
    }

    //1) 前缀必须存在，且值正确
    int present = 0;

    while(present < n)
    {
        std::string v;

        if(!s.get(key_of(present),v))
        {
            break;   //到边界了：后面的记录允许不存在
        }

        if(v != val_of(present))
        {
            std::cout << "VERIFY: value mismatch at " << present
                      << " got = " << v << " want = " << val_of(present) << std::endl;
            return 1;
        }

        present++;
    }

    //2) 边界之后不允许再出现任何 key（否则就是"跳号/空洞"）
    for(int i = present; i < n; i++)
    {
        std::string v;

        if(s.get(key_of(i),v))
        {
            std::cout << "VERIFY: hole at " << present
                      << " but key " << i << " exists" << std::endl;
            return 1;
        }
    }

    //3) key_count 必须和"前缀长度"一致
    if(s.key_count() != static_cast<size_t>(present))
    {
        std::cout << "VERIFY: key_count = " << s.key_count()
                  << " but prefix length = " << present << std::endl;
        return 1;
    }

    std::cout << "VERIFY: ok (" << present << "/" << n << " records, key_count = "
              << s.key_count() << ", size = " << file_size(path) << ")" << std::endl;

    //把前缀长度写进 stdout 的最后一行，脚本会解析它
    std::cout << "PRESENT " << present << std::endl;
    return 0;
}

//从文件末尾截掉 bytes 字节（模拟断电留下的残尾）
static int cmd_truncate(const char *path,long long bytes)
{
    long long size = file_size(path);

    if(size < 0 || bytes <= 0 || bytes > size)
    {
        std::cerr << "truncate: bad args (size = " << size << ", bytes = " << bytes << ")" << std::endl;
        return 2;
    }

    int fd = open(path,O_RDWR);

    if(fd < 0)
    {
        std::cerr << "truncate: open failed" << std::endl;
        return 2;
    }

    int rc = ftruncate(fd,static_cast<off_t>(size - bytes));
    close(fd);

    if(rc != 0)
    {
        std::cerr << "truncate: ftruncate failed" << std::endl;
        return 2;
    }

    std::cout << "TRUNCATED from " << size << " to " << size - bytes << std::endl;
    return 0;
}

//翻转指定偏移的 1 个 bit
static int cmd_flip(const char *path,long long offset)
{
    std::fstream f(path,std::ios::binary | std::ios::in | std::ios::out);

    if(!f)
    {
        std::cerr << "flip: open failed" << std::endl;
        return 2;
    }

    f.seekg(offset);

    char c = 0;
    f.read(&c,1);

    if(!f)
    {
        std::cerr << "flip: offset out of range" << std::endl;
        return 2;
    }

    c = static_cast<char>(c ^ 0xFF);

    f.seekp(offset);
    f.write(&c,1);
    f.flush();

    if(!f.good())
    {
        return 2;
    }

    std::cout << "FLIPPED offset " << offset << std::endl;
    return 0;
}

int main(int argc,char **argv)
{
    if(argc < 3)
    {
        std::cerr << "usage: crash_writer <write|verify|truncate|flip|size> <path> [n]"
                  << std::endl;
        return 2;
    }

    std::string cmd = argv[1];
    const char *path = argv[2];

    if(cmd == "write")
    {
        return (argc >= 4) ? cmd_write(path,std::atoi(argv[3])) : 2;
    }

    if(cmd == "verify")
    {
        return (argc >= 4) ? cmd_verify(path,std::atoi(argv[3])) : 2;
    }

    if(cmd == "truncate")
    {
        return (argc >= 4) ? cmd_truncate(path,std::atoll(argv[3])) : 2;
    }

    if(cmd == "flip")
    {
        return (argc >= 4) ? cmd_flip(path,std::atoll(argv[3])) : 2;
    }

    if(cmd == "size")
    {
        std::cout << file_size(path) << std::endl;
        return 0;
    }

    std::cerr << "unknown command: " << cmd << std::endl;
    return 2;
}
