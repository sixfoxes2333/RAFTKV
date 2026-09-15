#ifndef STORAGE_BE32_H
#define STORAGE_BE32_H
#include<cstdint>
#include<string>
#include<cstring>
#include<arpa/inet.h>

//大端 32 位读写工具 record.cpp 与 storage.cpp 共用（原来在 storage.cpp 里）
namespace kv
{

    //将 32 位无符号整数按照 大端字节序 转换为 4 个字节
    // 32 位无符号整数存的是 CRC key_len value_len
    static void append_be32(std::string &s,uint32_t v)
    {
        uint32_t be = htonl(v); //主机序转换为大端序
        s.append(reinterpret_cast<const char *>(&be),4); //将 4 字节追加写入缓冲区
    }

    //将 4 个字节转换为 32 位无符号整数
    static uint32_t read_be32(const char *p)
    {
        uint32_t be;
        memcpy(&be,p,4); //原样取得 4 字节
        return ntohl(be); //由
    }

}

#endif
