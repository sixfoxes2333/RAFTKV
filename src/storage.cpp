#include"storage.h"
#include<cstring>
#include<arpa/inet.h>


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

    //查表加速 通过空间换时间
    uint32_t crc32(const char *data,size_t len)
    {
        static uint32_t table[256]; //生成 256 字节查找表，处理每字节仅需一次异或+查表
        static bool init = false; //仅在第一次调用时建表

        if(!init)
        {
            for(uint32_t i = 0 ; i < 256 ; i++)
            {
                uint32_t c = i;
                for(int k = 0 ; k < 8 ; k++)
                {
                    c = (c & 1) ? 0xEDB88320u ^ (c >> 1) : (c >> 1); //逐位除法
                    table[i] = c;
                }
            }
            init = true;
        }

        uint32_t crc = 0xFFFFFFFFu; //初始化全为 1
        //逐字节滑动查表
        for(size_t i = 0 ; i < len ; i++)
        {
            crc = table[(crc ^ static_cast<unsigned char>(data[i])) & 0xFF] ^ (crc >> 8);
        }

        return crc ^ 0xFFFFFFFFu;
    }

    std::string encode_record(uint8_t type,const std::string &key,const std::string &value)
    {
        std::string body;
        body.push_back(static_cast<char>(type)); //type 1 字节
        append_be32(body,key.size()); //key_len 大端 4 字节
        append_be32(body,value.size()); //val_len 大端 4 字节
        body += key; //key 原始字节
        body += value; //value 原始字节

        uint32_t c = crc32(body.data(),body.size()); //对 body 算 CRC

        //CRC 在前，body 在后
        std::string out;
        append_be32(out,c);
        out += body;

        return out;
    }


    bool decode_record(const char* buf,size_t len,Record &out,size_t &consumed)
    {
        //最小记录头 : CRC(4) + type(1) + key_len(4) + val_len(4) = 13

        if(len < 13)
        {
            return false;
        }

        //解析各个字段
        uint32_t crc_stored = read_be32(buf); //偏移 0
        uint8_t type = static_cast<uint8_t>(buf[4]); //偏移 4
        uint32_t klen = read_be32(buf+5); //偏移 5
        uint32_t vlen = read_be32(buf+9); //偏移 9

        //若超过上限，可能是因为损坏
        if(klen > MAX_KEY || vlen > MAX_VALUE)
        {
            return false;
        }

        //检查内容是否到齐
        size_t total = 13 + static_cast<size_t>(klen) + vlen;
        if(len < total)
        {
            return false;
        }

        //CRC 校验 type 起 (buf+4)，共 9 + klen + vlen 字节
        //9 = type(1) + key_len(4) + val_len(4)
        uint32_t crc_calc = crc32(buf+4,9+klen + vlen);
        if(crc_calc != crc_stored)
        {
            return false;
        }

        //全部校验都通过则填写输出参数
        out.type = type;
        out.key.assign(buf+13,klen); //清空并重新赋值
        out.value.assign(buf+13+klen,vlen); //清空并重新赋值

        consumed = total; //告诉调用方该记录占用多少字节

        return true;

    }


}//namespace kv
