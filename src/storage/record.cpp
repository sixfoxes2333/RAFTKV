#include "storage/record.h"
#include "base/be32.h"
#include <array>

namespace kv
{

    //查表加速 通过空间换时间
    uint32_t crc32(const char *data,size_t len)
    {
        static const std::array<uint32_t,256> table = [] 
        {
        std::array<uint32_t,256> result{};

        for(uint32_t i = 0; i < 256; ++i)
        {
            uint32_t c = i;
            for(int k = 0; k < 8; ++k)
            {
                c = (c & 1)
                    ? 0xEDB88320u ^ (c >> 1)
                    : (c >> 1);
            }
            result[i] = c;
        }

        return result;
        }();

        uint32_t crc = 0xFFFFFFFFu; //初始化全为 1
        //逐字节滑动查表
        for(size_t i = 0 ; i < len ; i++)
        {
            crc = table[(crc ^ static_cast<unsigned char>(data[i])) & 0xFF] ^ (crc >> 8);
        }

        return crc ^ 0xFFFFFFFFu;
    }

    std::string encode_record(RecordType type,const std::string &key,const std::string &value)
    {
        std::string payload;
        payload.push_back(static_cast<char>(type)); //type 1 字节
        append_be32(payload,key.size()); //key_len 大端 4 字节
        append_be32(payload,value.size()); //val_len 大端 4 字节
        payload += key; //key 原始字节
        payload += value; //value 原始字节

        uint32_t payload_crc = crc32(payload.data(),payload.size()); //对 payload 算 CRC

        //固定头 除了 header_crc 之外还有 payload_crc + payload 的前 9 字节
        std::string fixed;
        append_be32(fixed,payload_crc);
        fixed.append(payload.data(),RECORD_PAYLOAD_PREFIX_SIZE);
        
        //头校验和
        uint32_t head_crc = crc32(fixed.data(),fixed.size());

        std::string out;
        append_be32(out,head_crc);
        out += fixed;
        out.append(payload.data() + RECORD_PAYLOAD_PREFIX_SIZE,payload.size()-RECORD_PAYLOAD_PREFIX_SIZE);

        return out;
    }


    bool decode_record(const char* buf,size_t len,Record &out,size_t &consumed)
    {
        //最小记录头 : CRC(4) + type(1) + key_len(4) + val_len(4) = 13

        if(len < RECORD_HEADER_SIZE)
        {
            return false;
        }

        //头校验和
        if(crc32(buf + RECORD_PAYLOAD_CRC_OFF,RECORD_HEADER_SIZE - RECORD_HEADER_CRC_SIZE) != read_be32(buf))
        {
            return false;
        }


        //解析各个字段
        uint32_t payload_crc = read_be32(buf + RECORD_PAYLOAD_CRC_OFF); //偏移 0
        uint8_t type = static_cast<uint8_t>(buf[RECORD_TYPE_OFF]); //偏移 4
        uint32_t klen = read_be32(buf+RECORD_KEY_LEN_OFF); //偏移 5
        uint32_t vlen = read_be32(buf+RECORD_VAL_LEN_OFF); //偏移 9

        //合法性检查
        if(type != static_cast<uint8_t>(RecordType::Set) && type != static_cast<uint8_t>(RecordType::Del))
        {
            return false;
        }

        //若超过上限，可能是因为损坏
        if(klen > MAX_KEY || vlen > MAX_VALUE)
        {
            return false;
        }

        //检查内容是否到齐
        size_t total = RECORD_HEADER_SIZE + static_cast<size_t>(klen) + vlen;
        if(len < total)
        {
            return false;
        }

        // 负载校验和
        if(crc32(buf + RECORD_TYPE_OFF,RECORD_PAYLOAD_PREFIX_SIZE + static_cast<size_t>(klen) + vlen) != payload_crc)
        {
            return false;
        }

        //全部校验都通过则填写输出参数
        out.type = static_cast<RecordType>(type);
        out.key.assign(buf+RECORD_HEADER_SIZE,klen); //清空并重新赋值
        out.value.assign(buf+RECORD_HEADER_SIZE+klen,vlen); //清空并重新赋值

        consumed = total; //告诉调用方该记录占用多少字节

        return true;

    }

}//namespace kv
