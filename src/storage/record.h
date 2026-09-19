#ifndef RECORD_H
#define RECORD_H

#include<string>
#include<cstdint>
#include<cstddef>
#include "base/limits.h" //MAX_KEY / MAX_VALUE（网络帧、协议、记录共用）

inline constexpr size_t RECORD_HEADER_CRC_SIZE = 4;
inline constexpr size_t RECORD_CRC_SIZE = 4;
inline constexpr size_t RECORD_TYPE_SIZE = 1;
inline constexpr size_t RECORD_LEN_SIZE = 4;

inline constexpr size_t RECORD_PAYLOAD_PREFIX_SIZE = RECORD_TYPE_SIZE + 2 * RECORD_LEN_SIZE;


//命名空间防止和其他库同名冲突
namespace kv
{

    inline constexpr size_t RECORD_HEADER_SIZE = RECORD_CRC_SIZE + RECORD_TYPE_SIZE + 2 * RECORD_LEN_SIZE + RECORD_CRC_SIZE;

    inline constexpr size_t RECORD_PAYLOAD_CRC_OFF = RECORD_HEADER_CRC_SIZE;                   //4
    inline constexpr size_t RECORD_TYPE_OFF        = RECORD_PAYLOAD_CRC_OFF + RECORD_CRC_SIZE; //8
    inline constexpr size_t RECORD_KEY_LEN_OFF     = RECORD_TYPE_OFF + RECORD_TYPE_SIZE;       //9
    inline constexpr size_t RECORD_VAL_LEN_OFF     = RECORD_KEY_LEN_OFF + RECORD_LEN_SIZE;     //13

    static_assert(RECORD_HEADER_SIZE == 17,"记录头长度变了必须同时改 FORMAT_VERSION 和测试里的黄金值");

    //MAX_KEY / MAX_VALUE 见 base/limits.h（已在上方 include）

    //类型记录码
    enum class RecordType : uint8_t
    {
        Set = 1, //写入/覆盖
        Del = 2, //删除墓碑
    };

    //数据包 作为 decode 的输出容器
    struct Record
    {
        RecordType type; //记录类型 1 = SET (写入/覆盖) 2 = DEL (删除墓碑)
        std::string key; //键
        std::string value; //值
    };

    //CRC32 计算检验
    uint32_t crc32(const char *data,size_t len);

    //编码 将记录转换为字节流 完整的一条记录包含 4 字节 CRC头 校验由 put() 负责
    std::string encode_record(RecordType type,const std::string &key,const std::string &value);

    //解码 将字节流转换为记录 buf 为起始位置 len 即 buf 里可用字节数 out 即解析成功时写入 comsumed 即写入成功时总共占用了多少字节
    bool decode_record(const char *buf,size_t len,Record &out,size_t &consumed);

}


#endif
