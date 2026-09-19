#ifndef NET_PROTOCOL_H
#define NET_PROTOCOL_H

#include<cstddef>
#include<cstdint>
#include<string>
#include "base/be32.h" 

namespace kv
{

    //请求里的命令类型 不同于存储层的 enum class RecordType
    enum class Op : uint8_t
    {
        Set = 1,
        Get = 2,
        Del = 3,
    };

    //响应状态
    enum class Status : uint8_t
    {
        Ok = 0,
        NotFound = 1,
        Error = 2,

    };

    struct Request
    {
        Op op = Op::Get;
        std::string key;
        std::string value; //仅 SET 才有
    };

    struct Response
    {
        Status status = Status::Error;
        std::string value; //只有 GET 命中才有
    };

    //固定头长度
    inline constexpr size_t REQUEST_HEADER_SIZE = sizeof(uint8_t) + 2 * sizeof(uint32_t); //op 1 //key_len 4 val_len 4
    inline constexpr size_t RESPONSE_HEADER_SIZE = sizeof(uint8_t) + sizeof(uint32_t); //status 1 val_len 4

    //请求 op1 key_len4 val_len4 key value
    std::string encode_request(const Request &req); //客户端把 Request 变为字节
    bool parse_request(const char* buf,size_t len,Request &out); //服务端把收到的字节变为 Request

    //响应 status1 val_len4 value
    std::string encode_response(const Response &resp); //服务端把Response 变为字节
    bool parse_response(const char* buf,size_t len,Response &out); //客户端把收到的字节变成 Response



}//namspace kv


#endif