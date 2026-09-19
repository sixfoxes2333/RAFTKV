#include "net/protocol.h"
#include "base/be32.h"
#include "base/limits.h"

namespace kv
{

    std::string encode_request(const Request &req)
    {
        //编码不校验
        std::string out;

        out.push_back(static_cast<char>(req.op));

        append_be32(out,req.key.size());
        append_be32(out,req.value.size());

        out += req.key;
        out += req.value;

        return out;
    }

    bool parse_request(const char* buf,size_t len,Request &out)
    {
        if(len < REQUEST_HEADER_SIZE)
        {
            return false;
        }

        uint8_t op = static_cast<uint8_t>(buf[0]);
        uint32_t klen = read_be32(buf + 1);
        uint32_t vlen = read_be32(buf + 5);

        if(op != static_cast<uint8_t>(Op::Set) && op != static_cast<uint8_t>(Op::Get) && op != static_cast<uint8_t>(Op::Del))
        {
            return false;
        }
        
        //key 不能为空 不能超过上限 value 不超过上限
        if(klen == 0 || klen > MAX_KEY || vlen > MAX_VALUE)
        {
            return false;
        }

        //GET / DEL 不允许带 value
        if(op != static_cast<uint8_t>(Op::Set) && vlen != 0)
        {
            return false;
        }

        //声明长度要与实际字节数完全相同
        if(len != REQUEST_HEADER_SIZE + static_cast<size_t>(klen) + static_cast<size_t>(vlen))
        {
            return false;
        }

        out.op = static_cast<Op>(op);
        out.key.assign(buf + REQUEST_HEADER_SIZE,klen);
        out.value.assign(buf + REQUEST_HEADER_SIZE + klen,vlen);
        return true;

    }

    std::string encode_response(const Response &resp)
    {
        std::string out;
        out.push_back(static_cast<char>(resp.status));
        append_be32(out,resp.value.size());
        out += resp.value;
        return out;
    }

    bool parse_response(const char* buf,size_t len,Response &out)
    {
        if(len < RESPONSE_HEADER_SIZE)
        {
            return false;
        }

        uint8_t status =  static_cast<uint8_t>(buf[0]);
        uint32_t vlen = read_be32(buf + 1);

        if(status != static_cast<uint8_t>(Status::Ok) && status != static_cast<uint8_t>(Status::NotFound) && status != static_cast<uint8_t>(Status::Error))
        {
            return false;
        }

        if(vlen > MAX_VALUE)
        {
            return false;
        }

        if(len != RESPONSE_HEADER_SIZE + static_cast<size_t>(vlen))
        {
            return false;
        }

        out.status = static_cast<Status>(status);

        out.value.assign(buf + RESPONSE_HEADER_SIZE,vlen);

        return true;

    }


}//namespace kv