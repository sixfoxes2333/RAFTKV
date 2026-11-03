#include "raft/protocol/command.h"
#include "raft/core/logstore.h"
#include "base/limits.h"
#include <utility>

namespace raft
{

//单字节 0 为内部 no-op
std::string make_noop_command()
{
    return std::string(1,'\0');
}

bool encode_kv_command(const kv::Request& request,std::string& out,std::string& error)
{
    error.clear();

    if(request.op != kv::Op::Set && request.op != kv::Op::Get && request.op != kv::Op::Del)
    {
        error = "raft command : unknown operation";
        return false;
    }

    if(request.key.empty() || request.key.size() > kv::MAX_KEY || request.value.size() > kv::MAX_VALUE)
    {
        error = "raft command : invalid key/value length";
        return false;
    }

    if(request.op != kv::Op::Set && !request.value.empty())
    {
        error = "raft command : GET/DEL cannot carry value";
        return false;
    }

    const std::size_t room = LogStore::MAX_COMMAND_SIZE - kv::REQUEST_HEADER_SIZE;

    if(request.key.size() > room || request.value.size() > room - request.key.size())
    {
        error = "raft command : command exceeds 16 MiB";
        return false;
    }

    std::string next = kv::encode_request(request);
    out.swap(next);
    return true;
}

//解码
bool decode_command(const std::string& bytes,Command& out,std::string& error)
{
    error.clear();

    if(bytes.empty() || bytes.size() > LogStore::MAX_COMMAND_SIZE)
    {
        error = "raft command : invalid command length";
        return false;
    }

    Command next;

    if(bytes[0] == '\0')
    {
        if(bytes.size() != 1)
        {
            error = "raft command : trailing bytes after noop";
            return false;
        }

        next.noop = true;
    }
    else
    {
        if(!kv::parse_request(bytes.data(),bytes.size(),next.request))
        {
            error = "raft command : malformed KV request";
            return false;
        }
    }

    out = std::move(next);
    return true;
}

}//namespace raft