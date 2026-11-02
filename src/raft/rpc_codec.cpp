#include "raft/rpc_codec.h"
#include "base/be32.h"
#include <cstring>
#include <utility>

namespace raft
{

namespace
{
    //排除未知 RPC
    bool type_known(std::uint8_t raw)
    {
        return raw >= static_cast<std::uint8_t>(RpcType::VoteRequest)&&raw <= static_cast<std::uint8_t>(RpcType::Hello);
    }

    bool read_bool(const std::string& body,std::size_t at,bool& out,std::string& error)
    {
        const auto raw = static_cast<unsigned char>(body[at]);

        if(raw > 1)
        {
            error = "rpc body : bool field is not 0 or 1";
            return false;
        }

        out = (raw == 1);
        return true;
    }

}

bool encode_frame(RpcType type,const std::string& body,std::string& out,std::string& error)
{
    error.clear();

    const std::size_t payload = RPC_TYPE_SIZE + body.size();

    if(payload > MAX_RPC_PAYLOAD)
    {
        error = "rpc encode: payload too large";
        return false;
    }

    std::string frame;

    frame.reserve(RPC_LENGTH_SIZE + payload);

    kv::append_be32(frame,static_cast<std::uint32_t>(payload));
    frame.push_back(static_cast<char>(type));
    frame += body;

    out = std::move(frame);
    return true;

}

//在原正文写入前写入编号
bool encode_rpc_frame(RpcType type,std::uint64_t rpc_id,const std::string& body,std::string& out,std::string& error)
{
    error.clear();

    if(!type_known(static_cast<std::uint8_t>(type)) || type == RpcType::Hello)
    {
        error = "rpc encode : business type required";
        return false;
    }

    if(rpc_id == 0)
    {
        error = "rpc encode : rpc id must not be zero";
        return false;
    }

    if(body.size() > MAX_RPC_PAYLOAD - RPC_TYPE_SIZE - RPC_ID_SIZE)
    {
        error = "rpc encode : payload too large";
        return false;
    }

    std::string payload(RPC_ID_SIZE,'\0');
    kv::put_be64(payload.data(),rpc_id);
    payload += body;

    return encode_frame(type,payload,out,error);
}


bool peek_frame_size(const char* data,std::size_t len,std::size_t& frame_size,std::string& error)
{
    error.clear();

    if(len < RPC_LENGTH_SIZE)
    {
        error = "rpc decode : header incomplete";
        return false;
    }

    const std::uint32_t payload = kv::read_be32(data);

    if(payload > MAX_RPC_PAYLOAD)
    {
        error = "rpc decode : payload too large";
        return false;
    }

    if(payload < RPC_TYPE_SIZE)
    {
        error = "rpc decode : payload too short for type";
        return false;
    }

    frame_size = RPC_LENGTH_SIZE + static_cast<std::size_t>(payload);
    return true;
}

bool decode_frame(const char* data,std::size_t len,RpcType& type,std::string& body,std::string& error)
{
    std::size_t frame_size = 0;

    if(!peek_frame_size(data,len,frame_size,error))
    {
        return false;
    }

    if(len != frame_size)
    {
        error = "rpc decode : frame length mismatch";
        return false;
    }

    const std::uint8_t raw = static_cast<std::uint8_t>(data[RPC_LENGTH_SIZE]);

    if(!type_known(raw))
    {
        error = "rpc decode : unknown type";
        return false;
    }

    RpcType parsed_type = static_cast<RpcType>(raw);
    std::string parsed_body(data+RPC_HEADER_SIZE,len-RPC_HEADER_SIZE);

    type = parsed_type;
    body = std::move(parsed_body);
    return true;
}

//handle_frame 收到业务帧时调用
bool decode_rpc_body(const std::string& wire_body,std::uint64_t& rpc_id,std::string& body,std::string& error)
{
    error.clear();

    if(wire_body.size() < RPC_ID_SIZE)
    {
        error = "rpc decode : rpc id incomplete";
        return false;
    }

    if(wire_body.size() > MAX_RPC_PAYLOAD - RPC_TYPE_SIZE)
    {
        error = "rpc decode : payload too large";
        return false;
    }

    const std::uint64_t parsed_id = kv::read_be64(wire_body.data());

    if(parsed_id == 0)
    {
        error = "rpc decode : rpc id must not be zero";
        return false;
    }

    std::string parsed_body(wire_body.data() + RPC_ID_SIZE,wire_body.size() - RPC_ID_SIZE);

    body = std::move(parsed_body);
    rpc_id = parsed_id;

    return true;
}





bool encode_vote_request(const RequestVoteRequest& in,std::string& out,std::string& error)
{
    error.clear();

    std::string body(32,'\0');

    kv::put_be64(body.data(),in.term);
    kv::put_be64(body.data() + 8 ,in.candidate_id);
    kv::put_be64(body.data() + 16 ,in.last_log_index);
    kv::put_be64(body.data() + 24 ,in.last_log_term);

    out = std::move(body);
    return true;
}


bool decode_vote_request(const std::string& body,RequestVoteRequest& out,std::string& error)
{
    error.clear();

    if(body.size() != 32)
    {
        error = "rpc body : vote request size mismatch";
        return false;
    }

    RequestVoteRequest parsed;

    parsed.term = kv::read_be64(body.data());
    parsed.candidate_id = kv::read_be64(body.data()+8);
    parsed.last_log_index = kv::read_be64(body.data()+16);
    parsed.last_log_term = kv::read_be64(body.data()+24);

    out = parsed;
    return true;
}

bool encode_vote_response(const RequestVoteResponse& in,std::string& out,std::string& error)
{
    error.clear();

    //固定 9 字节 term 8 B bool 1 B
    std::string body(9,'\0');

    kv::put_be64(body.data(),in.term);
    body[8] = static_cast<char>(in.vote_granted ? 1 : 0);

    out = std::move(body);
    return true;
}

bool decode_vote_response(const std::string& body,RequestVoteResponse& out,std::string& error)
{
    error.clear();

    if(body.size() != 9)
    {
        error = "rpc body : vote response size mismatch";
        return false;
    }

    RequestVoteResponse parsed;
    parsed.term = kv::read_be64(body.data());

    if(!read_bool(body,8,parsed.vote_granted,error))
    {
        return false;
    }

    out = parsed;
    return true;
}

bool encode_append_entries(const AppendEntriesRequest& in,std::string& out,std::string& error)
{
    error.clear();

    if(in.entries.size() > MAX_RPC_ENTRIES)
    {
        error = "rpc body : too many entries";
        return false;
    }

    std::size_t total = APPEND_HEADER_SIZE;

    for(const auto& entry:in.entries)
    {
        total += ENTRY_HEADER_SIZE + entry.command.size();
    }

    std::string body(total,'\0');

    kv::put_be64(body.data(),in.term);
    kv::put_be64(body.data()+8,in.leader_id);
    kv::put_be64(body.data()+16,in.prev_log_index);
    kv::put_be64(body.data()+24,in.prev_log_term);
    kv::put_be32(body.data()+32,static_cast<uint32_t>(in.entries.size()));
    kv::put_be64(body.data()+36,in.leader_commit);

    std::size_t at = APPEND_HEADER_SIZE;

    for(const auto& entry:in.entries)
    {
        kv::put_be64(body.data()+at,entry.term);
        kv::put_be32(body.data()+at+8,static_cast<std::uint32_t>(entry.command.size()));
        std::memcpy(body.data()+at+ENTRY_HEADER_SIZE,entry.command.data(),entry.command.size());

        at += ENTRY_HEADER_SIZE + entry.command.size();
    }

    out = std::move(body);
    return true;

}

bool decode_append_entries(const std::string& body,AppendEntriesRequest& out,std::string& error)
{
    error.clear();

    if(body.size() < APPEND_HEADER_SIZE)
    {
        error = "rpc body : append request too short";
        return false;
    }

    const std::uint32_t count = kv::read_be32(body.data() + 32);

    // 第一道闸：协议上限。对端可以把这个字段写成 40 亿
    if(count > MAX_RPC_ENTRIES)
    {
        error = "rpc body : too many entries";
        return false;
    }

    // 第二道闸：剩下的字节根本放不下这么多条（每条至少 12 字节）
    // 光靠第一道闸，17MiB 的"空命令小条目"能拼出上百万条，把内存放大成几十 MB
    if(count > (body.size() - APPEND_HEADER_SIZE) / ENTRY_HEADER_SIZE)
    {
        error = "rpc body : entry count does not fit the body";
        return false;
    }

    AppendEntriesRequest parsed;

    parsed.term           = kv::read_be64(body.data() + 0);
    parsed.leader_id      = kv::read_be64(body.data() + 8);
    parsed.prev_log_index = kv::read_be64(body.data() + 16);
    parsed.prev_log_term  = kv::read_be64(body.data() + 24);
    parsed.leader_commit  = kv::read_be64(body.data() + 36);

    // 两道闸都过了，这里的 reserve 才是有界的
    parsed.entries.reserve(count);

    std::size_t at = APPEND_HEADER_SIZE;

    for(std::uint32_t i = 0 ; i < count ; i++)
    {
        // 连条目头都放不下：count 写多了，或者正文被截断
        if(body.size() - at < ENTRY_HEADER_SIZE)
        {
            error = "rpc body : entry header truncated";
            return false;
        }

        LogEntry entry;
        entry.term = kv::read_be64(body.data() + at);

        const std::uint32_t cmd_len = kv::read_be32(body.data() + at + 8);

        at += ENTRY_HEADER_SIZE;

        if(body.size() - at < cmd_len)
        {
            error = "rpc body : command truncated";
            return false;
        }

        entry.command.assign(body.data() + at,cmd_len);
        at += cmd_len;

        // index 不上线，留给 Core 按 prev_log_index+1+序号 填
        entry.index = 0;

        parsed.entries.push_back(std::move(entry));
    }

    // 走完必须正好用光：多一个字节说明这不是一条完整合法报文
    if(at != body.size())
    {
        error = "rpc body : trailing bytes after entries";
        return false;
    }

    out = std::move(parsed);
    return true;
}


bool encode_append_response(const AppendEntriesResponse& in,std::string& out,std::string& error)
{
    error.clear();

    std::string body(9,'\0');

    kv::put_be64(body.data(),in.term);
    body[8] = static_cast<char>(in.success ? 1 : 0);

    out = std::move(body);
    return true;
}


bool decode_append_response(const std::string& body,AppendEntriesResponse& out,std::string& error)
{
    error.clear();

    if(body.size() != 9)
    {
        error = "rpc body : append response size mismatch";
        return false;
    }

    AppendEntriesResponse parsed;
    parsed.term = kv::read_be64(body.data());

    if(!read_bool(body,8,parsed.success,error))
    {
        return false;
    }

    out = parsed;
    return true;
}

bool encode_hello(std::uint64_t self_id,std::string& out,std::string& error)
{
    error.clear();

    if(self_id == 0)
    {
        error = "rpc body : hello with zero id";
        return false;
    }

    std::string body(HELLO_BODY_SIZE,'\0');
    kv::put_be64(body.data(),self_id);

    out = std::move(body);
    return true;
}

bool decode_hello(const std::string& body,std::uint64_t& self_id,std::string& error)
{
    error.clear();

    if(body.size() != HELLO_BODY_SIZE)
    {
        error = "rpc body : hello size mismatch";
        return false;
    }

    const std::uint64_t id = kv::read_be64(body.data());

    if(id == 0)
    {
        error = "rpc body : hello with zero id";
        return false;
    }

    self_id = id;
    return true;
}




}//namespace raft