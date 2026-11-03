#ifndef RAFT_COMMAND_H
#define RAFT_COMMAND_H

#include "net/protocol.h"
#include <string>

namespace raft
{

//日志命令解码结果 noop 为 true 则不执行 request
struct Command
{
    bool noop = false;
    kv::Request request;
};

std::string make_noop_command();

//Work 接收客户端请求后调用，生成 LogEntry::command
bool encode_kv_command(const kv::Request& request,std::string& out,std::string& error);

//准备执行已提交日志时调用
bool decode_command(const std::string& bytes,Command& out,std::string& error);


}//namespace raft


#endif