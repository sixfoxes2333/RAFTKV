#ifndef RAFT_RPC_H
#define RAFT_RPC_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace raft
{

enum class RpcType:std::uint8_t
{
    VoteRequest = 1, //候选人拉票
    VoteResponse = 2, //对方是否投票
    AppendEntries = 3, //leader 复制日志或者仅心跳
    AppendResponse = 4, //对方是否接收
    Hello = 5, //连接后报自己的 ID
};

// 帧 4 B 正文长度 + 1 B 类型 + 正文
inline constexpr std::size_t RPC_LENGTH_SIZE = 4;
inline constexpr std::size_t RPC_TYPE_SIZE = 1;
inline constexpr std::size_t RPC_HEADER_SIZE = RPC_LENGTH_SIZE + RPC_TYPE_SIZE;
inline constexpr std::size_t HELLO_BODY_SIZE = 8;

//四类 RPC 的请求编号占 8 字节，请求与回复使用同一编号
inline constexpr std::size_t RPC_ID_SIZE = 8;

//一条 command 最多 16 MiB 17 MiB 装下一个合法请求
inline constexpr std::size_t MAX_RPC_PAYLOAD = 17*1024*1024;

//接收端允许一条报文最多带多少条目
inline constexpr std::size_t MAX_RPC_ENTRIES = 4096;

//复制请求的定长头 term/leader_id/prev_log_index/prev_log_term 各 8B，count 4B，leader_commit 8B
inline constexpr std::size_t APPEND_HEADER_SIZE =  44;

//每个条目的定长部分：term 8B + cmd_len 4B
inline constexpr std::size_t ENTRY_HEADER_SIZE = 12;



}//namespace raft

#endif