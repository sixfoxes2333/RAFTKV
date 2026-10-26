#ifndef RAFT_RPC_CODEC_H
#define RAFT_RPC_CODEC_H

#include "raft/rpc.h"
#include "raft/core.h"
#include <cstddef>
#include <string>

namespace raft
{

//编码为帧
bool encode_frame(RpcType type,const std::string& body,std::string& out,std::string& error);

//读 header 算出帧长
bool peek_frame_size(const char* data,std::size_t len,std::size_t& frame_size,std::string& error);

//帧解码
bool decode_frame(const char* data,std::size_t len,RpcType& type,std::string& body,std::string& error);

//结构编解码

bool encode_vote_request(const RequestVoteRequest& in,std::string& out,std::string& error);
bool decode_vote_request(const std::string& body,RequestVoteRequest& out,std::string& error);

bool encode_vote_response(const RequestVoteResponse& in,std::string& out,std::string& error);
bool decode_vote_response(const std::string& body,RequestVoteResponse& out,std::string& error);

bool encode_append_entries(const AppendEntriesRequest& in,std::string& out,std::string& error);
bool decode_append_entries(const std::string& body,AppendEntriesRequest& out,std::string& error);

bool encode_append_response(const AppendEntriesResponse& in,std::string& out,std::string& error);
bool decode_append_response(const std::string& body,AppendEntriesResponse& out,std::string& error);

}//namespace raft

#endif