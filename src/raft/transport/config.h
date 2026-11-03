#ifndef RAFT_CONFIG_H
#define RAFT_CONFIG_H

#include <cstdint>
#include <string>
#include <vector>

namespace raft
{

//一个集群成员在网络上的位置
struct PeerAddress
{
    std::uint64_t id = 0;
    std::string host;
    std::uint16_t port = 0;
};

struct RaftConfig
{
    std::uint64_t self_id = 0;

    PeerAddress self;
    //不含自己的其他成员 ID 升序
    std::vector<PeerAddress>peers;

    //状态/日志放在哪里
    std::string data_dir;

    //含自己的全部成员 ID 升序
    std::vector<std::uint64_t>members()const;
};

//解析
bool parse_raft_config(const std::string& id_text,const std::string& peers_text,const std::string& data_dir,RaftConfig& out,std::string& error);

//读 ID / PEERS / DATA
bool load_raft_config_from_env(RaftConfig& out,std::string& error);


}//namespace raft

#endif