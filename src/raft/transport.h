#ifndef RAFT_TRANSPORT_H
#define RAFT_TRANSPORT_H

#include "raft/config.h"
#include "raft/rpc.h"
#include <cstdint>
#include <functional>
#include <string>
#include <chrono>
#include <cstdint>
#include <map>

namespace raft
{

class RaftTransport
{
public:

//收到一条已解码。已校验的 RPC
using MessageHandler = std::function<void(std::uint64_t peer,RpcType type,const std::string& body)>;

//到某个 peer 时连接断了，外层据此清理掉在途请求
using PeerDownHandler = std::function<void(std::uint64_t peer)>;

RaftTransport(const RaftConfig& config,MessageHandler on_message,PeerDownHandler on_peer_down);
~RaftTransport();


RaftTransport(const RaftTransport&) = delete;
RaftTransport& operator=(const RaftTransport&) = delete;

//绑定自己的地址并开始监听
bool start(std::string& error);

//一次 epoll_wait + 处理时间
void poll(int timeout_ms);

//关闭所有连接和监听
void stop();

//发一帧给 peer ，对端未连接则直接返回 false
bool send(std::uint64_t peer,RpcType type,const std::string& body,std::string& error);

//这条连接是否握手完成
bool connected(std::uint64_t peer)const;

//最近一次轮询中有无事件
bool busy()const;

private:
//一条连接
struct Connection
{
    int fd = -1;
    bool inbound = false; //true = accept 

    //握手后确认的对端 ID
    std::uint64_t peer = 0;

    std::string in; //收到的字节
    std::string out; //还没写出去的字节
    bool want_write = false; //是否等级 EPOLLOUT

    std::uint64_t expect_peer = 0;
};


RaftConfig config_;
MessageHandler on_message_;
PeerDownHandler on_peer_down_;

int epfd_ = -1;
int listen_fd_ = -1;

std::map<int,Connection>conns_;
std::map<std::uint64_t,int>peer_fd_;

//每个 peer 下一次可尝试连接的时间
std::map<std::uint64_t,std::chrono::steady_clock::time_point>next_dial_;

bool running_ = false;
bool busy_ = false;

bool open_listener(std::string& error); //建立监听 socket 并注册 epoll
void dial_due_peers(); //每轮 poll 开头让连接且超时的 peer 各拨一次
void dial_peer(const PeerAddress& peer); //非阻塞 connect 一个 peer
void accept_ready(); //监听 fd 可读
void connect_ready(Connection& conn); //出站连接可写
void read_ready(Connection& conn); //连接可读
void write_ready(Connection& conn); //EPOLLOUT 触发
void send_hello(Connection& conn); //发第一帧 为自己的 ID
bool handle_frame(Connection& conn,RpcType type,const std::string& body,std::string& error); //解释一帧
bool pump_write(Connection& conn); //写空 conn.out
void close_conn(int fd,bool notify); //关连接

};


}//namespcae raft


#endif