#ifndef RAFT_CORE_H
#define RAFT_CORE_H

#include "raft/state.h" // 复用 hardstate
#include "raft/logstore.h"
#include <cstdint>
#include <string>
#include <vector>
#include <cstddef>
#include <optional>

namespace raft
{

enum class Role
{
    Follower,
    Candidate,
    Leader
};

//投票请求
struct RequestVoteRequest
{
    //候选人正在竞选的任期
    std::uint64_t term = 0;

    //候选人节点 ID
    std::uint64_t candidate_id = 0;

    //候选人最后一条日志的 index 空日志为 0
    std::uint64_t last_log_index = 0;

    //以上日志的 term 空日志为 0
    std::uint64_t last_log_term = 0;

};

//回复投票请求
struct RequestVoteResponse
{
    //接收方处理请求后的当前任期
    std::uint64_t term = 0;

    //是否授予选票  持久化后才发送授票回复
    bool vote_granted = false;
};

//保存一轮选举产生的持久化数据和投票请求
struct ElectionOutPut
{
    HardState hard_state{};

    RequestVoteRequest request{};

    std::vector<std::uint64_t>targets; //不含本机的接收请求的节点 ID

};

//处理投票请求后产生的结果
struct VoteOutPut
{
    std::optional<HardState>hard_state{}; //本次需要保存的任期/投票
    RequestVoteResponse response{}; //发回请求发送者的回复
    bool reset_election_timer = false; //是否重新设置选举超时
};

//处理一张投票回复后产生的结果
struct VoteCountOutPut
{
    std::optional<HardState> hard_state{}; //任期变化时保存数据
    bool become_leader = false;

};


//Core 只读查看日志,拿到 index 
class LogView
{
public:
    virtual ~LogView() = default;

    //最后一条日志的 index 空返回 0
    virtual std::uint64_t last_index()const = 0;

    //取到 index 处的任期
    virtual bool term_at(std::uint64_t index,std::uint64_t& out)const = 0;


    //取到 index 处的完整条目
    virtual bool entry_at(std::uint64_t index,LogEntry& out)const = 0;

};

//leader 发给 follower 的日志复制请求
//entries 为空即心跳
struct AppendEntriesRequest
{
    //leader 当前的任期，接收方用于判断自己是否过期
    std::uint64_t term = 0;

    //leader 的节点 ID ，接收方用其记录当前 leader 
    std::uint64_t leader_id = 0;

    //紧挨着要发的日志的前一位，用于一致性检查
    std::uint64_t prev_log_index = 0;

    //prev_log_index 那条日志的任期
    std::uint64_t prev_log_term = 0;

    //从 pre_log_index + 1 开始连续复制的条目，心跳时为空
    std::vector<LogEntry>entries;

    //leader 已知提交位置，接收方根据此推进自己的 commitIndex
    std::uint64_t leader_commit = 0;
};

//接收方对日志请求的回复
struct AppendEntriesResponse
{   
    //接收方处理后的当前任期
    //leader 见到 更大 任期则退位
    std::uint64_t term = 0;

    //一致性检查
    bool success = false;
};

//处理完 leader 的日志请求后
struct AppendEntriesOut
{
    //本次保存的任期与投票 仅任期变化时才有
    std::optional<HardState>hard_state{};

    //回复里要带的本地任期
    std::uint64_t term = 0;

    //核对通过
    bool accept = false;

    //从该处开始往后删除 0 表示不删
    std::uint64_t truncate_from = 0;

    //需要追加到本地的日志
    std::vector<LogEntry>entries;

    //重开选举计时器
    bool reset_election_timer = false;

};


class RaftCore
{
public:

    RaftCore() = default;

    //禁用复制
    RaftCore(const RaftCore&) = delete;
    RaftCore& operator=(const RaftCore&) = delete;
    RaftCore(RaftCore&&) = delete;
    RaftCore& operator=(RaftCore&&) = delete;

    //初始化数据
    //member 即固定集群的全部节点 ID recovered 即 StateStore 加载的 term/vote
    bool init(std::uint64_t self_id,const std::vector<std::uint64_t>& members,const HardState& recovered,std::uint64_t last_log_index,std::uint64_t last_log_term,std::string& error);


    //完成初始化且未停止才可继续处理
    bool good()const;

    //获取当前角色 未初始化则默认为 Follower
    Role role()const;

    //只读访问 Core 当前的 term/vote
    const HardState& hard_state()const;

    //获取多数票的门槛，在 good() 中用于协议判断
    std::size_t majority()const;

    //不可逆地停止此实例
    void stop();

    //开始选举
    bool start_election(ElectionOutPut& out,std::string& error);

    //是否处理完成
    bool handle_request_vote(std::uint64_t from,const RequestVoteRequest& request,VoteOutPut& out,std::string& error);

    bool handle_vote_response(std::uint64_t from,const RequestVoteResponse& response,VoteCountOutPut& out,std::string& error);

    //收到 leader 发来的日志请求
    bool handle_append_entries(std::uint64_t from,const AppendEntriesRequest& request,AppendEntriesOut& out,std::string& error);

    //绑定外层提供的只读日志视图
    bool attach_log(const LogView* log,std::string& error);

    //日志写成功后调用
    bool on_log_persisted(std::string& error);

    //下一条准备发给某个节点的是第几条
    std::uint64_t next_index(std::uint64_t peer)const;

    //已经确认复制到某个节点的是第几条
    std::uint64_t match_index(std::uint64_t peer)const;

    //给 peer 打包一份日志复制请求
    bool build_append_entries(std::uint64_t peer,AppendEntriesRequest& out,std::string& error)const;



private:

//本节点身份 默认未初始化为 0
std::uint64_t self_id_ = 0;

//存储固定集群的成员
std::vector<std::uint64_t>members_;

//多数票的门槛 成员数 / 2 +1
std::size_t majority_ = 0;

HardState hard_state_{};

//当前角色 默认为 Follower
Role role_ = Role::Follower;

std::uint64_t last_log_index_ = 0;
std::uint64_t last_log_term_ = 0;

//用于计票去重 谁投给了本机
std::vector<std::uint64_t>votes_;

bool initalized_ = false;

bool stopped_ = false;

const LogView* log_ = nullptr;

//当选 leader 时调用，改角色，按成员准备复制进度
void become_leader();

//下标和 member_ 对齐 : 下一条发给它的 index
std::vector<std::uint64_t>next_index_;

//已经确认复制成功的 index
std::vector<std::uint64_t>match_index_;

//一次最多 64 条日志，不超过 1 Mib
static constexpr std::size_t MAX_APPEND_ENTRIES = 64;
static constexpr std::size_t MAX_APPEND_COMMAND_BYTES = 1024*1024;

//现已提交到第几条
std::uint64_t commit_index_ = 0;



};


}//namespace raft

#endif