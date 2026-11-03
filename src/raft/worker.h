#ifndef RAFT_WORKER_H
#define RAFT_WORKER_H

#include "raft/config.h"
#include "raft/core.h"
#include "raft/state.h"
#include "raft/logstore.h"
#include "raft/logview.h"
#include "raft/transport.h"
#include "net/protocol.h"
#include "raft/state_machine.h"
#include <chrono>
#include <random>
#include <memory>
#include <map>
#include <atomic>
#include <condition_variable>
#include <cstddef>
#include <mutex>
#include <thread>
#include <deque>
#include <vector>


namespace raft
{

//每个节点创建一个 worker
class RaftWorker
{
public:

//main 创建 worker 时调用
explicit RaftWorker(const RaftConfig& config);
~RaftWorker();

RaftWorker(const RaftWorker&) = delete;
RaftWorker& operator=(const RaftWorker&) = delete;
RaftWorker(RaftWorker&&) = delete;
RaftWorker& operator=(RaftWorker&&) = delete;


bool init(std::string& error);

//主线程读取的运行快照
struct Status
{
    bool running = false;
    Role role = Role::Follower;
    std::uint64_t term = 0;
    std::uint64_t commit = 0;
    std::uint64_t last = 0;
    std::uint64_t applied = 0; //KV 已经执行到哪里
    bool serving = false; //本任期屏障是否已完成，可以服务客户端
    std::uint64_t updated_ms = 0; //后台采样时间
    std::string error;
};

//主线程调用，等待后台监听建立成功或启动失败后返回
bool start(std::string& error);

//主线程请求停止并等待后台结束，可重复调用
void stop();

//返回加锁复制的快照，主线程不直接访问 core
Status status() const;

    //Reactor 线程投递的一条客户端请求
    struct ClientRequest
    {
        int fd = -1;
        std::uint64_t connection_id = 0;
        std::uint64_t sequence = 0;
        kv::Request request;
        std::size_t bytes = 0;
    };

    //后台执行完成后交回 Reactor 的结果
    struct ClientResult
    {
        int fd = -1;
        std::uint64_t connection_id = 0;
        std::uint64_t sequence = 0;
        kv::Response response;
        std::size_t bytes = 0;
    };

    //Reactor 线程调用：只排队
    bool submit_client(ClientRequest request);

    //Reactor 线程调用：搬走当前所有结果
    std::vector<ClientResult> take_client_results();

    //把这个 eventfd 注册进客户端 Reactor
    int wakeup_fd() const
    {
        return wakeup_fd_;
    }


private:

    RaftConfig config_; //本地 ID，成员地址，数据目录
    StateStore state_; //保存和恢复任期，投票
    LogStore log_; //保存和查询 Raft 日志
    LogStoreView log_view_; //将 log_ 的查询接口提供给 Core
    RaftCore core_; //判断选举，复制和提交


    int dir_fd_ = -1; //持有数据目录和锁
    bool init_attempted_ = false; //每个worker仅一次初始化


    using Clock = std::chrono::steady_clock;

    struct PendingAppend
    {
        std::uint64_t rpc_id = 0;
        AppendEntriesRequest request;
        Clock::time_point deadline{};
    };
    std::map<std::uint64_t,PendingAppend>pending_appends_; 

    //leader 的心跳，追平或者超时重试调度调用
    bool send_append(std::uint64_t peer,std::string& error);


    //重新抽取选举等待时间，更新下一次选举的截止时间
    void reset_election_timer();

    std::mt19937_64 random_{std::random_device{}()};
    Clock::time_point election_deadline_{};
    Clock::time_point heartbeat_deadline_{}; //Leader 下一次发送心跳的时间

    //循环检测选举超时，保存本轮投票，再发送拉票请求
    bool start_election(std::string& error);

    std::unique_ptr<RaftTransport>transport_; //后续 start() 创建并启动网络
    std::uint64_t next_rpc_id_ = 1; //本进程递增的请求编号
    std::map<std::uint64_t,std::uint64_t>vote_rpc_ids_;

    // 网络回调收到合法拉票请求后调用，保存投票结果，再发送回复
    bool handle_vote_request(std::uint64_t peer,std::uint64_t rpc_id,const RequestVoteRequest& request,std::string& error);

    //网络回调收到合法投票回复后调用，处理任期变化，本轮计票和胜选
    bool handle_vote_response(std::uint64_t peer,std::uint64_t rpc_id,const RequestVoteResponse& response,std::string& error);

    //收到合法复制请求或心跳后调用，完成持久化，提交推进和回复
    bool handle_append_entries(std::uint64_t peer,std::uint64_t rpc_id,const AppendEntriesRequest& request,std::string& error);
    std::uint64_t committed_index_ = 0; //本次运行已确认的提交位置，供后续 KV apply 使用

    //复制回复先处理更高任期，再按编号取原请求，更新进度和提交位置
    bool handle_append_response(std::uint64_t peer,std::uint64_t rpc_id,const AppendEntriesResponse& response,std::string& error);


    //transport 断线回调调用，清理这个 peer 的在线请求
    void on_peer_down(std::uint64_t peer);

    //网络回调的统一入口 : 检查来源和正文，再分发四类业务 RPC
    bool handle_message(std::uint64_t peer,RpcType type,std::uint64_t rpc_id,const std::string& body,std::string& error);

    //后台线程入口，处理网络和计时器
    void worker_loop();

    //init 或后台完成状态动作后调用，更新快照
    void publish_status(bool running,const std::string& error);

    std::thread worker_thread_;
    std::atomic<bool>stop_requested_{false};
    bool start_attempted_ = false;

    mutable std::mutex status_mutex_; //保存快照和startup_done_
    std::condition_variable startup_cv_;
    bool startup_done_ = false;
    Status status_;
    std::string runtime_error_; //仅后台读写，通过快照交给主线程

    static constexpr std::size_t MAX_CLIENT_BYTES = 16 * 1024 * 1024;

    int wakeup_fd_ = -1; //通知 Reactor 有结果可取

    mutable std::mutex client_mutex_;
    std::deque<ClientRequest> client_inbox_; //Reactor 写，后台取
    std::vector<ClientResult> client_outbox_; //后台写，Reactor 取
    std::size_t client_in_bytes_ = 0;
    std::size_t client_out_bytes_ = 0;

    StateMachine machine_; //派生 KV 与 lastApplied

    //等待执行的客户端请求，按日志序号索引
    struct Waiter
    {
        int fd = -1;
        std::uint64_t connection_id = 0;
        std::uint64_t sequence = 0;
        std::uint64_t term = 0;
        std::size_t bytes = 0;
    };
    std::map<std::uint64_t,Waiter> waiting_;

    std::uint64_t barrier_index_ = 0; //本任期 no-op 的日志序号
    std::uint64_t leader_term_ = 0; //当前服务客户端的 leader 任期
    bool barrier_ready_ = false; //屏障已提交且已执行

    void notify_reactor();
    bool pump_clients(std::string& error); //取请求，处理领导权与屏障
    bool pump_apply(std::string& error); //提交已完成日志给 KV
    bool propose_command(const std::string& command,std::uint64_t& index,std::string& error);
    void publish_applied(const ApplyResult& result); //把执行结果交给客户端
    void answer_locked(ClientResult result); //调用方必须持有 client_mutex_
    void abort_waiting(); //退位或停止时统一回 Error
    void reject_inbox(); //非 leader 时拒绝排队中的请求
};

}//namespace raft

#endif