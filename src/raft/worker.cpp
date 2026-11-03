#include "raft/worker.h"
#include "raft/rpc_codec.h"
#include "raft/apply.h"
#include "raft/rpc_validate.h"
#include "raft/command.h"
#include <algorithm>
#include <cerrno>
#include <cstring>
#include <fcntl.h>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>
#include <sys/eventfd.h>
#include <utility>
#include <iostream>
#include <exception>

namespace raft
{

//构造时保存路径，使日志视图指向本节点的 logstore
RaftWorker::RaftWorker(const RaftConfig& config):config_(config),state_(config_.data_dir),log_(config_.data_dir),log_view_(log_),machine_(config_.data_dir)
{

}

//写 eventfd 唤醒客户端 Reactor；计数器已满说明通知已经生效
void RaftWorker::notify_reactor()
{
    if(wakeup_fd_ < 0)
    {
        return;
    }

    const std::uint64_t one = 1;
    ssize_t n;

    do
    {
        n = ::write(wakeup_fd_,&one,sizeof(one));
    }
    while(n < 0 && errno == EINTR);

    if(n == static_cast<ssize_t>(sizeof(one)) || (n < 0 && errno == EAGAIN))
    {
        return;
    }

    std::cerr << "raft worker : eventfd write failed" << std::endl;
}

RaftWorker::~RaftWorker()
{
    stop();
    if(wakeup_fd_ >= 0)
    {
        ::close(wakeup_fd_);
        wakeup_fd_ = -1;
    }
    if(dir_fd_ >= 0)
    {
        ::close(dir_fd_);
    }
}

//准备此节点的持久化文件
bool RaftWorker::init(std::string& error)
{
    error.clear();

    //这个 fd 跟着 worker 整个生命周期
    if(wakeup_fd_ < 0)
    {
        wakeup_fd_ = ::eventfd(0,EFD_NONBLOCK | EFD_CLOEXEC);

        if(wakeup_fd_ < 0)
        {
            error = std::string("raft worker : create eventfd : ")
                    + std::strerror(errno);
            return false;
        }
    }

    if(init_attempted_)
    {
        error = "raft worker : init already attempted";
        return false;
    }

    init_attempted_ = true;

    dir_fd_ = ::open(config_.data_dir.c_str(),O_RDONLY | O_DIRECTORY | O_CLOEXEC);

    if(dir_fd_ < 0)
    {
        const int code = errno;
        error = std::string("raft worker : open data directory : ") + std::strerror(code);
        return false;
    }

    while(::flock(dir_fd_,LOCK_EX | LOCK_NB) < 0)
    {
        if(errno == EINTR)
        {
            continue;
        }

        const int code = errno;

        error = std::string("raft worker : lock data directory : ") + std::strerror(code);
        return false;
    }

    struct stat state_stat{};

    const int state_result = ::fstatat(dir_fd_,"raft.state",&state_stat,AT_SYMLINK_NOFOLLOW);

    if(state_result < 0 && errno != ENOENT)
    {
        const int code = errno;
        error = std::string("raft worker : stat raft.state : ") + std::strerror(code);
        return false;
    }

    const bool state_exists = (state_result == 0);

    if(state_exists && !S_ISREG(state_stat.st_mode))
    {
        error = "raft worker : raft.state must be a regular file";
        return false;
    }

    struct stat log_stat{};
    const int log_result = ::fstatat(dir_fd_,"raft.log",&log_stat,AT_SYMLINK_NOFOLLOW);

    if(log_result < 0 && errno != ENOENT)
    {
        const int code = errno;
        error = std::string("raft worker : stat raft.log : ") + std::strerror(code);
        return false;
    }

    const bool log_exists = (log_result == 0);

    if(log_exists && !S_ISREG(log_stat.st_mode))
    {
        error = "raft worker : raft.log must be a regular file";
        return false;
    }

    //已有日志但是丢失任期选票，不可当作新节点重新开始
    if(!state_exists && log_exists)
    {
        error = "raft worker : raft.state missing but raft.log exists";
        return false;
    }

    HardState recovered{};

    if(state_exists)
    {
        if(!state_.load(recovered,error))
        {
            return false;
        }
    }
    else if(!state_.save(recovered,error))
    {
        return false;
    }

    if(log_exists)
    {
        if(!log_.open(error))
        {
            return false;
        }
    }
    else
    {
        //已经进入过任期的节点缺失日志则拒绝启动
        if(recovered.current_term != 0 || recovered.voted_for != 0)
        {
            error = "raft worker : raft.log missing after term or vote was saved";
            return false;
        }

        if(!log_.create(error))
        {
            return false;
        }
    }

    const std::uint64_t last_index = log_.last_index();
    std::uint64_t last_term = 0;

    if(last_index != 0 && !log_.term_at(last_index,last_term,error))
    {
        return false;
    }

    if(!core_.attach_log(&log_view_,error))
    {
        return false;
    }

    if(!core_.init(config_.self_id,config_.members(),recovered,last_index,last_term,error))
    {
        return false;
    }

    reset_election_timer();
    publish_status(false,"");
    //Raft 文件恢复完成后再重建派生 KV，lastApplied 从 0 开始
    if(!machine_.init(error))
    {
        return false;
    }

    return true;
}


void RaftWorker::reset_election_timer()
{
    std::uniform_int_distribution<int>timeout_ms(1000,2000);

    election_deadline_ = Clock::now() + std::chrono::milliseconds(timeout_ms(random_));
}


bool RaftWorker::start_election(std::string& error)
{
    error.clear();

    ElectionOutPut result;

    if(!core_.start_election(result,error))
    {
        core_.stop();
        return false;
    }

    //保存成功前，不发送任何依赖本轮任期和自投票的消息
    if(!state_.save(result.hard_state,error))
    {
        core_.stop();
        return false;
    }

    vote_rpc_ids_.clear();
    reset_election_timer();

    //单节点已经完成持久化自投票，core 成为 leader
    if(result.targets.empty())
    {
        return true;
    }

    std::string body;

    if(!encode_vote_request(result.request,body,error))
    {
        core_.stop();
        return false;
    }

    for(const auto peer : result.targets)
    {
        //编号用完后停止
        if(next_rpc_id_ == 0)
        {
            error = "raft worker : rpc id exhausted";
            core_.stop();
            return false;
        }

        const std::uint64_t id = next_rpc_id_++;
        std::string send_error;

        if(transport_->send(peer,RpcType::VoteRequest,id,body,send_error))
        {
            vote_rpc_ids_[peer] = id;
        }
    }

    return true;
}


bool RaftWorker::handle_vote_request(std::uint64_t peer,std::uint64_t rpc_id,const RequestVoteRequest& request,std::string& error)
{
    error.clear();

    VoteOutPut result;

    if(!core_.handle_request_vote(peer,request,result,error))
    {
        core_.stop();
        return false;
    }

    if(result.hard_state && !state_.save(*result.hard_state,error))
    {
        core_.stop();
        return false;
    }

    if(core_.role() == Role::Follower)
    {
        vote_rpc_ids_.clear();
    }

    if(result.reset_election_timer)
    {
        reset_election_timer();
    }

    std::string response_body;

    if(!encode_vote_response(result.response,response_body,error))
    {
        core_.stop();
        return false;
    }

    //回复沿用请求编号，断线时保留已经保存的投票，等待候选人重试
    std::string send_error;
    (void)transport_->send(peer,RpcType::VoteResponse,rpc_id,response_body,send_error);

    return true;
}


bool RaftWorker::handle_vote_response(std::uint64_t peer,std::uint64_t rpc_id,const RequestVoteResponse& response,std::string& error)
{
    error.clear();

    const bool higher_term = response.term > core_.hard_state().current_term;

    if(!higher_term)
    {
        const auto found = vote_rpc_ids_.find(peer);

        if(core_.role() != Role::Candidate || found == vote_rpc_ids_.end() || found->second != rpc_id)
        {
            return true;
        }
    }

    VoteCountOutPut result;

    if(!core_.handle_vote_response(peer,response,result,error))
    {
        core_.stop();
        return false;
    }

    if(result.hard_state && !state_.save(*result.hard_state,error))
    {
        core_.stop();
        return false;
    }

    if(higher_term)
    {
        vote_rpc_ids_.clear();
        reset_election_timer();
    }

    if(result.become_leader)
    {
        vote_rpc_ids_.clear();

        //下次后台循环立即进入 leader 心跳调度
        heartbeat_deadline_ = Clock::now();
    }

    return true;
}

//处理已解码，校验来源的复制请求
//必要状态和日志保存成功后，才能推进提交位置并发送成功回复
bool RaftWorker::handle_append_entries(std::uint64_t peer,std::uint64_t rpc_id,const AppendEntriesRequest& request,std::string& error)
{
    error.clear();

    AppendEntriesOut result;

    if(!core_.handle_append_entries(peer,request,result,error))
    {
        core_.stop();
        return false;
    }

    AppendEntriesResponse response;

    //这个已有函数负责 Raft 状态/日志落盘 失败时会停止 Core
    if(!apply_append_entries(core_,state_,log_,result,response,error))
    {
        return false;
    }

    if(core_.role() == Role::Follower)
    {
        vote_rpc_ids_.clear();
    }

    if(result.reset_election_timer)
    {
        reset_election_timer();
    }

    if(response.success)
    {
        CommitOut committed;

        if(!core_.advance_follower_commit(request,committed,error))
        {
            core_.stop();
            return false;
        }

        if(committed.committed_to != 0)
        {
            committed_index_ = committed.committed_to;
        }
    }

    std::string response_body;

    if(!encode_append_response(response,response_body,error))
    {
        core_.stop();
        return false;
    }

    std::string send_error;
    (void)transport_->send(peer,RpcType::AppendResponse,rpc_id,response_body,send_error);

    return true;

}

//各一个 peer 发送复制请求 ，无日志即心跳
//原请求先记录，再发送
bool RaftWorker::send_append(std::uint64_t peer,std::string& error)
{
    error.clear();

    if(core_.role() != Role::Leader)
    {
        return true;
    }

    const auto now = Clock::now();
    const auto found = pending_appends_.find(peer);

    if(found != pending_appends_.end())
    {
        //本任期的请求尚未超时，继续等待回复
        if(found->second.request.term == core_.hard_state().current_term && now < found->second.deadline)
        {
            return true;
        }

        pending_appends_.erase(found);
    }

    if(!transport_->connected(peer))
    {
        return true;
    }

    AppendEntriesRequest request;

    if(!core_.build_append_entries(peer,request,error))
    {
        core_.stop();
        return false;
    }

    std::string body;

    if(!encode_append_entries(request,body,error))
    {
        core_.stop();
        return false;
    }

    if(next_rpc_id_ == 0)
    {
        error = "raft worker : rpc id exhausted";
        core_.stop();
        return false;
    }

    const std::uint64_t id = next_rpc_id_++;

    PendingAppend pending;
    pending.rpc_id = id;
    pending.request = std::move(request);
    pending.deadline = Clock::now() + std::chrono::milliseconds(500);
    pending_appends_.emplace(peer,std::move(pending));

    std::string send_error;
    (void)transport_->send(peer,RpcType::AppendEntries,id,body,send_error);

    return true;

}

bool RaftWorker::handle_append_response(std::uint64_t peer,std::uint64_t rpc_id,const AppendEntriesResponse& response,std::string& error)
{
    error.clear();

    const auto current_term = core_.hard_state().current_term;
    const bool higher_term = response.term > current_term;

    AppendEntriesRequest sent;

    if(!higher_term)
    {
        if(core_.role() != Role::Leader || response.term != current_term)
        {
            return true;
        }

        const auto found = pending_appends_.find(peer);

        if(found == pending_appends_.end() || found->second.rpc_id != rpc_id || found->second.request.term != current_term)
        {
            return true;
        }

        sent = std::move(found->second.request);
        pending_appends_.erase(found);

    }

    AppendResponseOut result;

    //core 的更高任期分支不读取 sent 
    if(!core_.handle_append_entries_response(peer,sent,response,result,error))
    {
        core_.stop();
        return false;
    }

    if(result.hard_state && !state_.save(*result.hard_state,error))
    {
        core_.stop();
        return false;
    }

    if(higher_term)
    {
        pending_appends_.clear();
        vote_rpc_ids_.clear();
        reset_election_timer();
        return true;
    }

    CommitOut committed;

    if(!core_.advance_commit(committed,error))
    {
        core_.stop();
        return false;
    }

    if(committed.committed_to != 0)
    {
        committed_index_ = committed.committed_to;
    }

    if(result.retry || committed.committed_to != 0 || (response.success && core_.match_index(peer) < log_.last_index()))
    {
        heartbeat_deadline_ = Clock::now();
    }

    return true;
}

void RaftWorker::on_peer_down(std::uint64_t peer)
{
    vote_rpc_ids_.erase(peer);
    pending_appends_.erase(peer);
}

bool RaftWorker::handle_message(std::uint64_t peer,RpcType type,std::uint64_t rpc_id,const std::string& body,std::string& error)
{
    error.clear();

    //同一批网络事件中，本地失败则不再处理后续消息
    if(!core_.good())
    {
        return true;
    }

    const auto member = std::find_if(config_.peers.begin(),config_.peers.end(),[peer](const PeerAddress& address){return address.id == peer;});

    if(member == config_.peers.end())
    {
        return true;
    }

    std::string decode_error;
    bool ok = true;
    switch(type)
    {
        case RpcType::VoteRequest:
        {
            RequestVoteRequest request;

            if(!decode_vote_request(body,request,decode_error) || !validate_vote_request(request,decode_error) || request.candidate_id != peer || request.last_log_term > request.term)
            {
                return true;
            }

            ok = handle_vote_request(peer,rpc_id,request,error);
            break;
        }

        case RpcType::VoteResponse:
        {
            RequestVoteResponse response;

            if(!decode_vote_response(body,response,decode_error) || response.term == 0)
            {
                return true;
            }

            ok = handle_vote_response(peer,rpc_id,response,error);
            break;
        }

        case RpcType::AppendEntries:
        {
            AppendEntriesRequest request;

            if(!decode_append_entries(body,request,decode_error) || !validate_append_entries(request,decode_error) || request.leader_id != peer || request.prev_log_term > request.term)
            {
                return true;
            }

            for(const auto& entry : request.entries)
            {
                if(entry.command.size() > LogStore::MAX_COMMAND_SIZE)
                {
                    return true;
                }
            }

            ok = handle_append_entries(peer,rpc_id,request,error);
            break;
        }

        case RpcType::AppendResponse:
        {
            AppendEntriesResponse response;

            if(!decode_append_response(body,response,decode_error) || response.term == 0)
            {
                return true;
            }

            ok = handle_append_response(peer,rpc_id,response,error);
            break;
        }

        default:
            return true;
    }

    if(!ok || core_.role() != Role::Leader)
    {
        pending_appends_.clear();
    }

    return ok;
}

//更新快照
void RaftWorker::publish_status(bool running,const std::string& error)
{
    Status next;
    next.running = running;
    next.role = core_.role();
    next.term = core_.hard_state().current_term;
    next.commit = committed_index_;
    next.last = log_.last_index();
    next.applied = machine_.applied();
    next.serving = barrier_ready_;
    next.updated_ms = static_cast<std::uint64_t>(std::chrono::duration_cast<std::chrono::milliseconds>(Clock::now().time_since_epoch()).count());
    next.error = error;

    std::lock_guard<std::mutex>lock(status_mutex_);
    status_ = std::move(next);
}

RaftWorker::Status RaftWorker::status() const
{
    std::lock_guard<std::mutex>lock(status_mutex_);
    return status_;
}

//主线程调用，启动后台与等待监听就绪
bool RaftWorker::start(std::string& error)
{
    error.clear();

    if(start_attempted_)
    {
        error = "raft worker : start already attempted";
        return false;
    }

    if(!core_.good())
    {
        error = "raft worker : init has not succeeded or core is stopped";
        return false;
    }

    start_attempted_ = true;
    stop_requested_.store(false);

    {
        std::lock_guard<std::mutex>lock(status_mutex_);
        startup_done_ = false;
    }

    try
    {
        worker_thread_ = std::thread(&RaftWorker::worker_loop,this);
    }
    catch(const std::exception& ex)
    {
        error = std::string("raft worker : start thread : ") + ex.what();
        core_.stop();
        publish_status(false,error);
        return false;
    }

    std::unique_lock<std::mutex>lock(status_mutex_);
    startup_cv_.wait(lock,[this]{return startup_done_;});

    const bool ok = status_.running;

    if(!ok)
    {
        error = status_.error.empty() ? "raft worker : stopped during startup" : status_.error;
    }

    lock.unlock();

    if(!ok)
    {
        stop();
    }

    return ok;

}

//后台运行循环
void RaftWorker::worker_loop()
{
    std::string error;
    runtime_error_.clear();

    try
    {
        transport_ = std::make_unique<RaftTransport>(config_,[this](std::uint64_t peer,RpcType type,std::uint64_t rpc_id,const std::string& body)
        {
            std::string message_error;

            if(!handle_message(peer,type,rpc_id,body,message_error))
            {
                runtime_error_ = std::move(message_error);
                core_.stop();
            }
        },
        [this](std::uint64_t peer)
        {
            on_peer_down(peer);
        });

        if(!transport_->start(error))
        {
            runtime_error_ = std::move(error);
        }
        else
        {
            reset_election_timer();
            heartbeat_deadline_ = Clock::now();
            publish_status(true,"");

            {
                std::lock_guard<std::mutex>lock(status_mutex_);
                startup_done_ = true;
            }

            startup_cv_.notify_all();

            while(!stop_requested_.load() && core_.good())
            {
                if(!transport_->poll(10,error))
                {
                    runtime_error_ = std::move(error);
                    break;
                }

                if(stop_requested_.load() || !core_.good())
                {
                    break;
                }

                const auto now = Clock::now();

                //先把排队请求变成日志，再推进已提交命令的执行
                if(!pump_clients(error))
                {
                    runtime_error_ = std::move(error);
                    break;
                }

                if(!pump_apply(error))
                {
                    runtime_error_ = std::move(error);
                    break;
                }

                if(core_.role() == Role::Leader)
                {
                    if(now >= heartbeat_deadline_)
                    {
                        heartbeat_deadline_ = now + std::chrono::milliseconds(150);

                        for(const auto& peer:config_.peers)
                        {
                            if(!send_append(peer.id,error))
                            {
                                runtime_error_ = std::move(error);
                                break;
                            }
                        }
                    }
                }
                else if(now >= election_deadline_)
                {
                    if(!start_election(error))
                    {
                        runtime_error_ = std::move(error);
                        break;
                    }
                }

                if(!core_.good())
                {
                    break;
                }

                publish_status(true,"");

            }

        }

    }
    catch(const std::exception& ex)
    {
        runtime_error_ = std::string("raft worker : runtime exception : ") + ex.what();
    }
    catch(...)
    {
        runtime_error_ = "raft worker : unknown runtime exception";
    }
    
    //停止时不能留下永远等不到回复的客户端
    abort_waiting();
    machine_.stop();

    if(transport_)
    {

        transport_->stop();
        transport_.reset();
    }

    core_.stop();
    publish_status(false,runtime_error_);

    {
        std::lock_guard<std::mutex>lock(status_mutex_);
        startup_done_ = true;
    }

    startup_cv_.notify_all();
}

void RaftWorker::stop()
{
    stop_requested_.store(true);

    if(worker_thread_.joinable())
    {
        worker_thread_.join();
    }

    core_.stop();
}



//Reactor 线程调用：只入队并通知，不在 Reactor 里等 Raft
bool RaftWorker::submit_client(ClientRequest request)
{
    if(request.fd < 0 || request.bytes > MAX_CLIENT_BYTES)
    {
        return false;
    }

    {
        std::lock_guard<std::mutex> lock(client_mutex_);

        if(stop_requested_.load() || !core_.good())
        {
            return false;
        }

        //请求和结果共用一份预算，避免慢连接把内存拖爆
        if(client_in_bytes_ + client_out_bytes_ + request.bytes > MAX_CLIENT_BYTES)
        {
            return false;
        }

        client_in_bytes_ += request.bytes;
        client_inbox_.push_back(std::move(request));
    }

    notify_reactor();
    return true;
}

//Reactor 线程调用：一次取走所有已完成的结果
std::vector<RaftWorker::ClientResult> RaftWorker::take_client_results()
{
    std::vector<ClientResult> out;
    std::lock_guard<std::mutex> lock(client_mutex_);

    out.swap(client_outbox_);
    client_out_bytes_ = 0;
    return out;
}

//调用方必须持有 client_mutex_；预算在入队时已经算过
void RaftWorker::answer_locked(ClientResult result)
{
    client_out_bytes_ += result.bytes;
    client_outbox_.push_back(std::move(result));
}

//退位、换届或停止：等待中的请求全部按失败回答，不能永久悬挂
void RaftWorker::abort_waiting()
{
    if(waiting_.empty())
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(client_mutex_);

        for(const auto& item : waiting_)
        {
            ClientResult out;
            out.fd = item.second.fd;
            out.connection_id = item.second.connection_id;
            out.sequence = item.second.sequence;
            out.bytes = item.second.bytes;
            out.response.status = kv::Status::Error;

            client_in_bytes_ -= item.second.bytes;
            answer_locked(std::move(out));
        }
    }

    waiting_.clear();
    notify_reactor();
}

//非 leader：排队中的请求立即拒绝，客户端不会一直等
void RaftWorker::reject_inbox()
{
    std::deque<ClientRequest> incoming;

    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        incoming.swap(client_inbox_);
    }

    if(incoming.empty())
    {
        return;
    }

    {
        std::lock_guard<std::mutex> lock(client_mutex_);

        for(const auto& item : incoming)
        {
            ClientResult out;
            out.fd = item.fd;
            out.connection_id = item.connection_id;
            out.sequence = item.sequence;
            out.bytes = item.bytes;
            out.response.status = kv::Status::Error;

            client_in_bytes_ -= item.bytes;
            answer_locked(std::move(out));
        }
    }

    notify_reactor();
}

//领导权变化、屏障和提案都在这里决定；返回 false 表示必须停止
bool RaftWorker::pump_clients(std::string& error)
{
    error.clear();

    const bool leader = (core_.role() == Role::Leader);
    const std::uint64_t term = core_.hard_state().current_term;

    //换届或退位：旧的等待上下文已经不可能由本次 leader 完成
    if(!leader || term != leader_term_)
    {
        abort_waiting();
        barrier_ready_ = false;
        barrier_index_ = 0;
        leader_term_ = leader ? term : 0;
    }

    if(!leader)
    {
        reject_inbox();
        return true;
    }

    //失去多数派时 committed 不再前进：等待中的请求必须超时失败，不能永久悬挂
    {
        const auto now = Clock::now();
        bool expired = false;

        for(auto it = waiting_.begin(); it != waiting_.end();)
        {
            if(now < it->second.deadline)
            {
                ++it;
                continue;
            }

            ClientResult out;
            out.fd = it->second.fd;
            out.connection_id = it->second.connection_id;
            out.sequence = it->second.sequence;
            out.bytes = it->second.bytes;
            out.response.status = kv::Status::Error;

            {
                std::lock_guard<std::mutex> lock(client_mutex_);
                client_in_bytes_ -= it->second.bytes;
                answer_locked(std::move(out));
            }

            it = waiting_.erase(it);
            expired = true;
        }

        if(expired)
        {
            notify_reactor();
        }
    }

    //新 leader 先提本任期 no-op，提交并执行完才放行客户端
    if(!barrier_ready_)
    {
        if(barrier_index_ == 0)
        {
            std::uint64_t index = 0;

            if(!propose_command(make_noop_command(),index,error))
            {
                return false;
            }

            barrier_index_ = index;
        }

        if(machine_.applied() >= barrier_index_)
        {
            barrier_ready_ = true;
        }
        else
        {
            return true;
        }
    }

    //一次最多提案 32 条，避免长时间不处理网络
    for(int round = 0; round < 32; ++round)
    {
        ClientRequest request;

        {
            std::lock_guard<std::mutex> lock(client_mutex_);

            if(client_inbox_.empty())
            {
                break;
            }

            request = std::move(client_inbox_.front());
            client_inbox_.pop_front();
        }

        std::string command;

        if(!encode_kv_command(request.request,command,error))
        {
            //坏命令只影响这一条，不影响节点
            std::lock_guard<std::mutex> lock(client_mutex_);
            ClientResult out;
            out.fd = request.fd;
            out.connection_id = request.connection_id;
            out.sequence = request.sequence;
            out.bytes = request.bytes;
            out.response.status = kv::Status::Error;

            client_in_bytes_ -= request.bytes;
            answer_locked(std::move(out));
            notify_reactor();
            continue;
        }

        std::uint64_t index = 0;

        if(!propose_command(command,index,error))
        {
            return false;
        }

        waiting_[index] = Waiter{request.fd,request.connection_id,
                                 request.sequence,term,request.bytes,
                                 Clock::now() + std::chrono::milliseconds(CLIENT_TIMEOUT_MS)};
    }

    return true;
}

//构造并保存一条本条目的日志；返回它的序号
bool RaftWorker::propose_command(const std::string& command,std::uint64_t& index,std::string& error)
{
    error.clear();

    LogEntry entry;

    if(!core_.build_proposal(command,entry,error))
    {
        return false;
    }

    //先落盘，成功后才让 Core 认为日志已经存在
    if(!log_.append(entry,error))
    {
        return false;
    }

    if(!core_.on_log_persisted(error))
    {
        core_.stop();
        return false;
    }

    CommitOut committed;

    if(!core_.advance_commit(committed,error))
    {
        return false;
    }

    if(committed.committed_to > committed_index_)
    {
        committed_index_ = committed.committed_to;
    }

    //尽快把新日志复制出去
    heartbeat_deadline_ = Clock::now();
    index = entry.index;
    return true;
}

//按顺序把已提交日志交给 KV，并把结果交回客户端
bool RaftWorker::pump_apply(std::string& error)
{
    error.clear();

    if(!machine_.good())
    {
        error = "raft worker : state machine unavailable";
        return false;
    }

    //先收上一条的执行结果
    bool done = false;
    ApplyResult result;

    if(!machine_.poll(done,result,error))
    {
        return false;
    }

    if(done)
    {
        publish_applied(result);
    }

    //空闲就继续提交下一条，缺口由 StateMachine 自己拒绝
    while(!machine_.busy() && machine_.applied() < committed_index_)
    {
        const std::uint64_t index = machine_.applied() + 1;
        LogEntry entry;

        if(!log_.get(index,entry,error))
        {
            return false;
        }

        if(!machine_.submit(index,entry.term,entry.command,done,result,error))
        {
            return false;
        }

        if(done)
        {
            publish_applied(result);
        }
    }

    return true;
}

//把一条执行结果交回对应的客户端；noop 和已断开连接没有结果
void RaftWorker::publish_applied(const ApplyResult& result)
{
    if(result.noop)
    {
        return;
    }

    const auto it = waiting_.find(result.index);

    if(it == waiting_.end())
    {
        return;
    }

    const Waiter waiter = it->second;
    waiting_.erase(it);

    ClientResult out;
    out.fd = waiter.fd;
    out.connection_id = waiter.connection_id;
    out.sequence = waiter.sequence;
    out.bytes = waiter.bytes;

    //任期对不上说明这条日志已被新 leader 覆盖，不能当作成功
    if(result.term != waiter.term)
    {
        out.response.status = kv::Status::Error;
    }
    else
    {
        out.response = result.response;
    }

    {
        std::lock_guard<std::mutex> lock(client_mutex_);
        client_in_bytes_ -= waiter.bytes;
        answer_locked(std::move(out));
    }

    notify_reactor();
}

}//namespace raft