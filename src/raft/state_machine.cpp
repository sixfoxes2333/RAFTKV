#include "raft/state_machine.h"
#include <iostream>
#include <utility>

namespace raft
{

namespace
{
//派生 KV 与 Raft 文件放在同一个加锁的数据目录
const char* const CACHE_NAME = "/kv.cache";
}

StateMachine::StateMachine(const std::string& data_dir):data_dir_(data_dir)
{

}

StateMachine::~StateMachine()
{
    stop();
}

//重建派生缓存
bool StateMachine::init(std::string& error)
{
    error.clear();

    if(worker_ != nullptr)
    {
        error = "state machine : init already attempted";
        return false;
    }

    if(data_dir_.empty() || data_dir_.back() == '/')
    {
        error = "state machine : invalid data directory";
        return false;
    }

    try
    {
        worker_ = std::make_unique<kv::StorageWorker>(data_dir_ + CACHE_NAME,8,kv::StorageWorker::SyncMode::Off,kv::Storage::OpenMode::Rebuild);

    }
    catch(const std::exception& ex)
    {
        error = std::string("state machine : create KV worker : ") + ex.what();
        return false;
    }

    if(!worker_->ready())
    {
        worker_.reset();
        error = "state machine : derived KV could not be rebuilt";
        return false;
    }

    applied_ = 0;
    inflight_ = false;
    return true;

}

//执行一条命令，先核对序号连续，再交给 KV 线程或者当场跳过 noop
bool StateMachine::submit(std::uint64_t index,std::uint64_t term,const std::string& command,bool& done,ApplyResult& result,std::string& error)
{
    error.clear();

    done = false;

    if(!good())
    {
        error = "state machine : not usable";
        return false;
    }

    if(inflight_)
    {
        error = "state machine L previous command is still applying";
        return false;
    }

    //只允许连续执行，缺口意味着提交返回判断出错
    if(index != applied_ + 1)
    {
        error = "state machine : apply index is not contiguous";
        return false;
    }

    Command decoded;

    if(!decode_command(command,decoded,error))
    {
        return false;
    }

    if(decoded.noop)
    {
        //noop 只推进执行位置，不访问 KV
        applied_ = index;
        result.index = index;
        result.term = term;
        result.noop = true;
        result.response = kv::Response{};
        done = true;
        return true;
    }

    kv::StorageTask task;
    task.fd = -1;
    task.connection_id = 0;
    task.sequence = index;
    task.request = std::move(decoded.request);
    task.bytes = command.size();

    if(!worker_->submit(std::move(task)))
    {
        error = "state machine : KV worker rejected the command";
        return false;
    }

    pending_.index = index;
    pending_.term = term;
    pending_.noop = false;
    pending_.response = kv::Response{};
    inflight_ = true;
    return true;
}

//取一条完成结果，执行出错时返回 false
bool StateMachine::poll(bool& done,ApplyResult& result,std::string& error)
{
    error.clear();
    done = false;

    if(worker_ == nullptr)
    {
        return true;
    }

    auto completed = worker_->take_completed();

    if(completed.empty())
    {
        //线程还活着就继续等，否则这一轮执行已经不可能完成
        if(!worker_->ready())
        {
            error = "state machine : KV worker stopped while applying";
            return false;
        }

        return true;
    }

    if(!inflight_ || completed.size() != 1)
    {
        error = "state machine : unexpected KV completion";
        return false;
    }

    const auto& finished = completed.front();

    if(finished.sequence != pending_.index)
    {
        error = "state machine : KV completion dose not match the submitted index";
        return false;
    }

    //写失败时 KV 可能已经部分改变，不能当作正常结果继续
    if(finished.response.status == kv::Status::Error)
    {
        error = "state machine : KV execution failed";
        return false;
    }

    ApplyResult next;
    next.index = pending_.index;
    next.term = pending_.term;
    next.noop = false;
    next.response = finished.response;

    applied_ = pending_.index;
    inflight_ = false;
    pending_ = ApplyResult{};

    result = std::move(next);
    done = true;
    return true;
}

void StateMachine::stop()
{
    worker_.reset();
    inflight_ = false;
    pending_ = ApplyResult{};
}

}//namespace raft