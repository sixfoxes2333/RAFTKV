#ifndef RAFT_STATE_MACHINE_H
#define RAFT_STATE_MACHINE_H

#include "app/storage_worker.h"
#include "raft/command.h"
#include <cstdint>
#include <memory>
#include <string>

namespace raft
{

//一条已提交命令执行完后的结果;noop 不产生客户端响应
struct ApplyResult
{
    std::uint64_t index = 0;
    std::uint64_t term = 0;
    bool noop = false;
    kv::Response response;
};

//持有派生 KV 与 lastApplied ，只执行已经确认提交的连续日志
class StateMachine
{
public:
    explicit StateMachine(const std::string& data_dir);
    ~StateMachine();

    StateMachine(const StateMachine&) = delete;
    StateMachine& operator=(const StateMachine&) = delete;
    StateMachine(StateMachine&&) = delete;
    StateMachine& operator=(StateMachine&&) = delete;

    //重建派生 KV,lastapplied 从 0 开始
    bool init(std::string& error);

    bool good()const
    {
        return worker_ != nullptr && worker_->ready();
    }

    //已经执行到了哪一条
    std::uint64_t applied()const
    {
        return applied_;
    }

    //是否有一条命令正在 KV 线程中执行
    bool busy() const
    {
        return inflight_;
    }

    //提交下一条连续命令 ; noop 当场完成， done = true 表示 result 已可用
    bool submit(std::uint64_t index,std::uint64_t term,const std::string& command,bool& done,ApplyResult& result,std::string& error);

    //取一条执行结果 done = false 表示未执行完
    bool poll(bool& done,ApplyResult& result,std::string& error);

    //停止后台线程
    void stop();

private:

    std::string data_dir_;
    std::unique_ptr<kv::StorageWorker>worker_;

    std::uint64_t applied_ = 0;
    bool inflight_ = false; //KV 线程中是否有一条命令
    ApplyResult pending_; //对应那条命令的序号，任期和 noop

};


}//namspace raft

#endif