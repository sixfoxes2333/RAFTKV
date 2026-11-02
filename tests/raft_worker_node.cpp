// M7 进程夹具：只驱动仓库里的 RaftWorker，不实现另一份 Raft，也不提供 KV 服务。
#include "raft/worker.h"

#include <atomic>
#include <chrono>
#include <csignal>
#include <iostream>
#include <string>
#include <thread>
#include <unistd.h>

namespace {
std::atomic<bool> stop_requested{false};
static_assert(std::atomic<bool>::is_always_lock_free);

// 信号处理只设置无锁原子标志，主线程负责 stop/join。
void request_stop(int)
{
    stop_requested.store(true,std::memory_order_relaxed);
}

// 输出的角色来自 Worker 的同一份快照。
const char* role_name(raft::Role role)
{
    if(role == raft::Role::Leader) return "leader";
    if(role == raft::Role::Candidate) return "candidate";
    return "follower";
}
}

int main(int argc,char** argv)
{
    if(argc != 4)
    {
        std::cerr << "usage: raft_worker_node ID PEERS DATA_DIR\n";
        return 2;
    }
    std::signal(SIGTERM,request_stop);
    std::signal(SIGINT,request_stop);
    raft::RaftConfig config;
    std::string error;
    if(!raft::parse_raft_config(argv[1],argv[2],argv[3],config,error))
    {
        std::cerr << "CONFIGFAIL " << error << '\n';
        return 2;
    }
    raft::RaftWorker worker(config);
    if(!worker.init(error) || !worker.start(error))
    {
        std::cerr << "STARTFAIL " << error << '\n';
        return 1;
    }
    std::cout << "READY id=" << config.self_id << " pid=" << ::getpid() << std::endl;
    bool failed = false;
    while(!stop_requested.load(std::memory_order_relaxed))
    {
        const auto status = worker.status();
        std::cout << "RAFTSTATUS id=" << config.self_id
                  << " pid=" << ::getpid()
                  << " role=" << role_name(status.role)
                  << " term=" << status.term
                  << " commit=" << status.commit
                  << " last=" << status.last
                  << " running=" << (status.running ? 1 : 0)
                  << " updated_ms=" << status.updated_ms << std::endl;
        if(!status.running)
        {
            std::cerr << "WORKERFAIL " << status.error << std::endl;
            failed = true;
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    worker.stop();
    std::cout << "STOPPED id=" << config.self_id << " pid=" << ::getpid() << std::endl;
    return failed ? 1 : 0;
}
