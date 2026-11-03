// M8：状态机只执行连续已提交命令，执行完才推进 lastApplied。
#include "raft/node/state_machine.h"
#include "raft/protocol/command.h"
#include <chrono>
#include <cstdlib>
#include <iostream>
#include <string>
#include <thread>

#define CHECK(x) do { if(!(x)) { std::cerr << __FILE__ << ':' << __LINE__ \
    << ": FAIL " #x << '\n'; std::exit(1); } } while(false)

namespace {
using Clock = std::chrono::steady_clock;

//等待一条命令执行完；超时就是真的卡住。
bool wait_done(raft::StateMachine& machine,raft::ApplyResult& result,int ms = 3000)
{
    const auto deadline = Clock::now() + std::chrono::milliseconds(ms);
    while(Clock::now() < deadline)
    {
        bool done = false;
        std::string error;
        CHECK(machine.poll(done,result,error));
        if(done) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(2));
    }
    return false;
}

std::string temp_dir()
{
    char name[] = "/tmp/raft-sm.XXXXXX";
    CHECK(::mkdtemp(name) != nullptr);
    return name;
}

//提交一条非 noop 命令并等到结果。
raft::ApplyResult apply(raft::StateMachine& machine,std::uint64_t index,
                        std::uint64_t term,const kv::Request& request)
{
    std::string command,error;
    CHECK(raft::encode_kv_command(request,command,error));
    bool done = false;
    raft::ApplyResult result;
    CHECK(machine.submit(index,term,command,done,result,error));
    if(done)
    {
        CHECK(index == result.index && result.term == term);
        return result;
    }
    CHECK(machine.busy());
    CHECK(wait_done(machine,result));
    CHECK(result.index == index && result.term == term && !result.noop);
    return result;
}

//启动时清空旧派生数据，lastApplied 从 0 开始。
void fresh_start()
{
    const auto dir = temp_dir();
    {
        raft::StateMachine machine(dir);
        std::string error;
        CHECK(machine.init(error));
        CHECK(machine.good() && machine.applied() == 0 && !machine.busy());
        CHECK(apply(machine,1,3,{kv::Op::Set,"k","stale"}).response.status == kv::Status::Ok);
        CHECK(machine.applied() == 1);
    }
    {
        raft::StateMachine machine(dir);
        std::string error;
        CHECK(machine.init(error));
        CHECK(machine.applied() == 0 && machine.good());
        const auto missing = apply(machine,1,9,{kv::Op::Get,"k",""});
        CHECK(missing.response.status == kv::Status::NotFound);
        std::cout << "PASS state machine: rebuild resets applied and stale data\n";
    }
}

//noop 只推进位置，不产生客户端响应，也不占用 KV 线程。
void noop_advances()
{
    const auto dir = temp_dir();
    raft::StateMachine machine(dir);
    std::string error;
    CHECK(machine.init(error));
    bool done = false;
    raft::ApplyResult result;
    result.response.status = kv::Status::Error;
    CHECK(machine.submit(1,5,raft::make_noop_command(),done,result,error));
    CHECK(done && result.noop && result.index == 1 && result.term == 5);
    CHECK(result.response.status == kv::Status::Error);
    CHECK(machine.applied() == 1 && !machine.busy());
    CHECK(machine.submit(2,5,raft::make_noop_command(),done,result,error));
    CHECK(done && machine.applied() == 2);
    std::cout << "PASS state machine: noop advances position without touching KV\n";
}

//结果必须对应本节点自己的执行，读回的值来自真实 KV。
void ordered_execution()
{
    const auto dir = temp_dir();
    raft::StateMachine machine(dir);
    std::string error;
    CHECK(machine.init(error));
    CHECK(apply(machine,1,2,{kv::Op::Set,"a","1"}).response.status == kv::Status::Ok);
    CHECK(apply(machine,2,2,{kv::Op::Set,"a","2"}).response.status == kv::Status::Ok);
    const auto hit = apply(machine,3,2,{kv::Op::Get,"a",""});
    CHECK(hit.response.status == kv::Status::Ok && hit.response.value == "2");
    CHECK(apply(machine,4,2,{kv::Op::Del,"a",""}).response.status == kv::Status::Ok);
    const auto gone = apply(machine,5,2,{kv::Op::Get,"a",""});
    CHECK(gone.response.status == kv::Status::NotFound);
    CHECK(apply(machine,6,2,{kv::Op::Del,"a",""}).response.status == kv::Status::NotFound);
    CHECK(machine.applied() == 6);
    std::cout << "PASS state machine: SET/GET/DEL applied in order with real values\n";
}

//缺口、重复和坏命令都必须拒绝，并且不改变执行位置。
void rejected_commands()
{
    const auto dir = temp_dir();
    raft::StateMachine machine(dir);
    std::string error;
    CHECK(machine.init(error));
    bool done = false;
    raft::ApplyResult result;
    std::string set;
    CHECK(raft::encode_kv_command({kv::Op::Set,"k","v"},set,error));
    CHECK(!machine.submit(2,1,set,done,result,error));
    CHECK(!done && error.find("contiguous") != std::string::npos);
    CHECK(machine.applied() == 0 && !machine.busy());
    error.clear();
    CHECK(!machine.submit(1,1,"",done,result,error));
    CHECK(!done && machine.applied() == 0);
    error.clear();
    CHECK(!machine.submit(1,1,std::string("\0x",2),done,result,error));
    CHECK(!done && machine.applied() == 0);
    error.clear();
    CHECK(!machine.submit(1,1,std::string("\x04",1),done,result,error));
    CHECK(!done && machine.applied() == 0);
    CHECK(apply(machine,1,1,{kv::Op::Set,"k","v"}).response.status == kv::Status::Ok);
    CHECK(!machine.submit(4,1,raft::make_noop_command(),done,result,error));
    CHECK(!done && machine.applied() == 1);
    error.clear();
    //上一条都已完成后，紧接着的下一条连续命令必须被接受。
    CHECK(machine.submit(2,1,raft::make_noop_command(),done,result,error));
    CHECK(done && machine.applied() == 2);
    std::cout << "PASS state machine: gaps, duplicates and bad commands rejected\n";
}
}

int main()
{
    fresh_start();
    noop_advances();
    ordered_execution();
    rejected_commands();
    std::cout << "raft state machine PASS (4 groups; committed apply only)\n";
}
