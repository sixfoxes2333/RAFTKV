// M6 第3步：入站语义校验
//
// 这份测试分两半：
//   前半：规则表，用 check() 收集所有失败一口气报出来
//   后半：真实存储 + 真实 Core，证明"绕过校验会发生什么"
// 前半故意不用 fail-fast，因为同时可能有几条规则写错。

#include "raft/apply.h"
#include "raft/logview.h"
#include "raft/rpc_validate.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

using raft::AppendEntriesRequest;
using raft::LogEntry;
using raft::RequestVoteRequest;
using raft::Role;

using raft::validate_append_entries;

int failures = 0;

void check(bool ok,const std::string& what)
{
    if(!ok)
    {
        std::cerr << "FAIL: " << what << '\n';
        ++failures;
    }
}

RequestVoteRequest vote(std::uint64_t term,std::uint64_t candidate,
                        std::uint64_t last_index,std::uint64_t last_term)
{
    RequestVoteRequest request;
    request.term = term;
    request.candidate_id = candidate;
    request.last_log_index = last_index;
    request.last_log_term = last_term;
    return request;
}

AppendEntriesRequest append(std::uint64_t term,std::uint64_t leader,
                            std::uint64_t prev_index,std::uint64_t prev_term)
{
    AppendEntriesRequest request;
    request.term = term;
    request.leader_id = leader;
    request.prev_log_index = prev_index;
    request.prev_log_term = prev_term;
    request.leader_commit = 0;
    return request;
}

// ---------- 投票请求 ----------

void test_vote_rules()
{
    std::string error;

    // 合法：全新节点的空日志（index 0 且 term 0）
    check(raft::validate_vote_request(vote(1,2,0,0),error),"vote: empty log (0,0) must be valid");
    // 合法：有日志
    check(raft::validate_vote_request(vote(5,2,130,5),error),"vote: (130,5) must be valid");
    // 合法：日志尾任期小于当前任期（落后节点也能参选）
    check(raft::validate_vote_request(vote(9,2,40,3),error),"vote: tail term older than request term must be valid");

    // 非法：任期 0
    check(!raft::validate_vote_request(vote(0,2,0,0),error),"vote: zero term must be refused");
    // 非法：候选人 ID 0
    check(!raft::validate_vote_request(vote(3,0,0,0),error),"vote: zero candidate id must be refused");
    // 非法：空日志却带任期
    check(!raft::validate_vote_request(vote(3,2,0,7),error),"vote: empty log with non-zero term must be refused");
    // 非法：有日志却任期 0
    check(!raft::validate_vote_request(vote(3,2,25,0),error),"vote: non-empty log with zero term must be refused");
}

// ---------- 复制请求 ----------

void test_append_rules()
{
    std::string error;

    // 合法：空日志上的心跳
    check(validate_append_entries(append(1,1,0,0),error),"append: heartbeat (0,0) must be valid");
    // 合法：有日志上的心跳
    check(validate_append_entries(append(4,1,20,3),error),"append: heartbeat (20,3) must be valid");

    // 合法：一条与请求同任期的条目
    {
        auto request = append(5,1,10,4);
        request.entries.push_back(LogEntry{0,5,"a"});
        check(validate_append_entries(request,error),"append: single entry with request term must be valid");
    }

    // 合法：任期非递减的多条（3 → 3 → 5）
    {
        auto request = append(5,1,10,3);
        request.entries.push_back(LogEntry{0,3,"a"});
        request.entries.push_back(LogEntry{0,3,"b"});
        request.entries.push_back(LogEntry{0,5,"c"});
        check(validate_append_entries(request,error),"append: non-decreasing entry terms must be valid");
    }

    // 合法：条目任期等于前一条的任期
    {
        auto request = append(6,1,30,6);
        request.entries.push_back(LogEntry{0,6,"x"});
        check(validate_append_entries(request,error),"append: entry term equal to prev term must be valid");
    }

    // 非法：任期 0
    check(!validate_append_entries(append(0,1,0,0),error),"append: zero term must be refused");
    // 非法：leader ID 0
    check(!validate_append_entries(append(3,0,0,0),error),"append: zero leader id must be refused");
    // 非法：第 0 条却带任期
    check(!validate_append_entries(append(3,1,0,5),error),"append: index 0 with prev term 5 must be refused");
    // 非法：前一条存在却任期 0
    check(!validate_append_entries(append(3,1,7,0),error),"append: prev index 7 with term 0 must be refused");

    // 非法：条目任期 0
    {
        auto request = append(4,1,0,0);
        request.entries.push_back(LogEntry{0,0,"z"});
        check(!validate_append_entries(request,error),"append: entry term 0 must be refused");
    }

    // 非法：条目任期高于请求任期 —— 就是复审 R3 那一项
    {
        auto request = append(3,1,0,0);
        request.entries.push_back(LogEntry{0,4,"poison"});
        check(!validate_append_entries(request,error),"append: entry term 4 under request term 3 must be refused");
    }

    // 非法：条目任期递减 5 → 3
    {
        auto request = append(5,1,0,0);
        request.entries.push_back(LogEntry{0,5,"a"});
        request.entries.push_back(LogEntry{0,3,"b"});
        check(!validate_append_entries(request,error),"append: decreasing entry terms must be refused");
    }

    // 非法：条目任期低于前一条（prev term 6，条目 5）
    {
        auto request = append(6,1,10,6);
        request.entries.push_back(LogEntry{0,5,"a"});
        check(!validate_append_entries(request,error),"append: entry term below prev term must be refused");
    }

    // 非法：位置溢出（prev 接近 UINT64_MAX 还带条目）
    {
        auto request = append(9,1,std::numeric_limits<std::uint64_t>::max() - 1,4);
        request.entries.push_back(LogEntry{0,9,"a"});
        request.entries.push_back(LogEntry{0,9,"b"});
        check(!validate_append_entries(request,error),"append: log position overflow must be refused");
    }
}

void test_error_flag()
{
    std::string error = "stale";
    (void)raft::validate_vote_request(vote(1,2,0,0),error);
    check(error.empty(),"vote: success must clear an old error");

    error = "stale";
    (void)validate_append_entries(append(1,1,0,0),error);
    check(error.empty(),"append: success must clear an old error");

    error.clear();
    (void)raft::validate_vote_request(vote(0,2,0,0),error);
    check(!error.empty(),"vote: failure must set error");

    error.clear();
    (void)validate_append_entries(append(0,1,0,0),error);
    check(!error.empty(),"append: failure must set error");
}

// ---------- 后半：真实存储证明绕过校验的后果 ----------

struct TempDir {
    std::string path;
    TempDir() {
        char pattern[] = "/tmp/raft-rpc-validate-XXXXXX";
        const char* made = ::mkdtemp(pattern);
        CHECK(made != nullptr);
        path = made;
    }
    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path,ignored);
    }
};

// 一个刚启动的 Follower：任期 3，日志为空
struct Node {
    TempDir dir;
    raft::LogStore log;
    raft::StateStore state;
    raft::LogStoreView view;
    raft::RaftCore core;
    std::string error;

    Node() : log(dir.path+"/log"),state(dir.path+"/state"),view(log)
    {
        CHECK(std::filesystem::create_directory(dir.path+"/log"));
        CHECK(std::filesystem::create_directory(dir.path+"/state"));
        CHECK(log.create(error));
        CHECK(state.save({3,0},error));

        raft::HardState recovered;
        CHECK(state.load(recovered,error));
        CHECK(core.attach_log(&view,error));
        CHECK(core.init(2,{1,2,3},recovered,0,0,error));
    }
};

void test_core_writes_what_validate_refuses()
{
    Node node;

    // term 3 的请求，却带一条 term 4 的条目
    auto poison = append(3,1,0,0);
    poison.entries.push_back(LogEntry{0,4,"poison"});

    // 1) 校验层应当拒绝它
    std::string error;
    check(!validate_append_entries(poison,error),"evidence: validate must refuse entry term 4 under term 3");
    check(!error.empty(),"evidence: refusal must carry an error message");

    // 2) 绕过校验直接喂 Core：它确实会写进日志并回 success
    raft::AppendEntriesOut action;
    CHECK(node.core.handle_append_entries(1,poison,action,node.error));
    CHECK(action.accept);

    raft::AppendEntriesResponse response;
    CHECK(raft::apply_append_entries(node.core,node.state,node.log,action,response,node.error));
    check(response.success,"evidence: Core accepts and ACKs the invalid entry");

    LogEntry written;
    CHECK(node.log.get(1,written,node.error));
    check(written.term == 4,"evidence: the term-4 entry really landed in the log");
    check(written.command == "poison","evidence: command content is preserved");

    // 3) 重启等价：用存储里的值重建 Core，必须因为"日志尾任期 > 当前任期"被拒绝
    raft::HardState recovered;
    CHECK(node.state.load(recovered,node.error));
    check(recovered.current_term == 3,"evidence: saved term stayed 3");

    std::uint64_t tail_term = 0;
    CHECK(node.log.term_at(1,tail_term,node.error));
    check(tail_term == 4,"evidence: log tail term is 4");

    raft::RaftCore restored;
    raft::LogStoreView restored_view(node.log);
    CHECK(restored.attach_log(&restored_view,node.error));
    check(!restored.init(2,{1,2,3},recovered,1,tail_term,node.error),
          "evidence: restart must refuse the inconsistent term pair");
    check(!restored.good(),"evidence: refused restart leaves the Core unusable");
    check(restored.role() == Role::Follower,"evidence: refused restart does not become anything else");
    check(!node.error.empty(),"evidence: refused restart explains why");

    std::cout << "PASS evidence: without validation a term-4 entry is written and ACKed, then restart refuses\n";
}

// 对照：如果先过校验，同样一条请求连 Core 都进不去
void test_validation_blocks_before_core()
{
    Node node;

    auto poison = append(3,1,0,0);
    poison.entries.push_back(LogEntry{0,4,"poison"});

    std::string error;
    check(!validate_append_entries(poison,error),"blocked: validate refuses the request");

    // 校验失败时不调用 Core，日志保持为空，节点照常可用
    check(node.log.last_index() == 0,"blocked: log stays empty");
    check(node.core.good(),"blocked: core is not stopped by a bad frame");
    check(node.core.role() == Role::Follower,"blocked: role unchanged");

    std::cout << "PASS drop-before-Core: refusal leaves log empty and core healthy\n";
}

} // namespace

int main()
{
    test_vote_rules();
    test_append_rules();
    test_error_flag();
    test_core_writes_what_validate_refuses();
    test_validation_blocks_before_core();

    if(failures != 0)
    {
        std::cerr << failures << " failures\n";
        return 1;
    }

    std::cout << "raft rpc validate PASS (rules + real stores; no sockets)\n";
    return 0;
}
