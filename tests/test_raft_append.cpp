#include "raft/core/core.h"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <utility>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

using raft::AppendEntriesOut;
using raft::AppendEntriesRequest;
using raft::LogEntry;
using raft::RaftCore;
using raft::Role;

// 内存日志：只实现 LogView 需要的东西，不接触磁盘。
// entries 按 1..n 连续存放，下标 i 对应第 i+1 条。
class MemoryLog : public raft::LogView
{
public:
    std::vector<LogEntry> entries;

    // 让指定位置读失败，用来测错误路径；0 表示不注入
    std::uint64_t fail_term = 0;
    std::uint64_t fail_entry = 0;

    // 第 0 条不该被查询，这里计数用于断言
    mutable std::size_t zero_term_reads = 0;

    std::uint64_t last_index()const override
    {
        return entries.size();
    }

    bool term_at(std::uint64_t index,std::uint64_t& out)const override
    {
        if(index == 0)
        {
            zero_term_reads++;
            return false;
        }

        if(index == fail_term || index > entries.size())
        {
            return false;
        }

        out = entries[static_cast<std::size_t>(index-1)].term;
        return true;
    }

    bool entry_at(std::uint64_t index,LogEntry& out)const override
    {
        if(index == 0 || index == fail_entry || index > entries.size())
        {
            return false;
        }

        out = entries[static_cast<std::size_t>(index-1)];
        return true;
    }

    void add(std::uint64_t term,std::string command)
    {
        entries.push_back(LogEntry{entries.size()+1,term,std::move(command)});
    }
};

// 一个装好日志、绑定视图的节点；需要时先选成 leader
struct Node
{
    MemoryLog log; // 必须比 core 活得久：Core 只借用指针
    RaftCore core;
    std::string error;

    Node(std::size_t size,bool make_leader)
    {
        for(std::size_t i = 0 ; i < size ; i++)
        {
            // 前两条任期 1，后面任期 2，和已有用例的风格一致
            log.add(i < 2 ? 1 : 2,"cmd"+std::to_string(i));
        }

        CHECK(core.attach_log(&log,error));

        const std::uint64_t term = size == 0 ? 0 : log.entries.back().term;

        CHECK(core.init(1,{1,2,3},{term,0},size,term,error));

        if(make_leader)
        {
            elect();
        }
    }

    void elect()
    {
        raft::ElectionOutPut election;
        CHECK(core.start_election(election,error));

        // 外层模型：本轮 term/vote 已经保存成功，再投递一张回复
        CHECK(election.hard_state.current_term == core.hard_state().current_term);
        CHECK(election.hard_state.voted_for == 1);

        raft::VoteCountOutPut count;
        CHECK(core.handle_vote_response(2,{election.request.term,true},count,error));
        CHECK(count.become_leader && core.role() == Role::Leader);
    }
};

AppendEntriesRequest sentinel()
{
    AppendEntriesRequest req;
    req.term = 90;
    req.leader_id = 91;
    req.prev_log_index = 92;
    req.prev_log_term = 93;
    req.leader_commit = 94;
    req.entries.push_back(LogEntry{95,96,"old"});
    return req;
}

void check_same(const AppendEntriesRequest& a,const AppendEntriesRequest& b)
{
    CHECK(a.term == b.term && a.leader_id == b.leader_id);
    CHECK(a.prev_log_index == b.prev_log_index);
    CHECK(a.prev_log_term == b.prev_log_term);
    CHECK(a.leader_commit == b.leader_commit);
    CHECK(a.entries.size() == b.entries.size());

    for(std::size_t i = 0 ; i < a.entries.size() ; i++)
    {
        CHECK(a.entries[i].index == b.entries[i].index);
        CHECK(a.entries[i].term == b.entries[i].term);
        CHECK(a.entries[i].command == b.entries[i].command);
    }
}

// 失败时必须保持 out 原样，避免调用者把半成品发出去
void check_build_fails(Node& node,std::uint64_t peer,const char* reason)
{
    auto out = sentinel();
    const auto before = out;

    CHECK(!node.core.build_append_entries(peer,out,node.error));
    CHECK(node.error.find(reason) != std::string::npos);
    check_same(out,before);
}

void test_build_heartbeat()
{
    Node node(3,true); // 日志 1..3 任期 1,1,2；当选后任期 3

    AppendEntriesRequest req;
    node.error = "old error";

    CHECK(node.core.build_append_entries(2,req,node.error));
    CHECK(node.error.empty());

    // leader 信息来自本机，leader_commit 目前恒为 0
    CHECK(req.term == 3 && req.leader_id == 1 && req.leader_commit == 0);

    // next=4 表示没有新日志：前一条是 3，entries 为空
    CHECK(req.prev_log_index == 3 && req.prev_log_term == 2);
    CHECK(req.entries.empty());

    // 打包只读进度，不因为发了就认为对方已经收到
    CHECK(node.core.next_index(2) == 4);
    CHECK(node.core.match_index(2) == 0);

    std::cout << "PASS build heartbeat / prev term / progress unchanged\n";
}

void test_build_entries()
{
    Node node(0,true); // 空日志，任期 0 → 当选后任期 1

    for(int i = 0 ; i < 100 ; i++)
    {
        node.log.add(1,"x");
    }

    AppendEntriesRequest req;
    CHECK(node.core.build_append_entries(3,req,node.error));

    // 空日志：前一条按第 0 条处理，任期 0，且不去查日志
    CHECK(req.prev_log_index == 0 && req.prev_log_term == 0);
    CHECK(node.log.zero_term_reads == 0);

    // 单批最多 64 条，从第 1 条开始
    CHECK(req.entries.size() == 64);
    CHECK(req.entries.front().index == 1 && req.entries.back().index == 64);

    // 命令内容原样带出
    CHECK(req.entries.front().command == "x");

    std::cout << "PASS empty-log prev sentinel / 64-entry batch\n";
}

void test_build_byte_budget()
{
    {
        // 两条 600KB：第二条会超过 1MiB，本批只发第一条
        Node node(0,true);
        node.log.add(1,std::string(600*1024,'a'));
        node.log.add(1,std::string(600*1024,'b'));

        AppendEntriesRequest req;
        CHECK(node.core.build_append_entries(2,req,node.error));
        CHECK(req.entries.size() == 1);
        CHECK(req.entries[0].command.size() == 600*1024);
    }

    {
        // 512KB + 512KB 正好 1MiB，再下一条留给下一批
        Node node(0,true);
        node.log.add(1,std::string(512*1024,'a'));
        node.log.add(1,std::string(512*1024,'b'));
        node.log.add(1,"c");

        AppendEntriesRequest req;
        CHECK(node.core.build_append_entries(2,req,node.error));
        CHECK(req.entries.size() == 2);
    }

    {
        // 一条合法的 16MiB 也要能发出去，不能因为超过软预算被丢掉
        Node node(0,true);
        node.log.add(1,std::string(raft::LogStore::MAX_COMMAND_SIZE,'a'));
        node.log.add(1,"b");

        AppendEntriesRequest req;
        CHECK(node.core.build_append_entries(2,req,node.error));
        CHECK(req.entries.size() == 1);
        CHECK(req.entries[0].command.size() == raft::LogStore::MAX_COMMAND_SIZE);
    }

    std::cout << "PASS 1MiB soft budget / exact boundary / 16MiB singleton\n";
}

void test_build_rejects()
{
    // 不是 leader：Follower 与 Candidate 都要拒绝
    {
        Node follower(3,false);
        check_build_fails(follower,2,"not a leader");

        raft::ElectionOutPut election;
        CHECK(follower.core.start_election(election,follower.error));
        CHECK(follower.core.role() == Role::Candidate);
        check_build_fails(follower,2,"not a leader");
    }

    // 目标必须是别的成员：自己、不存在的 ID 都不行
    {
        Node node(3,true);
        check_build_fails(node,1,"not another member");
        check_build_fails(node,99,"not another member");
        check_build_fails(node,0,"not another member");
    }

    // 没有绑定日志视图
    {
        RaftCore core;
        std::string error;
        CHECK(core.init(1,{1,2,3},{4,0},3,2,error));

        raft::ElectionOutPut election;
        CHECK(core.start_election(election,error));
        raft::VoteCountOutPut count;
        CHECK(core.handle_vote_response(2,{5,true},count,error));
        CHECK(core.role() == Role::Leader);

        auto out = sentinel();
        const auto before = out;
        CHECK(!core.build_append_entries(2,out,error));
        CHECK(error.find("no log view") != std::string::npos);
        check_same(out,before);
    }

    // 视图读不到日志：不能发半截请求
    {
        Node node(3,true);
        node.log.add(3,"new");
        node.log.fail_entry = 4;

        check_build_fails(node,2,"cannot read entry");
    }

    // 视图读不到前一条的任期
    {
        Node node(3,true);
        node.log.fail_term = 3;

        check_build_fails(node,2,"cannot read previous term");
    }

    // 停止后不再打包
    {
        Node node(3,true);
        node.core.stop();
        check_build_fails(node,2,"not a leader");
    }

    std::cout << "PASS build rejects follower/candidate/self/nonmember/no-view/read-failure/stopped\n";
}

// 组装一条 leader 发来的请求
AppendEntriesRequest make_request(std::uint64_t term,std::uint64_t leader,
                                  std::uint64_t prev,std::uint64_t prev_term)
{
    AppendEntriesRequest req;
    req.term = term;
    req.leader_id = leader;
    req.prev_log_index = prev;
    req.prev_log_term = prev_term;
    return req;
}

void test_append_higher_term()
{
    Node node(3,false); // 当前任期 2，日志 1..3 任期 1,1,2
    CHECK(node.core.role() == Role::Follower);

    auto req = make_request(3,2,3,2);
    req.leader_commit = 2;

    AppendEntriesOut out;
    CHECK(node.core.handle_append_entries(2,req,out,node.error));

    // 任期变大：先退位并给出需要保存的状态，回复里用新任期
    CHECK(out.term == 3 && out.accept);
    CHECK(out.hard_state.has_value());
    CHECK(out.hard_state->current_term == 3 && out.hard_state->voted_for == 0);
    CHECK(node.core.hard_state().current_term == 3);

    // 合法 leader 的消息要重开选举计时器
    CHECK(out.reset_election_timer);

    // 前一条对得上，没有要删要加的日志
    CHECK(out.truncate_from == 0 && out.entries.empty());

    std::cout << "PASS append higher term / step down / save state / timer reset\n";
}

void test_append_low_term()
{
    Node node(3,false); // 当前任期 2

    auto req = make_request(1,2,3,1);

    AppendEntriesOut out;
    CHECK(node.core.handle_append_entries(2,req,out,node.error));

    // 过期的 leader：拒绝，但把本地任期告诉他
    CHECK(!out.accept && out.term == 2);
    CHECK(!out.hard_state.has_value());
    CHECK(!out.reset_election_timer);
    CHECK(out.truncate_from == 0 && out.entries.empty());

    std::cout << "PASS append stale term rejected without state change\n";
}

void test_append_candidate_steps_down()
{
    Node node(3,false);

    raft::ElectionOutPut election;
    CHECK(node.core.start_election(election,node.error));
    CHECK(node.core.role() == Role::Candidate);
    const auto term = node.core.hard_state().current_term;

    // 同一任期收到了 leader 的心跳：退出竞选，不改进度
    auto req = make_request(term,2,3,2);

    AppendEntriesOut out;
    CHECK(node.core.handle_append_entries(2,req,out,node.error));

    CHECK(out.accept && out.term == term);
    CHECK(!out.hard_state.has_value());
    CHECK(node.core.role() == Role::Follower);
    CHECK(node.core.hard_state().voted_for == 1); // 本轮自票保留

    std::cout << "PASS append same term makes candidate a follower\n";
}

void test_append_prev_mismatch()
{
    {
        // 前一条任期不同：正常拒绝，但要重置计时器
        Node node(3,false);
        auto req = make_request(3,2,3,1);

        AppendEntriesOut out;
        CHECK(node.core.handle_append_entries(2,req,out,node.error));
        CHECK(!out.accept && out.reset_election_timer);
        CHECK(out.truncate_from == 0 && out.entries.empty());
    }

    {
        // 本地还没到这么远：同样拒绝
        Node node(3,false);
        auto req = make_request(3,2,9,3);

        AppendEntriesOut out;
        CHECK(node.core.handle_append_entries(2,req,out,node.error));
        CHECK(!out.accept && out.reset_election_timer);
    }

    std::cout << "PASS append prev mismatch rejected with timer reset\n";
}

void test_append_conflict()
{
    {
        // 本地 1..3 是 1,1,2；leader 说第 3 条任期 3 → 从第 3 条开始替换
        Node node(3,false);
        auto req = make_request(3,2,2,1);
        req.entries.push_back(LogEntry{3,3,"new3"});
        req.entries.push_back(LogEntry{4,3,"new4"});

        AppendEntriesOut out;
        CHECK(node.core.handle_append_entries(2,req,out,node.error));

        CHECK(out.accept && out.truncate_from == 3);
        CHECK(out.entries.size() == 2);
        CHECK(out.entries[0].index == 3 && out.entries[1].index == 4);
        CHECK(out.entries[0].term == 3 && out.entries[1].term == 3);
    }

    {
        // 本地 1..4；前三条一致，第 4 条一致，第 5 条之后本地没有
        Node node(4,false);
        auto req = make_request(3,2,3,2);
        req.entries.push_back(LogEntry{4,2,"same"});
        req.entries.push_back(LogEntry{5,3,"new5"});
        req.entries.push_back(LogEntry{6,3,"new6"});

        AppendEntriesOut out;
        CHECK(node.core.handle_append_entries(2,req,out,node.error));

        CHECK(out.accept && out.truncate_from == 5);
        CHECK(out.entries.size() == 2);
        CHECK(out.entries[0].index == 5 && out.entries[1].index == 6);
    }

    {
        // 重复收到已经存在的日志：不截断、不重复追加
        Node node(5,false); // 1,1,2,2,2
        auto req = make_request(3,2,3,2);
        req.entries.push_back(LogEntry{4,2,"same"});

        AppendEntriesOut out;
        CHECK(node.core.handle_append_entries(2,req,out,node.error));

        CHECK(out.accept && out.truncate_from == 0 && out.entries.empty());
    }

    std::cout << "PASS conflict truncation / tail append / duplicate batch keeps suffix\n";
}

void test_append_rejects()
{
    // 来源必须是别的成员，且 leader_id 必须就是发送方
    {
        Node node(3,false);
        AppendEntriesOut out;
        const auto before = out;

        auto from_self = make_request(3,1,3,2);
        CHECK(!node.core.handle_append_entries(1,from_self,out,node.error));
        CHECK(node.error.find("not another member") != std::string::npos);

        auto from_unknown = make_request(3,99,3,2);
        CHECK(!node.core.handle_append_entries(99,from_unknown,out,node.error));

        auto forged = make_request(3,3,3,2); // 发送方 2，却说自己是 3
        CHECK(!node.core.handle_append_entries(2,forged,out,node.error));
        CHECK(node.error.find("leader id mismatch") != std::string::npos);

        // 失败时不改 out
        CHECK(out.term == before.term && out.accept == before.accept);
        CHECK(out.truncate_from == before.truncate_from);
        CHECK(out.entries.empty() && !out.hard_state.has_value());
    }

    // 任期 0 不是合法 leader
    {
        Node node(3,false);
        auto req = make_request(0,2,3,2);

        AppendEntriesOut out;
        CHECK(!node.core.handle_append_entries(2,req,out,node.error));
        CHECK(node.error.find("zero term") != std::string::npos);
    }

    // 没有日志视图：整条拒绝，且不能留下“改了任期却没保存”的中间状态
    {
        RaftCore core;
        std::string error;
        CHECK(core.init(1,{1,2,3},{2,0},3,2,error));
        CHECK(core.hard_state().current_term == 2);

        auto req = make_request(3,2,3,2); // 任期更高：正常路径会先升任期

        AppendEntriesOut out;
        CHECK(!core.handle_append_entries(2,req,out,error));
        CHECK(error.find("no log view") != std::string::npos);

        // 失败时调用者拿不到 out.hard_state，所以内存里也不能已经升过任期
        CHECK(core.hard_state().current_term == 2);
        CHECK(core.hard_state().voted_for == 0);
        CHECK(core.role() == Role::Follower);
    }

    // 冲突位置在已提交区间之内：不能删，停止
    // 说明：commit_index_ 现在恒为 0，这条路径要等提交推进实现后才能构造，
    //       先用注释占位，等 M4 补断言。

    std::cout << "PASS append rejects bad sender / forged leader / zero term / no view\n";
}

} // namespace

int main()
{
    test_build_heartbeat();
    test_build_entries();
    test_build_byte_budget();
    test_build_rejects();
    test_append_higher_term();
    test_append_low_term();
    test_append_candidate_steps_down();
    test_append_prev_mismatch();
    test_append_conflict();
    test_append_rejects();
    std::cout << "raft append-entries build/receive tests PASS (memory log, no socket, no disk)\n";
}
