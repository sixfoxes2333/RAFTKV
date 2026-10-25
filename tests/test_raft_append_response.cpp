#include "raft/core.h"

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
using raft::AppendEntriesRequest;
using raft::AppendEntriesResponse;
using raft::AppendResponseOut;
using raft::HardState;
using raft::LogEntry;
using raft::RaftCore;
using raft::Role;

class MemoryLog final : public raft::LogView {
public:
    std::vector<LogEntry> entries;
    std::uint64_t last_index() const override { return entries.size(); }
    bool term_at(std::uint64_t index,std::uint64_t& out) const override {
        if(index == 0 || index > entries.size()) return false;
        out = entries[static_cast<std::size_t>(index-1)].term;
        return true;
    }
    bool entry_at(std::uint64_t index,LogEntry& out) const override {
        if(index == 0 || index > entries.size()) return false;
        out = entries[static_cast<std::size_t>(index-1)];
        return true;
    }
    void add(std::uint64_t term,std::string command) {
        entries.push_back({last_index()+1,term,std::move(command)});
    }
};

struct Node {
    // 声明顺序保证：先销毁 Core，再销毁借给它的日志视图。
    MemoryLog log;
    RaftCore core;
    HardState saved{}; // 仅是“保存成功”的内存模型，不是 StateStore/磁盘
    std::uint64_t id;
    std::string error;

    Node(std::uint64_t node_id,std::size_t count,
         const std::vector<std::uint64_t>& members = {1,2,3},
         std::uint64_t restored_vote = 0) : id(node_id) {
        for(std::size_t i = 1; i <= count; ++i)
            log.add(i <= 2 ? 1 : 2,"cmd"+std::to_string(i));
        saved = {4,restored_vote};
        CHECK(core.attach_log(&log,error));
        const auto last_term = count == 0 ? 0 : log.entries.back().term;
        CHECK(core.init(id,members,saved,count,last_term,error));
    }
    AppendEntriesRequest build(std::uint64_t peer) {
        AppendEntriesRequest sent;
        CHECK(core.build_append_entries(peer,sent,error));
        return sent;
    }
    AppendResponseOut reply(std::uint64_t peer,const AppendEntriesRequest& sent,
                            std::uint64_t term,bool success) {
        AppendResponseOut out;
        CHECK(core.handle_append_entries_response(peer,sent,{term,success},out,error));
        CHECK(error.empty());
        return out;
    }
    void grow(std::size_t count) {
        for(std::size_t i = 0; i < count; ++i)
            log.add(core.hard_state().current_term,"new"+std::to_string(log.last_index()+1));
        // 测试模型先完成日志变动，再通知 Core，模拟外层保存完成。
        CHECK(core.on_log_persisted(error));
    }
};

void campaign(Node& leader,Node& voter) {
    raft::ElectionOutPut election;
    CHECK(leader.core.start_election(election,leader.error));
    leader.saved = election.hard_state;
    raft::VoteOutPut vote;
    CHECK(voter.core.handle_request_vote(leader.id,election.request,vote,voter.error));
    if(vote.hard_state) voter.saved = *vote.hard_state;
    CHECK(vote.response.vote_granted);
    raft::VoteCountOutPut count;
    CHECK(leader.core.handle_vote_response(voter.id,vote.response,count,leader.error));
    CHECK(count.become_leader && leader.core.role() == Role::Leader);
}

AppendEntriesResponse deliver(Node& follower,const AppendEntriesRequest& sent) {
    raft::AppendEntriesOut out;
    CHECK(follower.core.handle_append_entries(sent.leader_id,sent,out,follower.error));
    if(out.hard_state) follower.saved = *out.hard_state;
    if(out.accept) {
        // 这里只执行内存日志动作，供 Core 协议测试；真实落盘由 persist 测试覆盖。
        if(out.truncate_from != 0) {
            CHECK(out.truncate_from <= follower.log.last_index()+1);
            follower.log.entries.resize(static_cast<std::size_t>(out.truncate_from-1));
        }
        for(const auto& entry : out.entries) {
            CHECK(entry.index == follower.log.last_index()+1);
            CHECK(entry.term != 0);
            CHECK(follower.log.entries.empty() || entry.term >= follower.log.entries.back().term);
            follower.log.entries.push_back(entry);
        }
        CHECK(follower.core.on_log_persisted(follower.error));
    }
    CHECK(follower.saved.current_term == follower.core.hard_state().current_term);
    return {out.term,out.accept};
}

AppendResponseOut sentinel() {
    AppendResponseOut out;
    out.hard_state = HardState{90,91};
    out.retry = true;
    return out;
}
void check_empty(const AppendResponseOut& out) {
    CHECK(!out.hard_state.has_value() && !out.retry);
}
void check_sentinel(const AppendResponseOut& out) {
    CHECK(out.hard_state.has_value());
    CHECK(out.hard_state->current_term == 90 && out.hard_state->voted_for == 91);
    CHECK(out.retry);
}

void test_higher_term_output() {
    // 保留最初 out 按值传递的红→绿围栏：内部任期变化不等于输出动作传出了。
    RaftCore core;
    std::string error;
    CHECK(core.init(1,{1,2,3},{3,1},0,0,error));
    AppendEntriesRequest sent;
    sent.term = 3;
    sent.leader_id = 1;
    AppendResponseOut out;
    CHECK(core.handle_append_entries_response(2,sent,{4,false},out,error));
    CHECK(error.empty() && core.hard_state().current_term == 4);
    CHECK(core.role() == Role::Follower);
    CHECK(out.hard_state.has_value());
    CHECK(out.hard_state->current_term == 4 && out.hard_state->voted_for == 0);
    CHECK(!out.retry);

    Node leader(1,5), voter(3,0);
    campaign(leader,voter);
    auto old = leader.build(2);
    old.term = 4; // 代表之前任期的原请求；不能因此忽略回复中的更高任期6。
    out = leader.reply(2,old,6,false);
    CHECK(leader.core.role() == Role::Follower);
    CHECK(leader.core.hard_state().current_term == 6 && leader.core.hard_state().voted_for == 0);
    CHECK(out.hard_state.has_value() && out.hard_state->current_term == 6 && !out.retry);
    CHECK(leader.saved.current_term == 5); // 输出不等于已经保存；外层必须消费这个动作。
    leader.saved = *out.hard_state;
    CHECK(leader.saved.current_term == 6);
    std::cout << "PASS higher-term reply returns save action before old-request/role filtering\n";
}

void test_sent_range_not_current_tail() {
    Node leader(1,0), voter(3,0);
    campaign(leader,voter);
    leader.grow(5);
    const auto sent = leader.build(2); // 本次只发送1..5。
    leader.grow(4);                  // 等回复时，本机又保存了6..9。
    auto out = leader.reply(2,sent,5,true);
    check_empty(out);
    CHECK(leader.core.match_index(2) == 5 && leader.core.next_index(2) == 6);
    CHECK(leader.core.match_index(1) == 9); // 本机进度与节点2进度分开。
    CHECK(leader.core.match_index(3) == 0 && leader.core.next_index(3) == 1);
    std::cout << "PASS success confirms sent 1..5, not later local tail 9; other peer unchanged\n";
}

void test_backoff_and_duplicate_failures() {
    Node leader(1,10), voter(3,0);
    campaign(leader,voter);
    const auto probe = leader.build(2); // prev=10，next=11。
    auto out = leader.reply(2,probe,5,false);
    CHECK(out.retry && !out.hard_state.has_value());
    CHECK(leader.core.next_index(2) == 10 && leader.core.match_index(2) == 0);
    out = leader.reply(2,probe,5,false); // 同一失败不能让next再减一次。
    check_empty(out);
    CHECK(leader.core.next_index(2) == 10);
    const auto current = leader.build(2); // prev=9，发送第10条。
    CHECK(current.prev_log_index == 9 && current.entries.size() == 1);
    out = leader.reply(2,probe,5,false); // 新请求已发，旧失败才到。
    check_empty(out);
    CHECK(leader.core.next_index(2) == 10);
    out = leader.reply(2,current,5,true);
    check_empty(out);
    CHECK(leader.core.match_index(2) == 10 && leader.core.next_index(2) == 11);
    out = leader.reply(2,probe,5,false); // 起点又是11，但不能退过已确认的10。
    check_empty(out);
    CHECK(leader.core.match_index(2) == 10 && leader.core.next_index(2) == 11);
    std::cout << "PASS one-step backoff / duplicate and late failures / confirmed floor\n";
}

void test_old_success_and_heartbeat_floor() {
    Node leader(1,5), voter(3,0);
    campaign(leader,voter);
    const auto old = leader.build(2); // 空心跳，只能确认到prev=5。
    leader.grow(5);
    const auto newer = leader.build(2); // 原请求超时后，新请求覆盖6..10。
    CHECK(newer.prev_log_index == 5 && newer.entries.size() == 5);
    check_empty(leader.reply(2,newer,5,true));
    check_empty(leader.reply(2,old,5,true));
    check_empty(leader.reply(2,newer,5,true));
    CHECK(leader.core.match_index(2) == 10 && leader.core.next_index(2) == 11);

    Node empty(1,0), empty_voter(3,0);
    campaign(empty,empty_voter);
    const auto beat = empty.build(2);
    CHECK(beat.prev_log_index == 0 && beat.entries.empty());
    check_empty(empty.reply(2,beat,5,true));
    for(int i = 0; i < 3; ++i) check_empty(empty.reply(2,beat,5,false));
    CHECK(empty.core.next_index(2) == 1 && empty.core.match_index(2) == 0);
    std::cout << "PASS late/duplicate success cannot regress; empty heartbeat keeps next >= 1\n";
}

void test_old_terms_and_roles() {
    Node leader(1,10), voter(3,0);
    campaign(leader,voter);
    const auto sent = leader.build(2);
    auto old = sent;
    old.term = 4; // 构造先前任期的请求快照，与迟到的任期4回复配对。
    auto out = sentinel();
    leader.error = "old error";
    CHECK(leader.core.handle_append_entries_response(2,old,{4,false},out,leader.error));
    CHECK(leader.error.empty()); check_empty(out);
    // 对旧请求的拒绝可以带接收者当前任期5，不能拿它回退新任期的进度。
    CHECK(leader.core.handle_append_entries_response(2,old,{5,false},out,leader.error));
    check_empty(out);
    CHECK(leader.core.next_index(2) == 11 && leader.core.match_index(2) == 0);

    Node follower(1,10,{1,2,3},1); // 旧leader重启，恢复任期4及其已保存的自投票。
    CHECK(follower.core.hard_state().voted_for == 1);
    auto before_restart = sent;
    before_restart.term = 4; // 模拟同任期重启后，旧leader请求的回复才到。
    out = sentinel();
    CHECK(follower.core.handle_append_entries_response(2,before_restart,{4,true},out,follower.error));
    check_empty(out);
    CHECK(follower.core.role() == Role::Follower && follower.core.next_index(2) == 0);
    raft::ElectionOutPut election;
    CHECK(follower.core.start_election(election,follower.error));
    follower.saved = election.hard_state;
    CHECK(follower.core.role() == Role::Candidate);
    CHECK(follower.core.handle_append_entries_response(2,before_restart,{5,false},out,follower.error));
    check_empty(out);
    CHECK(follower.core.role() == Role::Candidate && follower.core.next_index(2) == 0);
    std::cout << "PASS old reply/request terms / restarted Follower / Candidate ignore replication progress\n";
}

void test_invalid_calls_preserve_output() {
    Node leader(1,10), voter(3,0);
    campaign(leader,voter);
    const auto sent = leader.build(2);
    for(const auto peer : {std::uint64_t{0},std::uint64_t{1},std::uint64_t{99}}) {
        auto out = sentinel();
        CHECK(!leader.core.handle_append_entries_response(peer,sent,{99,false},out,leader.error));
        CHECK(!leader.error.empty()); check_sentinel(out);
        CHECK(leader.core.hard_state().current_term == 5 && leader.core.role() == Role::Leader);
    }
    for(int kind = 0; kind < 3; ++kind) {
        auto wrong = sent;
        if(kind == 0) wrong.leader_id = 2;
        if(kind == 1) wrong.prev_log_index = 11;
        if(kind == 2) { wrong.prev_log_index = 9; wrong.entries.resize(2); }
        auto out = sentinel();
        CHECK(!leader.core.handle_append_entries_response(2,wrong,{5,true},out,leader.error));
        CHECK(!leader.error.empty()); check_sentinel(out);
        CHECK(leader.core.match_index(2) == 0 && leader.core.next_index(2) == 11);
    }
    leader.core.stop();
    auto out = sentinel();
    CHECK(!leader.core.handle_append_entries_response(2,sent,{99,false},out,leader.error));
    check_sentinel(out);
    CHECK(leader.core.hard_state().current_term == 5);
    RaftCore fresh;
    CHECK(!fresh.handle_append_entries_response(2,sent,{5,true},out,leader.error));
    check_sentinel(out);
    std::cout << "PASS invalid sender/context/range/not-ready/stopped: error and original out preserved\n";
}

void test_sparse_member_slots() {
    const std::vector<std::uint64_t> members{300,100,200};
    Node leader(100,4,members), voter(300,0,members), follower(200,4,members);
    campaign(leader,voter);
    const auto sent = leader.build(200);
    const auto response = deliver(follower,sent);
    check_empty(leader.reply(200,sent,response.term,response.success));
    CHECK(leader.core.match_index(200) == 4 && leader.core.next_index(200) == 5);
    CHECK(leader.core.match_index(300) == 0 && leader.core.next_index(300) == 5);
    CHECK(leader.core.match_index(100) == 4);
    std::cout << "PASS sparse/unordered member IDs map to correct progress slot\n";
}

void test_response_index_limit() {
    // 元数据级边界，不制造巨大日志；只验证M3拒绝不能+1的确认范围。
    // init/become_leader 对MAX的完整契约是已记录的另一项，不在此声称已修复。
    const auto max = std::numeric_limits<std::uint64_t>::max();
    RaftCore core;
    std::string error;
    CHECK(core.init(1,{1,2,3},{4,0},max,4,error));
    raft::ElectionOutPut election;
    CHECK(core.start_election(election,error));
    raft::VoteCountOutPut vote;
    CHECK(core.handle_vote_response(2,{5,true},vote,error) && vote.become_leader);
    AppendEntriesRequest sent;
    sent.term = 5; sent.leader_id = 1; sent.prev_log_index = max;
    auto out = sentinel();
    CHECK(!core.handle_append_entries_response(2,sent,{5,true},out,error));
    CHECK(!error.empty()); check_sentinel(out);
    CHECK(core.match_index(2) == 0);
    std::cout << "PASS response range cannot confirm UINT64_MAX then wrap next index\n";
}

void test_catchup_roundtrip() {
    Node leader(1,130), voter(3,0), follower(2,7);
    // 节点2保留未提交的冲突后缀。由节点3投票，使节点1合法获得多数。
    for(std::size_t i = 3; i < follower.log.entries.size(); ++i) {
        follower.log.entries[i].term = 3;
        follower.log.entries[i].command = "conflict";
    }
    CHECK(follower.core.on_log_persisted(follower.error));
    campaign(leader,voter);
    std::size_t failures = 0, successes = 0;
    for(std::size_t round = 0; round < 512; ++round) {
        const auto before = leader.core.match_index(2);
        const auto sent = leader.build(2);
        const auto response = deliver(follower,sent);
        const auto out = leader.reply(2,sent,response.term,response.success);
        CHECK(!out.hard_state.has_value());
        CHECK(leader.core.match_index(2) >= before);
        CHECK(leader.core.next_index(2) >= leader.core.match_index(2)+1);
        if(response.success) {
            CHECK(!out.retry);
            CHECK(leader.core.match_index(2) == sent.prev_log_index+sent.entries.size());
            ++successes;
        } else {
            CHECK(out.retry);
            ++failures;
        }
        if(leader.core.match_index(2) == leader.log.last_index()) break;
    }
    CHECK(failures > 0 && successes == 2); // 找到第3条后，两批补完4..130。
    CHECK(leader.core.match_index(2) == 130 && leader.core.next_index(2) == 131);
    CHECK(follower.log.last_index() == leader.log.last_index());
    for(std::size_t i = 0; i < leader.log.entries.size(); ++i) {
        const auto& expected = leader.log.entries[i];
        const auto& actual = follower.log.entries[i];
        CHECK(actual.index == expected.index && actual.term == expected.term);
        CHECK(actual.command == expected.command);
    }
    CHECK(leader.core.match_index(3) == 0 && leader.core.next_index(3) == 131);
    std::cout << "PASS actual Core build/receive/reply loop: backoff, conflicting suffix, two-batch catchup\n";
}
} // namespace

int main() {
    test_higher_term_output();
    test_sent_range_not_current_tail();
    test_backoff_and_duplicate_failures();
    test_old_success_and_heartbeat_floor();
    test_old_terms_and_roles();
    test_invalid_calls_preserve_output();
    test_sparse_member_slots();
    test_response_index_limit();
    test_catchup_roundtrip();
    std::cout << "raft append-response tests PASS (actual Core; memory log/save model; no sockets)\n";
}
