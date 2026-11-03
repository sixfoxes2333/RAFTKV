#include "raft/core/core.h"

#include <cstdlib>
#include <iostream>
#include <string>
#include <utility>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {
using raft::AppendEntriesRequest;
using raft::CommitOut;
using raft::LogEntry;
using raft::RaftCore;
using raft::Role;

CommitOut sentinel() { return {90,91}; }
void empty(const CommitOut& out) {
    CHECK(out.committed_from == 0 && out.committed_to == 0);
}
void range(const CommitOut& out,std::uint64_t first,std::uint64_t last) {
    CHECK(out.committed_from == first && out.committed_to == last);
}

// 保存完成用内存日志表示；本文件不模拟真实磁盘故障或网络传输。
class MemoryLog final : public raft::LogView {
public:
    std::vector<LogEntry> entries;
    std::uint64_t fail_term_at = 0;
    mutable std::size_t term_reads = 0;
    mutable bool queried_zero = false;
    std::uint64_t last_index() const override { return entries.size(); }
    bool term_at(std::uint64_t index,std::uint64_t& out) const override {
        ++term_reads;
        if(index == 0) { queried_zero = true; return false; }
        if(index > entries.size() || index == fail_term_at) return false;
        out = entries[static_cast<std::size_t>(index-1)].term;
        return true;
    }
    bool entry_at(std::uint64_t index,LogEntry& out) const override {
        if(index == 0 || index > entries.size()) return false;
        out = entries[static_cast<std::size_t>(index-1)];
        return true;
    }
};

struct Node {
    MemoryLog log;
    RaftCore core;
    raft::HardState saved{2,0}; // 仅表示测试已消费term/vote保存动作。
    std::uint64_t id;
    std::vector<std::uint64_t> members;
    std::string error;
    Node(std::uint64_t self = 1,std::vector<std::uint64_t> ids = {1,2,3},std::size_t old = 0)
        : id(self),members(std::move(ids)) {
        for(std::size_t i = 1; i <= old; ++i)
            log.entries.push_back({i,2,"old"+std::to_string(i)});
        CHECK(core.attach_log(&log,error));
        CHECK(core.init(id,members,saved,old,old == 0 ? 0 : 2,error));
    }
    void elect() {
        raft::ElectionOutPut election;
        CHECK(core.start_election(election,error));
        saved = election.hard_state;
        // 构造本轮赞成票，只用于让本机进入Leader，不代表对方已存后续日志。
        for(const auto peer : members) {
            if(core.role() == Role::Leader) break;
            if(peer == id) continue;
            raft::VoteCountOutPut vote;
            CHECK(core.handle_vote_response(peer,{saved.current_term,true},vote,error));
        }
        CHECK(core.role() == Role::Leader);
    }
    void append(std::size_t count = 1) {
        for(std::size_t i = 0; i < count; ++i) {
            LogEntry entry;
            CHECK(core.build_proposal("cmd"+std::to_string(log.last_index()+1),entry,error));
            log.entries.push_back(std::move(entry)); // 测试模型：这一条已保存。
            CHECK(core.on_log_persisted(error));
        }
    }
    AppendEntriesRequest request(std::uint64_t peer) {
        AppendEntriesRequest sent;
        CHECK(core.build_append_entries(peer,sent,error));
        return sent;
    }
    void accept(std::uint64_t peer,const AppendEntriesRequest& sent,bool success = true) {
        raft::AppendResponseOut action;
        // 保留原请求来处理其回复；不拿新的日志尾部冒充原发送范围。
        CHECK(core.handle_append_entries_response(peer,sent,{sent.term,success},action,error));
        CHECK(!action.hard_state.has_value());
    }
    void ack(std::uint64_t peer) {
        const auto sent = request(peer);
        accept(peer,sent);
    }
    CommitOut advance() {
        auto out = sentinel();
        error = "old error";
        CHECK(core.advance_commit(out,error));
        CHECK(error.empty());
        return out;
    }
    std::uint64_t advertised(std::uint64_t peer) { return request(peer).leader_commit; }
};

void reject(RaftCore& core,std::string& error) {
    auto out = sentinel();
    CHECK(!core.advance_commit(out,error));
    CHECK(!error.empty());
    range(out,90,91);
}

void test_commit_retained_and_advertised() {
    // 原缺行红→绿围栏：返回区间还不够，Core必须记住新commit。
    Node node;
    node.elect();
    node.append();
    node.ack(2);
    range(node.advance(),1,1);
    CHECK(node.advertised(2) == 1);
    empty(node.advance());
    CHECK(node.advertised(2) == 1);
    std::cout << "PASS committed index is retained/advertised; no duplicate output\n";
}

void test_empty_and_majorities() {
    for(std::uint64_t size = 1; size <= 6; ++size) {
        std::vector<std::uint64_t> ids;
        for(std::uint64_t id = 1; id <= size; ++id) ids.push_back(id);
        Node node(1,ids);
        node.elect();
        const auto reads = node.log.term_reads;
        empty(node.advance());
        CHECK(node.log.term_reads == reads && !node.log.queried_zero);
        node.append();
        std::size_t copies = 1;
        const auto majority = node.core.majority();
        auto out = node.advance();
        if(majority == 1) range(out,1,1); else empty(out);
        for(const auto peer : ids) {
            if(peer == 1) continue;
            node.ack(peer);
            ++copies;
            out = node.advance();
            if(copies == majority) range(out,1,1); else empty(out);
            CHECK(node.advertised(peer) == (copies >= majority ? 1U : 0U));
        }
        CHECK(!node.log.queried_zero);
    }
    std::cout << "PASS N=1..6: empty/no term_at(0), own durable copy, strict majority incl even sizes\n";
}

void test_unsaved_proposal_not_counted() {
    Node node(1,{1});
    node.elect();
    LogEntry pending;
    CHECK(node.core.build_proposal("pending",pending,node.error));
    // 只观察保存前的判定，不把未保存提案发给其他节点。
    empty(node.advance());
    CHECK(node.core.match_index(1) == 0);
    node.log.entries.push_back(pending);
    // 日志模型已改变，但Core尚未收到保存完成通知，自己仍不能算一份。
    empty(node.advance());
    CHECK(node.core.on_log_persisted(node.error));
    range(node.advance(),1,1);
    std::cout << "PASS single member also waits for saved-log synchronization before committing\n";
}

void test_highest_majority_position() {
    Node node(1,{1,2,3},10);
    node.elect();
    const auto to3 = node.request(3); // 当时只能确认到10。
    node.append();
    const auto to2 = node.request(2); // 当时只能确认到11。
    node.append();                  // 自己已经保存到12，等待之前的回话。
    node.accept(3,to3);
    node.accept(2,to2);
    CHECK(node.core.match_index(1) == 12 && node.core.match_index(2) == 11);
    CHECK(node.core.match_index(3) == 10);
    range(node.advance(),1,11); // 不是12：只有自己存到了12。
    CHECK(node.advertised(2) == 11);
    CHECK(node.core.match_index(1) == 12 && node.core.match_index(2) == 11);
    CHECK(node.core.match_index(3) == 10); // 提交判定不改复制进度。
    empty(node.advance());
    node.ack(2);
    range(node.advance(),12,12);
    std::cout << "PASS [12,11,10] chooses11, then emits only12 after its majority confirmation\n";
}

void test_old_term_majority_waits_for_current_term() {
    Node node(1,{1,2,3,4,5},2);
    node.elect(); // 任期3，但原来的两条日志属于任期2。
    node.ack(2);
    node.ack(3);
    CHECK(node.core.match_index(1) == 2 && node.core.match_index(2) == 2);
    empty(node.advance()); // 旧任期第2条虽有3/5份，不能直接作为提交点。
    CHECK(node.advertised(2) == 0);
    node.append();        // 新第3条属于当前任期3。
    node.ack(2);
    empty(node.advance()); // 新条目只有2/5份，旧条目仍不能借此提交。
    node.ack(3);
    range(node.advance(),1,3); // 当前任期条目达多数，前面的旧日志一起提交。
    CHECK(node.advertised(2) == 3);
    empty(node.advance());
    std::cout << "PASS 5 members: old-term majority alone cannot commit; current-term majority commits prefix\n";
}

void test_duplicate_ack_is_not_another_copy() {
    Node node(1,{1,2,3,4,5});
    node.elect();
    node.append();
    empty(node.advance()); // 赢过选举，不等于后来的日志也有多数副本。
    const auto sent = node.request(2);
    node.accept(2,sent);
    node.accept(2,sent);
    empty(node.advance()); // 自己+节点2只有两份，重复回话不能变三份。
    node.ack(3);
    range(node.advance(),1,1);
    node.accept(2,sent,false); // 旧失败也不能让已提交位置倒退。
    empty(node.advance());
    CHECK(node.advertised(2) == 1);
    std::cout << "PASS election votes are not log copies; duplicate replies do not make a majority\n";
}

void test_sparse_unordered_members() {
    Node node(90,{50,10,90,30,70});
    node.elect();
    node.append(4);
    const auto to50 = node.request(50);
    node.append(2);
    const auto to30 = node.request(30);
    node.append(2);
    const auto to10 = node.request(10);
    node.append(2);
    node.accept(50,to50);
    node.accept(30,to30);
    node.accept(10,to10);
    CHECK(node.core.match_index(50) == 4 && node.core.match_index(10) == 8);
    CHECK(node.core.match_index(90) == 10 && node.core.match_index(30) == 6);
    CHECK(node.core.match_index(70) == 0);
    range(node.advance(),1,6); // [4,8,10,6,0]的第三高是6，与节点ID大小无关。
    CHECK(node.advertised(50) == 6);
    std::cout << "PASS unordered sparse IDs and unequal progress choose highest majority position6\n";
}

void test_ranges_are_contiguous_and_monotonic() {
    Node node;
    node.elect();
    std::uint64_t previous = 0;
    std::vector<AppendEntriesRequest> history;
    for(const auto count : {3U,1U,4U,2U}) {
        node.append(count);
        empty(node.advance());
        history.push_back(node.request(2));
        node.accept(2,history.back());
        const auto out = node.advance();
        range(out,previous+1,node.log.last_index());
        previous = out.committed_to;
        empty(node.advance());
        CHECK(node.advertised(2) == previous);
    }
    CHECK(previous == 10);
    node.accept(2,history.front());
    node.accept(2,history.front(),false);
    empty(node.advance());
    CHECK(node.advertised(2) == 10);
    std::cout << "PASS multiple batches produce contiguous non-overlapping ranges; late replies cannot regress\n";
}

void test_term_read_failure_preserves_commit_and_output() {
    Node node;
    node.elect();
    node.append(2);
    const auto sent = node.request(2);
    node.append();
    node.accept(2,sent); // [3,2,0]的多数位置是2。
    node.log.fail_term_at = 2;
    auto out = sentinel();
    CHECK(!node.core.advance_commit(out,node.error));
    CHECK(!node.error.empty());
    range(out,90,91);
    // 这里只读打包结果观察旧commit，没有发送报文；真实外层遇到读失败应停止协议。
    CHECK(node.advertised(3) == 0);
    node.core.stop();
    std::cout << "PASS known-index term read failure is an error, not an old-term no-op; commit/out unchanged\n";
}

void test_roles_and_no_view() {
    RaftCore fresh;
    std::string error;
    reject(fresh,error);
    Node node;
    reject(node.core,node.error);
    raft::ElectionOutPut election;
    CHECK(node.core.start_election(election,node.error));
    node.saved = election.hard_state;
    CHECK(node.core.role() == Role::Candidate);
    reject(node.core,node.error);
    raft::VoteCountOutPut vote;
    CHECK(node.core.handle_vote_response(2,{3,true},vote,node.error) && vote.become_leader);
    node.core.stop();
    reject(node.core,node.error);
    RaftCore no_view;
    CHECK(no_view.init(1,{1},{2,0},0,0,error));
    CHECK(no_view.start_election(election,error)); // 模型视作消费了本轮保存动作。
    CHECK(no_view.role() == Role::Leader);
    reject(no_view,error);
    std::cout << "PASS uninitialized/Follower/Candidate/stopped/no-view reject without replacing out\n";
}

void test_commit_survives_stepdown_and_new_election() {
    Node node;
    node.elect();
    node.append();
    node.ack(2);
    range(node.advance(),1,1);
    const auto old = node.request(2);
    raft::AppendResponseOut higher;
    CHECK(node.core.handle_append_entries_response(2,old,{4,false},higher,node.error));
    CHECK(higher.hard_state.has_value());
    node.saved = *higher.hard_state;
    CHECK(node.core.role() == Role::Follower);
    reject(node.core,node.error);
    node.elect(); // 新任期5，peer的复制进度重新从未知开始，但已提交的1不能丢。
    CHECK(node.core.hard_state().current_term == 5);
    empty(node.advance());
    node.append();
    node.accept(2,old); // 任期3的旧成功，不能算作本任期第2条的确认。
    empty(node.advance());
    CHECK(node.advertised(2) == 1);
    node.ack(2);
    range(node.advance(),2,2);
    CHECK(node.advertised(2) == 2);
    std::cout << "PASS stepdown/new election keep committed prefix; new term needs fresh confirmations\n";
}

// 以下用真实Core处理五个节点的投票和复制；保存完成仍由内存模型表示。
raft::RequestVoteRequest campaign_actual(Node& candidate,const std::vector<Node*>& voters) {
    raft::ElectionOutPut election;
    CHECK(candidate.core.start_election(election,candidate.error));
    candidate.saved = election.hard_state;
    for(Node* voter : voters) {
        raft::VoteOutPut vote;
        CHECK(voter->core.handle_request_vote(candidate.id,election.request,vote,voter->error));
        if(vote.hard_state) voter->saved = *vote.hard_state;
        raft::VoteCountOutPut counted;
        CHECK(candidate.core.handle_vote_response(voter->id,vote.response,counted,candidate.error));
        if(counted.hard_state) candidate.saved = *counted.hard_state;
    }
    CHECK(candidate.core.role() == Role::Leader);
    return election.request;
}

bool send_actual(Node& leader,Node& follower) {
    const auto sent = leader.request(follower.id);
    raft::AppendEntriesOut action;
    CHECK(follower.core.handle_append_entries(leader.id,sent,action,follower.error));
    if(action.hard_state) follower.saved = *action.hard_state;
    if(action.accept) {
        if(action.truncate_from != 0) {
            CHECK(action.truncate_from <= follower.log.last_index()+1);
            follower.log.entries.resize(static_cast<std::size_t>(action.truncate_from-1));
        }
        for(const auto& entry : action.entries) {
            CHECK(entry.index == follower.log.last_index()+1);
            follower.log.entries.push_back(entry);
        }
        CHECK(follower.core.on_log_persisted(follower.error));
        CommitOut committed;
        CHECK(follower.core.advance_follower_commit(sent,committed,follower.error));
    }
    raft::AppendResponseOut counted;
    CHECK(leader.core.handle_append_entries_response(follower.id,sent,{action.term,action.accept},counted,leader.error));
    if(counted.hard_state) leader.saved = *counted.hard_state;
    return action.accept;
}

void test_figure8_old_majority_can_be_replaced() {
    const std::vector<std::uint64_t> ids{1,2,3,4,5};
    Node s1(1,ids), s2(2,ids), s3(3,ids), s4(4,ids), s5(5,ids);
    campaign_actual(s1,{&s2,&s3}); // 节点1在任期3先建立一个共同的已提交前缀。
    s1.append();
    CHECK(send_actual(s1,s2) && send_actual(s1,s3));
    range(s1.advance(),1,1);
    CHECK(send_actual(s1,s2) && send_actual(s1,s3));
    CHECK(send_actual(s1,s4) && send_actual(s1,s5));

    s1.append(); // 第2条属于任期3，只复制到节点2，未达3/5。
    CHECK(send_actual(s1,s2));
    empty(s1.advance());
    // 不再给节点1送消息，表示它暂时隔离；节点5借节点3/4的票当选任期4。
    const auto vote4 = campaign_actual(s5,{&s3,&s4});
    LogEntry other;
    CHECK(s5.core.build_proposal("different command at index2",other,s5.error));
    s5.log.entries.push_back(other);
    CHECK(s5.core.on_log_persisted(s5.error));
    empty(s5.advance()); // 不发给别人，任期4的这条也只有一份。

    // 当初发给节点1的任期4拉票请求现在才到；节点1见更高任期退位，但拒绝较短日志。
    raft::VoteOutPut delayed;
    CHECK(s1.core.handle_request_vote(5,vote4,delayed,s1.error));
    CHECK(!delayed.response.vote_granted && delayed.hard_state.has_value());
    s1.saved = *delayed.hard_state;
    CHECK(s1.core.role() == Role::Follower && s1.core.hard_state().current_term == 4);
    // 节点1重选任期5，节点5因自己的日志term4更新而拒绝，但节点2/3足够成多数。
    campaign_actual(s1,{&s2,&s3,&s5});
    CHECK(s1.core.hard_state().current_term == 5 && s5.core.role() == Role::Follower);
    CHECK(send_actual(s1,s2));
    CHECK(!send_actual(s1,s3)); // 节点3还没有第2条，先退一次发送位置。
    CHECK(send_actual(s1,s3));
    CHECK(s1.log.entries[1].term == 3 && s2.log.entries[1].term == 3 && s3.log.entries[1].term == 3);
    // 此刻旧任期第2条真的有3/5份，但不能因此直接提交。
    empty(s1.advance());
    CHECK(s1.advertised(2) == 1);

    // 节点1再次隔离；节点5在任期6凭更大的lastLogTerm拿到节点2/3的票。
    campaign_actual(s5,{&s2,&s3});
    CHECK(s5.core.hard_state().current_term == 6);
    CHECK(!send_actual(s5,s2));
    CHECK(send_actual(s5,s2)); // 旧第2条未提交，允许被term4的另一条覆盖。
    CHECK(!send_actual(s5,s3));
    CHECK(send_actual(s5,s3));
    CHECK(s2.log.entries[1].term == 4 && s3.log.entries[1].term == 4);
    CHECK(s2.log.entries[1].command == other.command && s3.log.entries[1].command == other.command);
    empty(s5.advance()); // term4仍不是当前任期6，仍不能直接提交。
    s5.append(); // 第3条属于当前任期6，复制过半后才能带上前面的第2条。
    CHECK(send_actual(s5,s2) && send_actual(s5,s3));
    range(s5.advance(),2,3);
    CHECK(send_actual(s5,s2) && send_actual(s5,s3));
    // 隔离解除，节点1的旧条目也能被替换，说明它没被错误标成已提交。
    CHECK(!send_actual(s5,s1));
    CHECK(send_actual(s5,s1));
    CHECK(s1.log.entries[1].term == 4 && s1.log.entries[1].command == other.command);
    CHECK(s1.log.entries[2].term == 6 && s5.advertised(1) == 3);
    std::cout << "PASS Figure8-style five-Core trace: old-term majority remains replaceable; current-term anchor commits prefix\n";
}

} // namespace

int main() {
    test_commit_retained_and_advertised();
    test_empty_and_majorities();
    test_unsaved_proposal_not_counted();
    test_highest_majority_position();
    test_old_term_majority_waits_for_current_term();
    test_duplicate_ack_is_not_another_copy();
    test_sparse_unordered_members();
    test_ranges_are_contiguous_and_monotonic();
    test_term_read_failure_preserves_commit_and_output();
    test_roles_and_no_view();
    test_commit_survives_stepdown_and_new_election();
    test_figure8_old_majority_can_be_replaced();
    std::cout << "raft leader-commit tests PASS (actual Core; memory save model; no sockets/KV apply)\n";
}
