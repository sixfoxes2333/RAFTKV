#include "raft/core/apply.h"
#include "raft/core/logview.h"

#include <cstdlib>
#include <filesystem>
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
using raft::LogEntry;
using raft::RaftCore;
using raft::Role;

LogEntry sentinel() { return {90,91,"keep original output"}; }
void same(const LogEntry& a,const LogEntry& b) {
    CHECK(a.index == b.index && a.term == b.term && a.command == b.command);
}

class MemoryLog final : public raft::LogView {
public:
    std::vector<LogEntry> entries;
    std::uint64_t last_index() const override { return entries.size(); }
    bool term_at(std::uint64_t i,std::uint64_t& out) const override {
        if(i == 0 || i > entries.size()) return false;
        out = entries[static_cast<std::size_t>(i-1)].term;
        return true;
    }
    bool entry_at(std::uint64_t i,LogEntry& out) const override {
        if(i == 0 || i > entries.size()) return false;
        out = entries[static_cast<std::size_t>(i-1)];
        return true;
    }
};

struct MemoryNode {
    MemoryLog log;
    RaftCore core;
    raft::HardState saved{2,0}; // 保存成功模型，只有下面的RealNode使用实际StateStore。
    std::string error;
    explicit MemoryNode(std::size_t count = 0) {
        for(std::size_t i = 1; i <= count; ++i)
            log.entries.push_back({i,2,"old"+std::to_string(i)});
        CHECK(core.attach_log(&log,error));
        CHECK(core.init(1,{1,2,3},saved,count,count == 0 ? 0 : 2,error));
    }
    void elect() {
        raft::ElectionOutPut election;
        CHECK(core.start_election(election,error));
        saved = election.hard_state;
        raft::VoteCountOutPut count;
        // 此处聚焦提案入口，构造一张合法成员的同任期赞成票。
        CHECK(core.handle_vote_response(2,{saved.current_term,true},count,error));
        CHECK(count.become_leader && core.role() == Role::Leader);
    }
};

void reject(RaftCore& core,const std::string& command,std::string& error) {
    auto out = sentinel();
    const auto before = out;
    CHECK(!core.build_proposal(command,out,error));
    CHECK(!error.empty());
    same(out,before);
}

void test_build_is_not_persistence() {
    MemoryNode node(10);
    node.elect();
    const RaftCore& read_only = node.core;
    auto out = sentinel();
    const std::string command("SET\0x",5);
    node.error = "old error";
    CHECK(read_only.build_proposal(command,out,node.error));
    CHECK(node.error.empty());
    CHECK(out.index == 11 && out.term == 3 && out.command == command);
    CHECK(node.log.last_index() == 10 && node.core.match_index(1) == 10);
    CHECK(node.core.next_index(2) == 11 && node.core.match_index(2) == 0);
    CHECK(node.core.role() == Role::Leader && node.core.hard_state().current_term == 3);
    CHECK(node.core.hard_state().voted_for == 1);
    LogEntry again;
    // 只是重复观察同一未保存状态，不将两个输出都当成已接收提案去保存。
    CHECK(read_only.build_proposal("another",again,node.error));
    CHECK(again.index == 11 && node.core.match_index(1) == 10);
    raft::AppendEntriesRequest request;
    CHECK(node.core.build_append_entries(2,request,node.error));
    CHECK(request.prev_log_index == 10 && request.entries.empty());
    CHECK(request.leader_commit == 0);
    std::cout << "PASS const builder: next index/current term/binary bytes; no log/progress/commit advance\n";
}

void test_command_bounds() {
    MemoryNode node;
    node.elect();
    const std::vector<std::size_t> accepted{0,1024*1024+1,raft::LogStore::MAX_COMMAND_SIZE};
    for(const auto size : accepted) {
        const std::string command(size,'v');
        LogEntry out;
        CHECK(node.core.build_proposal(command,out,node.error));
        CHECK(out.index == 1 && out.term == 3 && out.command == command);
        CHECK(node.core.match_index(1) == 0 && node.log.last_index() == 0);
    }
    const std::string too_large(raft::LogStore::MAX_COMMAND_SIZE+1,'v');
    reject(node.core,too_large,node.error);
    CHECK(node.core.good() && node.core.match_index(1) == 0);
    std::cout << "PASS empty / above 1MiB soft batch budget / exact 16MiB; oversize rejected without output change\n";
}

void test_roles_and_no_view() {
    std::string error;
    RaftCore fresh;
    reject(fresh,"cmd",error);
    MemoryNode node;
    reject(node.core,"cmd",node.error);
    raft::ElectionOutPut election;
    CHECK(node.core.start_election(election,node.error));
    node.saved = election.hard_state;
    CHECK(node.core.role() == Role::Candidate);
    reject(node.core,"cmd",node.error);
    CHECK(node.core.role() == Role::Candidate && node.core.hard_state().current_term == 3);
    raft::VoteCountOutPut count;
    CHECK(node.core.handle_vote_response(2,{3,true},count,node.error) && count.become_leader);
    node.core.stop();
    reject(node.core,"cmd",node.error);

    RaftCore no_view;
    CHECK(no_view.init(1,{1,2,3},{2,0},0,0,error));
    CHECK(no_view.start_election(election,error)); // 本单测视作保存了选举HardState。
    CHECK(no_view.handle_vote_response(2,{3,true},count,error) && count.become_leader);
    reject(no_view,"cmd",error);
    CHECK(no_view.good() && no_view.role() == Role::Leader);
    std::cout << "PASS uninitialized/Follower/Candidate/stopped/no-view: reject and preserve out\n";
}

class MetadataLog final : public raft::LogView {
public:
    explicit MetadataLog(std::uint64_t tail) : tail_(tail) {}
    std::uint64_t last_index() const override { return tail_; }
    bool term_at(std::uint64_t i,std::uint64_t& out) const override {
        if(i == 0 || i > tail_) return false;
        out = 2;
        return true;
    }
    bool entry_at(std::uint64_t,LogEntry&) const override { return false; }
private:
    std::uint64_t tail_;
};

void test_index_limit() {
    const auto max = std::numeric_limits<std::uint64_t>::max();
    for(const auto tail : {max-2,max-1,max}) {
        // 仅用元数据验证提案编号，不制造巨型日志，也不声称修复了init的MAX边界。
        MetadataLog view(tail);
        RaftCore core;
        std::string error;
        CHECK(core.attach_log(&view,error));
        CHECK(core.init(1,{1,2,3},{2,0},tail,2,error));
        raft::ElectionOutPut election;
        CHECK(core.start_election(election,error));
        raft::VoteCountOutPut count;
        CHECK(core.handle_vote_response(2,{3,true},count,error) && count.become_leader);
        if(tail == max-2) {
            LogEntry out;
            CHECK(core.build_proposal("cmd",out,error));
            CHECK(out.index == max-1 && out.term == 3);
        } else {
            reject(core,"cmd",error);
        }
        CHECK(core.match_index(1) == tail && core.match_index(2) == 0);
    }
    std::cout << "PASS proposal reserves representable next index; MAX-1/MAX tails rejected\n";
}

void test_stepdown_and_new_term() {
    MemoryNode node(10);
    node.elect();
    raft::AppendEntriesRequest sent;
    CHECK(node.core.build_append_entries(2,sent,node.error));
    raft::AppendResponseOut action;
    CHECK(node.core.handle_append_entries_response(2,sent,{4,false},action,node.error));
    CHECK(action.hard_state.has_value());
    node.saved = *action.hard_state;
    CHECK(node.core.role() == Role::Follower);
    reject(node.core,"cmd",node.error);
    node.elect();
    LogEntry out;
    CHECK(node.core.build_proposal("cmd",out,node.error));
    CHECK(out.index == 11 && out.term == 5 && node.log.last_index() == 10);
    std::cout << "PASS stepped-down node rejects; re-elected leader uses new current term, not log tail term\n";
}

struct TempDir {
    std::string path;
    TempDir() {
        char pattern[] = "/tmp/raft-proposal-XXXXXX";
        const char* made = ::mkdtemp(pattern);
        CHECK(made != nullptr);
        path = made;
    }
    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path,ignored);
    }
};

struct RealNode {
    // Core先销毁；日志视图/存储随后销毁；最后才清理目录。
    TempDir dir;
    raft::LogStore log;
    raft::StateStore state;
    raft::LogStoreView view;
    RaftCore core;
    std::uint64_t id;
    std::string error;
    RealNode(std::uint64_t node_id,const std::vector<std::uint64_t>& members,std::size_t existing)
        : log(dir.path),state(dir.path),view(log),id(node_id) {
        CHECK(log.create(error));
        CHECK(state.save({2,0},error));
        for(std::size_t i = 1; i <= existing; ++i)
            CHECK(log.append({i,2,"old"+std::to_string(i)},error));
        raft::HardState recovered;
        CHECK(state.load(recovered,error));
        CHECK(core.attach_log(&view,error));
        CHECK(core.init(id,members,recovered,existing,existing == 0 ? 0 : 2,error));
    }
};

void elect(RealNode& leader,RealNode* voter) {
    raft::ElectionOutPut election;
    CHECK(leader.core.start_election(election,leader.error));
    CHECK(leader.state.save(election.hard_state,leader.error));
    if(voter != nullptr) {
        raft::VoteOutPut vote;
        CHECK(voter->core.handle_request_vote(leader.id,election.request,vote,voter->error));
        if(vote.hard_state) CHECK(voter->state.save(*vote.hard_state,voter->error));
        CHECK(vote.response.vote_granted);
        raft::VoteCountOutPut count;
        CHECK(leader.core.handle_vote_response(voter->id,vote.response,count,leader.error));
        CHECK(count.become_leader);
    }
    CHECK(leader.core.role() == Role::Leader);
}

void test_real_single_node_save_sync_reopen() {
    RealNode node(1,{1},0);
    elect(node,nullptr);
    const auto before = std::filesystem::file_size(node.dir.path+"/raft.log");
    LogEntry first;
    CHECK(node.core.build_proposal("first",first,node.error));
    CHECK(first.index == 1 && first.term == 3);
    CHECK(node.log.last_index() == 0 && node.core.match_index(1) == 0);
    CHECK(std::filesystem::file_size(node.dir.path+"/raft.log") == before);
    CHECK(node.log.append(first,node.error));
    CHECK(node.log.last_index() == 1 && node.core.match_index(1) == 0);
    CHECK(node.core.on_log_persisted(node.error));
    CHECK(node.core.match_index(1) == 1);
    LogEntry next;
    CHECK(node.core.build_proposal("second",next,node.error));
    CHECK(next.index == 2 && next.term == 3 && node.log.last_index() == 1);
    raft::LogStore reopened(node.dir.path);
    CHECK(reopened.open(node.error));
    LogEntry actual;
    CHECK(reopened.get(1,actual,node.error));
    same(actual,first);
    raft::HardState saved;
    CHECK(node.state.load(saved,node.error));
    CHECK(saved.current_term == 3 && saved.voted_for == 1);
    std::cout << "PASS real single-node log: build -> append -> sync -> next index; reopened LogStore keeps entry\n";
}

void test_real_proposal_replication_roundtrip() {
    RealNode leader(1,{1,2,3},2), follower(2,{1,2,3},2);
    elect(leader,&follower);
    const std::string command("SET\0k\0v",7);
    LogEntry entry;
    CHECK(leader.core.build_proposal(command,entry,leader.error));
    CHECK(entry.index == 3 && entry.term == 3 && entry.command == command);
    CHECK(leader.log.append(entry,leader.error));
    CHECK(leader.core.match_index(1) == 2);
    CHECK(leader.core.on_log_persisted(leader.error));
    CHECK(leader.core.match_index(1) == 3 && leader.core.match_index(2) == 0);
    raft::AppendEntriesRequest sent;
    CHECK(leader.core.build_append_entries(2,sent,leader.error));
    CHECK(sent.prev_log_index == 2 && sent.prev_log_term == 2 && sent.entries.size() == 1);
    same(sent.entries.front(),entry);
    raft::AppendEntriesOut action;
    CHECK(follower.core.handle_append_entries(1,sent,action,follower.error) && action.accept);
    raft::AppendEntriesResponse response;
    CHECK(raft::apply_append_entries(follower.core,follower.state,follower.log,action,response,follower.error));
    CHECK(response.success && response.term == 3);
    raft::AppendResponseOut counted;
    CHECK(leader.core.handle_append_entries_response(2,sent,response,counted,leader.error));
    CHECK(!counted.hard_state.has_value() && !counted.retry);
    CHECK(leader.core.match_index(2) == 3 && leader.core.next_index(2) == 4);
    CHECK(leader.core.match_index(3) == 0 && leader.core.next_index(3) == 3);
    LogEntry actual;
    CHECK(follower.log.get(3,actual,follower.error));
    same(actual,entry);
    raft::AppendEntriesRequest heartbeat;
    CHECK(leader.core.build_append_entries(2,heartbeat,leader.error));
    CHECK(heartbeat.prev_log_index == 3 && heartbeat.entries.empty());
    // M4当前只增加提案入口；后续接提交推进时，扩展这里的commit断言。
    CHECK(heartbeat.leader_commit == 0);
    std::cout << "PASS real stores/two Core nodes: proposal -> local save -> M1/M2/M3 roundtrip (no commit/KV apply)\n";
}
} // namespace

int main() {
    test_build_is_not_persistence();
    test_command_bounds();
    test_roles_and_no_view();
    test_index_limit();
    test_stepdown_and_new_term();
    test_real_single_node_save_sync_reopen();
    test_real_proposal_replication_roundtrip();
    std::cout << "raft proposal tests PASS (actual Core; memory cases + real stores; no sockets/KV apply)\n";
}
