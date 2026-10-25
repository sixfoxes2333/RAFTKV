#include "raft/apply.h"
#include "raft/logview.h"

#include <cstddef>
#include <cstdlib>
#include <deque>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

namespace {
const char* scenario = "setup";
std::string event;
#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr \
              << " [" << scenario << "] " << event << '\n'; \
    std::exit(1); \
} } while (false)

using Id = std::uint64_t;
using raft::Role;
using raft::LogEntry;
using raft::CommitOut;
using raft::AppendEntriesRequest;

enum class Kind { VoteRequest, VoteResponse, AppendRequest, AppendResponse };
struct Message {
    Kind kind = Kind::VoteRequest;
    Id from = 0, to = 0, rpc = 0;
    raft::RequestVoteRequest vote_request;
    raft::RequestVoteResponse vote_response;
    // 请求和回复共同保留不可变的原请求；不会拿后来新建的请求冒充它。
    std::shared_ptr<const AppendEntriesRequest> sent;
    raft::AppendEntriesResponse append_response;
};
struct TempDir {
    std::string path;
    TempDir() {
        char pattern[] = "/tmp/raft-core-scenarios-XXXXXX";
        const char* made = ::mkdtemp(pattern);
        CHECK(made != nullptr);
        path = made;
    }
    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path,ignored);
    }
};
struct Node {
    Id id;
    std::string path, error;
    raft::LogStore log;
    raft::StateStore state;
    raft::LogStoreView view;
    raft::RaftCore core;
    bool online = false, active_leader = false;
    Id boot_commit = 0;
    std::size_t timer_resets = 0;
    std::vector<std::size_t> accepted_batch_sizes; // 成功请求的条数，不是实际磁盘写入次数。
    Id progress_term = 0;
    std::map<Id,Id> confirmed_before;
    std::map<Id,Id> pending; // 每个peer只允许一条尚未完成的复制请求。

    Node(Id self,std::string directory)
        : id(self),path(std::move(directory)),log(path+"/log"),state(path+"/state"),view(log) {}
    bool open(const std::vector<Id>& members,bool fresh) {
        if(fresh) {
            std::filesystem::create_directories(path+"/log");
            std::filesystem::create_directories(path+"/state");
            if(!log.create(error) || !state.save({0,0},error)) return false;
        } else if(!log.open(error)) return false;
        raft::HardState saved;
        if(!state.load(saved,error)) return false;
        Id term = 0;
        if(log.last_index() != 0 && !log.term_at(log.last_index(),term,error)) return false;
        if(!core.attach_log(&view,error) || !core.init(id,members,saved,log.last_index(),term,error)) return false;
        online = true;
        return true;
    }
};

// 测试调度器：同步完成实际文件保存后，才把消息加入队列。
// 它不是生产RaftWorker，也没有socket或真实计时线程。
class Cluster {
public:
    TempDir dir;
    std::vector<Id> members;
    std::vector<std::unique_ptr<Node>> nodes;
    std::deque<Message> queue;
    std::set<std::pair<Id,Id>> blocked;
    std::map<Id,LogEntry> committed; // 所有节点曾发布的已提交内容，逐条检查不能矛盾。
    std::map<Id,Id> node_floor;     // 该存储在之前启动中已经确认过的提交前缀。
    std::map<Id,Id> leader_by_term;
    std::map<Id,Id> max_term;
    std::map<std::pair<Id,Id>,Id> durable_vote;
    Id next_rpc = 1;

    explicit Cluster(Id size) {
        for(Id id = 1; id <= size; ++id) members.push_back(id);
        nodes.resize(static_cast<std::size_t>(size+1));
        for(const auto id : members) CHECK(boot(id,true));
    }
    Node& node(Id id) { CHECK(id < nodes.size() && nodes[id]); return *nodes[id]; }
    bool boot(Id id,bool fresh = false) {
        auto loaded = std::make_unique<Node>(id,dir.path+"/n"+std::to_string(id));
        const bool ok = loaded->open(members,fresh);
        if(!ok) loaded->core.stop();
        nodes[id] = std::move(loaded);
        if(ok) check();
        return ok;
    }
    void crash(Id id) { nodes[id].reset(); }
    void halt(Node& n) {
        n.core.stop(); n.online = false; n.active_leader = false; n.pending.clear();
    }
    bool save(Node& n,const raft::HardState& state) {
        if(n.state.save(state,n.error)) return true;
        halt(n);
        return false;
    }
    void finish(Node& n) {
        if(!n.core.good()) { halt(n); return; }
        if(n.core.role() != Role::Leader) {
            n.active_leader = false;
            n.pending.clear();
        } else if(!n.active_leader) {
            // 此处只在必要保存成功之后执行；逻辑Leader尚未保存时不对外算Leader。
            n.active_leader = true;
            const auto term = n.core.hard_state().current_term;
            const auto found = leader_by_term.find(term);
            CHECK(found == leader_by_term.end() || found->second == n.id);
            leader_by_term[term] = n.id;
            for(const auto& known : committed) {
                LogEntry actual;
                CHECK(n.log.get(known.first,actual,n.error));
                CHECK(actual.term == known.second.term && actual.command == known.second.command);
            }
        }
        if(n.active_leader) {
            const auto term = n.core.hard_state().current_term;
            if(n.progress_term != term) { n.progress_term = term; n.confirmed_before.clear(); }
            CHECK(n.core.match_index(n.id) == n.log.last_index());
            for(const auto peer : members) {
                const auto matched = n.core.match_index(peer);
                CHECK(matched >= n.confirmed_before[peer] && matched <= n.log.last_index());
                n.confirmed_before[peer] = matched;
                // self的next不由on_log_persisted更新；这里只检查其他peer的发送位置。
                if(peer != n.id) CHECK(n.core.next_index(peer) >= matched+1 && n.core.next_index(peer) <= n.log.last_index()+1);
            }
        }
        check();
    }
    void consume(Node& n,const CommitOut& out) {
        if(out.committed_from == 0) { CHECK(out.committed_to == 0); return; }
        CHECK(out.committed_from == n.boot_commit+1 && out.committed_to >= out.committed_from);
        for(Id index = out.committed_from; index <= out.committed_to; ++index) {
            LogEntry entry;
            CHECK(n.log.get(index,entry,n.error));
            CHECK(entry.index == index);
            const auto found = committed.find(index);
            if(found == committed.end()) committed.emplace(index,entry);
            else CHECK(found->second.term == entry.term && found->second.command == entry.command);
        }
        n.boot_commit = out.committed_to;
        if(node_floor[n.id] < n.boot_commit) node_floor[n.id] = n.boot_commit;
    }
    bool advance(Node& n) {
        CommitOut out;
        if(!n.core.advance_commit(out,n.error)) { halt(n); return false; }
        consume(n,out);
        return true;
    }
    void check() {
        for(const auto id : members) {
            if(!nodes[id] || !nodes[id]->online) continue;
            auto& n = *nodes[id];
            CHECK(n.core.good());
            raft::HardState saved;
            CHECK(n.state.load(saved,n.error));
            CHECK(saved.current_term == n.core.hard_state().current_term);
            CHECK(saved.voted_for == n.core.hard_state().voted_for);
            CHECK(saved.current_term >= max_term[id]);
            max_term[id] = saved.current_term;
            const auto key = std::make_pair(id,saved.current_term);
            const auto voted = durable_vote.find(key);
            if(voted != durable_vote.end()) CHECK(saved.voted_for == voted->second);
            if(saved.voted_for != 0) durable_vote[key] = saved.voted_for;
            CHECK(n.log.last_index() >= node_floor[id]);
            for(Id index = 1; index <= node_floor[id]; ++index) {
                LogEntry actual;
                CHECK(n.log.get(index,actual,n.error));
                const auto expected = committed.find(index);
                CHECK(expected != committed.end());
                CHECK(actual.term == expected->second.term && actual.command == expected->second.command);
            }
        }
    }
    void timeout(Id id) {
        auto& n = node(id);
        CHECK(n.online);
        event = "election timeout at node "+std::to_string(id);
        raft::ElectionOutPut out;
        if(!n.core.start_election(out,n.error)) { halt(n); return; }
        n.active_leader = false;
        n.pending.clear();
        if(!save(n,out.hard_state)) return;
        finish(n);
        for(const auto peer : out.targets) {
            Message m; m.kind = Kind::VoteRequest; m.from = id; m.to = peer;
            m.vote_request = out.request;
            queue.push_back(std::move(m));
        }
    }
    void propose(Id id,const std::string& command) {
        auto& n = node(id);
        CHECK(n.online && n.active_leader);
        event = "proposal at node "+std::to_string(id);
        LogEntry entry;
        CHECK(n.core.build_proposal(command,entry,n.error));
        if(!n.log.append(entry,n.error) || !n.core.on_log_persisted(n.error)) { halt(n); return; }
        if(!advance(n)) return;
        finish(n);
    }
    void send_append(Id from,Id to) {
        auto& n = node(from);
        CHECK(n.online && n.active_leader && from != to);
        if(n.pending.count(to) != 0) return;
        AppendEntriesRequest sent;
        CHECK(n.core.build_append_entries(to,sent,n.error));
        CHECK(sent.leader_commit == n.boot_commit);
        Message m; m.kind = Kind::AppendRequest; m.from = from; m.to = to; m.rpc = next_rpc++;
        m.sent = std::make_shared<const AppendEntriesRequest>(std::move(sent));
        n.pending[to] = m.rpc;
        queue.push_back(std::move(m));
    }
    void heartbeat(Id id) {
        for(const auto peer : members) if(peer != id) send_append(id,peer);
    }
    void expire(Id from,Id to) { node(from).pending.erase(to); }
    void expire_all(Id id) { node(id).pending.clear(); }
    void isolate(Id id) {
        for(const auto peer : members) if(peer != id) {
            blocked.emplace(id,peer); blocked.emplace(peer,id);
        }
    }
    void partition(const std::vector<Id>& left,const std::vector<Id>& right) {
        for(const auto a : left) for(const auto b : right) {
            blocked.emplace(a,b); blocked.emplace(b,a);
        }
    }
    void heal() { blocked.clear(); }
    std::size_t find(Kind kind,Id from,Id to) const {
        for(std::size_t i = 0; i < queue.size(); ++i)
            if(queue[i].kind == kind && queue[i].from == from && queue[i].to == to) return i;
        CHECK(false);
        return 0;
    }
    Message take(std::size_t index) {
        CHECK(index < queue.size());
        auto message = std::move(queue[index]);
        queue.erase(queue.begin()+static_cast<std::ptrdiff_t>(index));
        return message;
    }
    void deliver(Kind kind,Id from,Id to) { step(find(kind,from,to)); }
    void step(std::size_t index = 0) {
        auto m = take(index);
        const char* names[] = {"vote request","vote reply","append request","append reply"};
        event = std::string(names[static_cast<int>(m.kind)])+" "+std::to_string(m.from)+"->"+std::to_string(m.to);
        if(blocked.count({m.from,m.to}) || !nodes[m.to] || !node(m.to).online) return;
        auto& n = node(m.to);
        if(m.kind == Kind::VoteRequest) {
            raft::VoteOutPut out;
            if(!n.core.handle_request_vote(m.from,m.vote_request,out,n.error)) { halt(n); return; }
            if(out.hard_state && !save(n,*out.hard_state)) return;
            if(out.reset_election_timer) ++n.timer_resets;
            finish(n);
            Message reply; reply.kind = Kind::VoteResponse; reply.from = m.to; reply.to = m.from;
            reply.vote_response = out.response;
            queue.push_back(std::move(reply));
        } else if(m.kind == Kind::VoteResponse) {
            raft::VoteCountOutPut out;
            if(!n.core.handle_vote_response(m.from,m.vote_response,out,n.error)) { halt(n); return; }
            if(out.hard_state && !save(n,*out.hard_state)) return;
            finish(n);
        } else if(m.kind == Kind::AppendRequest) {
            CHECK(m.sent);
            raft::AppendEntriesOut out;
            if(!n.core.handle_append_entries(m.from,*m.sent,out,n.error)) { halt(n); return; }
            raft::AppendEntriesResponse response;
            if(!raft::apply_append_entries(n.core,n.state,n.log,out,response,n.error)) { halt(n); return; }
            if(response.success) {
                CommitOut committed_out;
                if(!n.core.advance_follower_commit(*m.sent,committed_out,n.error)) { halt(n); return; }
                consume(n,committed_out);
                if(!m.sent->entries.empty()) n.accepted_batch_sizes.push_back(m.sent->entries.size());
            }
            if(out.reset_election_timer) ++n.timer_resets;
            finish(n);
            Message reply; reply.kind = Kind::AppendResponse; reply.from = m.to; reply.to = m.from;
            reply.rpc = m.rpc; reply.sent = std::move(m.sent); reply.append_response = response;
            queue.push_back(std::move(reply));
        } else {
            CHECK(m.sent);
            const auto active = n.pending.find(m.from);
            if(active != n.pending.end() && active->second == m.rpc) n.pending.erase(active);
            // 旧回复仍交给Core观察更高term；原sent属于这条回复，不会被新请求覆盖。
            raft::AppendResponseOut out;
            if(!n.core.handle_append_entries_response(m.from,*m.sent,m.append_response,out,n.error)) { halt(n); return; }
            if(out.hard_state && !save(n,*out.hard_state)) return;
            finish(n);
            if(n.active_leader) {
                if(!advance(n)) return;
                finish(n);
                if(out.retry || (m.append_response.success && n.core.next_index(m.from) <= n.log.last_index()))
                    send_append(n.id,m.from);
            }
        }
    }
    void drain() {
        std::size_t steps = 0;
        while(!queue.empty()) { CHECK(++steps <= 10000); step(); }
        check();
    }
    void settle(Id leader) {
        heartbeat(leader); drain();
        heartbeat(leader); drain(); // 第二轮把新commit通知给已存好数据的Follower。
    }
    void elect(Id id) { timeout(id); drain(); CHECK(node(id).active_leader); }
    void break_state_path(Id id) {
        const auto path = node(id).path;
        std::filesystem::rename(path+"/state",path+"/state-down");
    }
};

void test_normal_queued_flow() {
    scenario = "normal queued flow";
    Cluster c(3);
    c.elect(1);
    c.propose(1,"duplicate-request");
    c.send_append(1,2);
    c.queue.push_back(c.queue.at(c.find(Kind::AppendRequest,1,2))); // 同一请求重复抵达，不多写一个索引。
    c.drain(); c.settle(1);
    CHECK(c.node(1).boot_commit == 1 && c.node(2).log.last_index() == 1);
    c.propose(1,"first"); c.settle(1);
    c.propose(1,std::string("binary\0value",12)); c.settle(1);
    CHECK(c.committed.size() == 3);
    for(const auto id : c.members) CHECK(c.node(id).boot_commit == 3);
    std::cout << "PASS queued election/proposal/replication/commit notice with real stores and global prefix checks\n";
}

void test_lagger_catches_up_in_bounded_batches() {
    scenario = "130 entries cross three bounded requests";
    Cluster c(3);
    c.elect(1); c.isolate(3);
    for(Id index = 1; index <= 130; ++index) c.propose(1,"command-"+std::to_string(index));
    c.settle(1);
    CHECK(c.node(1).boot_commit == 130 && c.node(2).boot_commit == 130);
    CHECK(c.node(3).log.last_index() == 0);
    c.heal(); c.expire(1,3); c.send_append(1,3); c.drain();
    const auto& batches = c.node(3).accepted_batch_sizes;
    CHECK(batches.size() == 3 && batches[0] == 64 && batches[1] == 64 && batches[2] == 2);
    CHECK(c.node(3).boot_commit == 130 && c.node(1).core.match_index(3) == 130);
    CHECK(c.node(1).core.next_index(3) == 131);
    std::cout << "PASS real-store lagger catches up 64+64+2; each ACK retains its own bounded request\n";
}

void test_split_and_duplicate_votes() {
    scenario = "split vote then higher election";
    Cluster split(3);
    split.timeout(1); split.timeout(2); split.timeout(3);
    split.drain();
    for(const auto id : split.members) CHECK(!split.node(id).active_leader);
    split.elect(1);
    CHECK(split.node(1).core.hard_state().current_term == 2);

    scenario = "duplicate vote below quorum";
    Cluster c(5);
    c.timeout(1);
    c.deliver(Kind::VoteRequest,1,2);
    auto reply = c.take(c.find(Kind::VoteResponse,2,1));
    c.queue.push_back(reply); c.queue.push_back(reply);
    c.deliver(Kind::VoteResponse,2,1); c.deliver(Kind::VoteResponse,2,1);
    CHECK(!c.node(1).active_leader && c.node(1).core.role() == Role::Candidate);
    c.deliver(Kind::VoteRequest,1,3); c.deliver(Kind::VoteResponse,3,1);
    CHECK(c.node(1).active_leader);
    c.drain();
    std::cout << "PASS split-vote recovery and duplicate grant cannot manufacture a majority\n";
}

void test_minority_partition_and_conflict_heal() {
    scenario = "2/5 isolated old leader versus 3/5 new leader";
    Cluster c(5);
    c.elect(1); c.propose(1,"base"); c.settle(1);
    c.partition({1,2},{3,4,5});
    c.propose(1,"minority-uncommitted"); c.settle(1);
    CHECK(c.node(1).boot_commit == 1 && c.committed.size() == 1);
    c.elect(3);
    c.propose(3,"majority-command"); c.settle(3);
    CHECK(c.committed.size() == 2 && c.committed.at(2).command == "majority-command");
    CHECK(c.node(1).boot_commit == 1 && c.node(2).boot_commit == 1);
    c.heal(); c.expire_all(3); c.settle(3);
    CHECK(c.node(1).core.role() == Role::Follower);
    for(const auto id : c.members) {
        CHECK(c.node(id).boot_commit == 2);
        LogEntry entry;
        CHECK(c.node(id).log.get(2,entry,c.node(id).error));
        CHECK(entry.command == "majority-command");
    }
    std::cout << "PASS minority cannot commit; majority elects/writes; heal replaces only uncommitted suffix\n";
}

void test_delayed_success_context() {
    scenario = "old success arrives after newer success";
    Cluster c(3);
    c.elect(1); c.propose(1,"one");
    c.send_append(1,2); c.deliver(Kind::AppendRequest,1,2);
    auto old = c.take(c.find(Kind::AppendResponse,2,1));
    CHECK(old.sent->entries.size() == 1);
    c.expire(1,2); // 原请求在外层超时，新请求可以发；旧回复仍可能在路上。
    c.propose(1,"two");
    c.send_append(1,2); c.deliver(Kind::AppendRequest,1,2);
    c.deliver(Kind::AppendResponse,2,1);
    CHECK(c.node(1).core.match_index(2) == 2 && c.node(1).boot_commit == 2);
    c.queue.push_back(old); c.queue.push_back(old);
    c.drain();
    CHECK(c.node(1).core.match_index(2) == 2 && c.node(1).boot_commit == 2);
    c.settle(1);
    std::cout << "PASS delayed/duplicate success keeps original sent range and cannot repeat or regress commit\n";
}

void test_delayed_failure_after_catchup() {
    scenario = "old failure arrives after catchup";
    Cluster c(3);
    c.elect(1); c.isolate(3);
    c.propose(1,"one"); c.settle(1);
    c.propose(1,"two"); c.settle(1);
    CHECK(c.node(2).boot_commit == 2 && c.node(3).log.last_index() == 0);
    c.heal(); c.isolate(1);
    c.elect(2); // 节点2日志到2；节点3有投票能力，但还没有日志。
    c.send_append(2,3); c.deliver(Kind::AppendRequest,2,3);
    auto old = c.take(c.find(Kind::AppendResponse,3,2));
    CHECK(!old.append_response.success && old.sent->prev_log_index == 2);
    c.expire(2,3);
    c.send_append(2,3); c.drain(); // 新失败使发送位置回退，最终补齐节点3。
    CHECK(c.node(2).core.match_index(3) == 2 && c.node(3).boot_commit == 2);
    c.queue.push_back(old); c.drain();
    CHECK(c.node(2).core.match_index(3) == 2 && c.node(2).core.next_index(3) == 3);
    CHECK(c.node(2).boot_commit == 2);
    std::cout << "PASS genuine delayed NACK after retry/catchup cannot retract confirmed prefix\n";
}

void test_old_reply_still_reports_higher_term() {
    scenario = "higher term in old reply while newer RPC is pending";
    Cluster c(3);
    c.elect(1); c.propose(1,"one"); c.send_append(1,2);
    auto old = c.take(c.find(Kind::AppendRequest,1,2));
    c.expire(1,2); c.propose(1,"two"); c.send_append(1,2);
    const auto newer_rpc = c.node(1).pending.at(2);
    CHECK(newer_rpc != old.rpc && old.sent->entries.size() == 1);
    c.timeout(2); // 节点2升到任期2，但它的投票请求还没送到节点1。
    c.queue.push_back(old); c.step(c.queue.size()-1);
    CHECK(c.node(1).pending.at(2) == newer_rpc);
    c.deliver(Kind::AppendResponse,2,1); // 对旧任期原请求的拒绝，携带较高任期2。
    CHECK(c.node(1).core.role() == Role::Follower && !c.node(1).active_leader);
    CHECK(c.node(1).core.hard_state().current_term == 2 && c.node(1).pending.empty());
    raft::HardState saved;
    CHECK(c.node(1).state.load(saved,c.node(1).error));
    CHECK(saved.current_term == 2 && saved.voted_for == 0);
    c.drain();
    std::cout << "PASS old RPC reply still persists higher term/steps down even with newer RPC pending\n";
}

void test_candidate_heartbeat_and_timer_flags() {
    scenario = "Candidate hears valid same-term leader";
    Cluster c(3);
    c.timeout(1); c.timeout(2);
    c.deliver(Kind::VoteRequest,1,3); c.deliver(Kind::VoteResponse,3,1);
    CHECK(c.node(1).active_leader && c.node(2).core.role() == Role::Candidate);
    const auto before = c.node(2).timer_resets;
    c.send_append(1,2); c.deliver(Kind::AppendRequest,1,2);
    CHECK(c.node(2).core.role() == Role::Follower);
    CHECK(c.node(2).timer_resets == before+1);
    raft::HardState saved;
    CHECK(c.node(2).state.load(saved,c.node(2).error));
    CHECK(saved.current_term == 1 && saved.voted_for == 2); // 退位不改写同任期已投的票。
    c.drain();
    std::cout << "PASS same-term heartbeat demotes Candidate, consumes timer flag, preserves durable vote\n";
}

void test_vote_survives_restart() {
    scenario = "restored vote refuses another candidate in same term";
    Cluster c(3);
    c.timeout(1); c.deliver(Kind::VoteRequest,1,2);
    c.take(c.find(Kind::VoteResponse,2,1)); // 该回复丢失，但节点2的投票已经保存。
    c.crash(2); CHECK(c.boot(2));
    CHECK(c.node(2).core.role() == Role::Follower);
    CHECK(c.node(2).core.hard_state().current_term == 1 && c.node(2).core.hard_state().voted_for == 1);
    c.timeout(3); // 节点3尚未处理别人的请求，此次竞选也是任期1。
    c.deliver(Kind::VoteRequest,3,2);
    auto denied = c.take(c.find(Kind::VoteResponse,2,3));
    CHECK(!denied.vote_response.vote_granted && denied.vote_response.term == 1);
    CHECK(c.node(2).core.hard_state().voted_for == 1);
    std::cout << "PASS actual StateStore reopen/rebuilt Core retains vote before lost reply\n";
}

void test_log_restart_tail_repair_and_new_leader() {
    scenario = "real log recovery and next election";
    Cluster c(3);
    c.elect(1); c.propose(1,"one"); c.settle(1); c.propose(1,"two"); c.settle(1);
    const auto path = c.node(2).path;
    c.crash(2);
    {
        std::ofstream tail(path+"/log/raft.log",std::ios::binary|std::ios::app);
        CHECK(tail.good()); tail.write("RL",2); CHECK(tail.good());
    }
    CHECK(c.boot(2)); // 真实LogStore修复不完整尾部，再以恢复值重建Core。
    CHECK(c.node(2).log.last_index() == 2 && c.node(2).core.role() == Role::Follower);
    CHECK(c.node(2).boot_commit == 0); // commit是易失的，不冒充已恢复KV/applied。
    c.elect(2);
    CHECK(c.node(2).core.hard_state().current_term == 2);
    CHECK(c.node(2).boot_commit == 0); // 旧term日志不能仅凭新选举直接提交。
    c.propose(2,"current-term-anchor"); c.settle(2);
    CHECK(c.committed.size() == 3);
    for(const auto id : c.members) CHECK(c.node(id).boot_commit == 3);
    std::cout << "PASS actual LogStore tail repair + rebuilt Core + current-term anchor safely relearns commit\n";
}

void test_complete_corruption_refuses_boot() {
    scenario = "complete record corruption is not an incomplete tail";
    Cluster c(3);
    c.elect(1); c.propose(1,"durable"); c.settle(1);
    const auto path = c.node(2).path+"/log/raft.log";
    const auto size = std::filesystem::file_size(path);
    c.crash(2);
    {
        std::fstream corrupt(path,std::ios::binary|std::ios::in|std::ios::out);
        CHECK(corrupt.good()); corrupt.put('X'); CHECK(corrupt.good());
    }
    CHECK(!c.boot(2));
    CHECK(!c.node(2).online && !c.node(2).core.good() && !c.node(2).active_leader);
    CHECK(!c.node(2).error.empty() && std::filesystem::file_size(path) == size);
    CHECK(c.queue.empty());
    std::cout << "PASS complete corrupt record refuses recovered Core startup without repair/leadership/messages\n";
}

void test_save_failure_has_no_publication() {
    scenario = "single member logical Leader cannot publish before term save";
    Cluster single(1);
    single.break_state_path(1); single.timeout(1);
    CHECK(!single.node(1).online && !single.node(1).core.good());
    CHECK(!single.node(1).active_leader && single.leader_by_term.empty() && single.queue.empty());

    scenario = "Follower term save failure cannot release ACK";
    Cluster c(3);
    c.timeout(1); c.deliver(Kind::VoteRequest,1,2); c.deliver(Kind::VoteResponse,2,1);
    c.take(c.find(Kind::VoteRequest,1,3)); // 节点3仍在任期0，没有收到这张拉票请求。
    CHECK(c.node(1).active_leader && c.queue.empty());
    c.propose(1,"pending"); c.break_state_path(3); c.send_append(1,3);
    c.deliver(Kind::AppendRequest,1,3);
    CHECK(!c.node(3).online && !c.node(3).core.good());
    CHECK(c.node(3).log.last_index() == 0 && c.queue.empty());
    CHECK(c.node(1).boot_commit == 0 && c.node(1).core.match_index(3) == 0);
    std::cout << "PASS actual save-path failures publish neither single-node leadership nor follower ACK/commit\n";
}
} // namespace

int main() {
    test_normal_queued_flow();
    test_lagger_catches_up_in_bounded_batches();
    test_split_and_duplicate_votes();
    test_minority_partition_and_conflict_heal();
    test_delayed_success_context();
    test_delayed_failure_after_catchup();
    test_old_reply_still_reports_higher_term();
    test_candidate_heartbeat_and_timer_flags();
    test_vote_survives_restart();
    test_log_restart_tail_repair_and_new_leader();
    test_complete_corruption_refuses_boot();
    test_save_failure_has_no_publication();
    std::cout << "raft Core scenarios PASS (queued calls + real stores; no TCP/Worker/KV apply)\n";
}
