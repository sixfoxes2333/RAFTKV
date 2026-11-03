#include "raft/core/apply.h"
#include "raft/core/logview.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <limits>
#include <string>
#include <utility>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {
using raft::AppendEntriesRequest;
using raft::CommitOut;
using raft::LogEntry;
using raft::Role;

void empty(const CommitOut& out) {
    CHECK(out.committed_from == 0 && out.committed_to == 0);
}
void range(const CommitOut& out,std::uint64_t first,std::uint64_t last) {
    CHECK(out.committed_from == first && out.committed_to == last);
}
void same(const LogEntry& a,const LogEntry& b) {
    CHECK(a.index == b.index && a.term == b.term && a.command == b.command);
}

struct TempDir {
    std::string path;
    TempDir() {
        char pattern[] = "/tmp/raft-follower-commit-XXXXXX";
        const char* made = ::mkdtemp(pattern);
        CHECK(made != nullptr);
        path = made;
    }
    ~TempDir() {
        std::error_code ignored;
        std::filesystem::remove_all(path,ignored);
    }
};

// 默认转发给真实LogStore；指定位置可注入term查询失败，检查保存后的错误处理。
class ControlledView final : public raft::LogView {
public:
    explicit ControlledView(raft::LogStore& store) : actual_(store) {}
    std::uint64_t fail_term_at = 0;
    std::uint64_t last_index() const override { return actual_.last_index(); }
    bool term_at(std::uint64_t index,std::uint64_t& out) const override {
        if(index != 0 && index == fail_term_at) return false;
        return actual_.term_at(index,out);
    }
    bool entry_at(std::uint64_t index,LogEntry& out) const override {
        return actual_.entry_at(index,out);
    }
private:
    raft::LogStoreView actual_;
};

struct Node {
    // 节点销毁后才清理目录，Core和视图先于它们借用的存储销毁。
    TempDir dir;
    raft::LogStore log;
    raft::StateStore state;
    ControlledView view;
    raft::RaftCore core;
    std::uint64_t id;
    std::string error;
    explicit Node(std::uint64_t self = 2,std::size_t existing = 0)
        : log(dir.path+"/log"),state(dir.path+"/state"),view(log),id(self) {
        CHECK(std::filesystem::create_directory(dir.path+"/log"));
        CHECK(std::filesystem::create_directory(dir.path+"/state"));
        CHECK(log.create(error));
        CHECK(state.save({2,0},error));
        for(std::size_t i = 1; i <= existing; ++i)
            CHECK(log.append({i,2,"old"+std::to_string(i)},error));
        raft::HardState recovered;
        CHECK(state.load(recovered,error));
        CHECK(core.attach_log(&view,error));
        CHECK(core.init(id,{1,2,3},recovered,existing,existing == 0 ? 0 : 2,error));
    }
    LogEntry propose(const std::string& command) {
        LogEntry entry;
        CHECK(core.build_proposal(command,entry,error));
        CHECK(log.append(entry,error));
        CHECK(core.on_log_persisted(error));
        return entry;
    }
    AppendEntriesRequest request(std::uint64_t peer) {
        AppendEntriesRequest sent;
        CHECK(core.build_append_entries(peer,sent,error));
        return sent;
    }
    CommitOut advance() {
        CommitOut out{90,91};
        CHECK(core.advance_commit(out,error));
        CHECK(error.empty());
        return out;
    }
};

struct Delivery {
    bool handled = false;
    bool saved = false;
    bool commit_called = false;
    bool reply_ready = false;
    raft::AppendEntriesOut action;
    raft::AppendEntriesResponse response;
    CommitOut committed;
};

// 这是测试扮演的外层调度，不是已经实现了RaftWorker。
// 一次请求必须核对、保存、更新提交位置都成功，才把成功回复标为可发。
Delivery deliver(Node& follower,const AppendEntriesRequest& request) {
    Delivery out;
    out.handled = follower.core.handle_append_entries(request.leader_id,request,out.action,follower.error);
    if(!out.handled) {
        follower.core.stop();
        return out;
    }
    out.saved = raft::apply_append_entries(follower.core,follower.state,follower.log,
                                          out.action,out.response,follower.error);
    if(!out.saved) return out; // 真实执行函数已停止Core，不能继续提交或确认。
    if(!out.response.success) {
        out.reply_ready = true; // 正常拒绝可以回false，但不调用提交方法。
        return out;
    }
    out.commit_called = true;
    if(!follower.core.advance_follower_commit(request,out.committed,follower.error)) {
        follower.core.stop();
        out.response.success = false;
        return out;
    }
    out.reply_ready = true;
    return out;
}

void good(const Delivery& out) {
    CHECK(out.handled && out.saved && out.commit_called && out.reply_ready);
    CHECK(out.response.success);
}
AppendEntriesRequest make(std::uint64_t prev = 0,std::uint64_t prev_term = 0,
                          std::uint64_t commit = 0) {
    AppendEntriesRequest request;
    request.term = 3;
    request.leader_id = 1;
    request.prev_log_index = prev;
    request.prev_log_term = prev_term;
    request.leader_commit = commit;
    return request;
}
void reject(raft::RaftCore& core,const AppendEntriesRequest& request,std::string& error) {
    CommitOut out{90,91};
    CHECK(!core.advance_follower_commit(request,out,error));
    CHECK(!error.empty());
    range(out,90,91);
}

void test_original_role_and_term_fences() {
    Node node;
    auto request = make(0,0,1);
    request.entries = {{1,3,"command"}};
    const auto first = deliver(node,request);
    good(first);
    CHECK(node.core.role() == Role::Follower);
    range(first.committed,1,1);
    CommitOut again;
    CHECK(node.core.advance_follower_commit(request,again,node.error));
    empty(again);
    // 故意传错回调上下文，不把这当作正常收包/保存流程。
    for(const auto term : {std::uint64_t{2},std::uint64_t{4}}) {
        auto wrong = request;
        wrong.term = term;
        reject(node.core,wrong,node.error);
    }
    std::cout << "PASS role/term red-green fences: saved Follower advances once; wrong callback epochs rejected\n";
}

void test_bound_uses_original_request_not_local_tail_or_write_suffix() {
    Node node(2,10);
    auto request = make(5,2,9);
    request.entries = {{6,2,"old6"},{7,2,"old7"}};
    const auto first = deliver(node,request);
    good(first);
    CHECK(first.action.entries.empty()); // 重复条目不用重写，但仍算匹配到7，不只是5。
    CHECK(node.log.last_index() == 10);
    range(first.committed,1,7); // 不能用本机尾部10，把未核对的8/9也提交。
    const auto duplicate = deliver(node,request);
    good(duplicate);
    empty(duplicate.committed);
    const auto beat = deliver(node,make(10,2,9)); // 这次空心跳已核对到10，可以跟到9。
    good(beat);
    range(beat.committed,8,9);
    const auto late = deliver(node,make(10,2,6)); // 迟到的旧提交通知不能收回9。
    good(late);
    empty(late.committed);
    const auto finish = deliver(node,make(10,2,10));
    good(finish);
    range(finish.committed,10,10);
    std::cout << "PASS original matched end bounds commit; duplicates count; heartbeat advances; late notice cannot regress\n";
}

void test_leader_limit_and_empty_log() {
    Node empty_node;
    const auto nothing = deliver(empty_node,make(0,0,9));
    good(nothing);
    empty(nothing.committed);
    CHECK(empty_node.log.last_index() == 0);
    Node node;
    auto request = make(0,0,2);
    for(std::uint64_t i = 1; i <= 5; ++i)
        request.entries.push_back({i,3,"new"+std::to_string(i)});
    const auto first = deliver(node,request);
    good(first);
    range(first.committed,1,2); // 存到5也不能超过leader明确通知的2。
    CHECK(node.log.last_index() == 5);
    const auto rest = deliver(node,make(5,3,5));
    good(rest);
    range(rest.committed,3,5);
    std::cout << "PASS empty log cannot commit absent entries; local matched end cannot exceed leader commit notice\n";
}

void test_rejection_does_not_call_commit() {
    Node node(2,3);
    const auto missing = deliver(node,make(4,2,100));
    CHECK(missing.handled && missing.saved && missing.reply_ready);
    CHECK(!missing.response.success && !missing.commit_called);
    empty(missing.committed);
    raft::HardState saved;
    CHECK(node.state.load(saved,node.error) && saved.current_term == 3);
    const auto mismatch = deliver(node,make(2,1,3));
    CHECK(mismatch.handled && mismatch.saved && mismatch.reply_ready);
    CHECK(!mismatch.response.success && !mismatch.commit_called);
    const auto now_valid = deliver(node,make(3,2,3));
    good(now_valid);
    range(now_valid.committed,1,3); // 前面拒绝过的通知没有提前提交任何一条。
    CHECK(node.core.good());
    std::cout << "PASS missing/mismatched prev rejects: save higher term if needed, never enter follower commit\n";
}

void test_conflict_replacement_and_committed_prefix() {
    Node node(2,6);
    auto request = make(2,2,4);
    request.entries = {{3,3,"replace3"},{4,3,"replace4"}};
    const auto first = deliver(node,request);
    good(first);
    CHECK(first.action.truncate_from == 3 && node.log.last_index() == 4);
    range(first.committed,1,4);
    // 故意构造企图覆盖已提交前缀的请求，检查Core拒绝，而非模拟合法当选历史。
    auto unsafe = make(2,2,4);
    unsafe.term = 4;
    unsafe.entries = {{3,4,"overwrite committed entry"}};
    const auto rejected = deliver(node,unsafe);
    CHECK(!rejected.handled && !rejected.reply_ready && !rejected.commit_called);
    CHECK(!node.core.good()); // 测试外层收到处理错误后停止，不发布未完成动作。
    CHECK(node.error.find("conflict below commit") != std::string::npos);
    LogEntry actual;
    CHECK(node.log.get(3,actual,node.error));
    same(actual,request.entries.front());
    std::cout << "PASS replace uncommitted suffix, then refuse conflicting committed prefix\n";
}

void test_callback_guards_and_unsynchronized_tail() {
    Node node;
    good(deliver(node,make())); // 先接受任期3的空请求，保存新term并同步空尾部。
    auto request = make(0,0,1);
    request.entries = {{1,3,"pending"}};
    raft::AppendEntriesOut action;
    CHECK(node.core.handle_append_entries(1,request,action,node.error) && action.accept);
    CHECK(node.log.append_batch(action.entries,node.error));
    // 故意漏掉同步，单测应看到回调拒绝；不是建议线上这样调度。
    reject(node.core,request,node.error);
    CHECK(node.core.on_log_persisted(node.error));
    CommitOut committed;
    CHECK(node.core.advance_follower_commit(request,committed,node.error));
    range(committed,1,1);
    auto overflow = request;
    overflow.prev_log_index = std::numeric_limits<std::uint64_t>::max();
    reject(node.core,overflow,node.error);
    raft::RaftCore fresh;
    reject(fresh,request,node.error);
    raft::RaftCore no_view;
    CHECK(no_view.init(2,{1,2,3},{3,1},0,0,node.error));
    reject(no_view,request,node.error);
    raft::ElectionOutPut election;
    CHECK(node.core.start_election(election,node.error));
    CHECK(node.state.save(election.hard_state,node.error));
    auto same_epoch = request;
    same_epoch.term = election.hard_state.current_term;
    CHECK(node.core.role() == Role::Candidate);
    reject(node.core,same_epoch,node.error);
    raft::VoteCountOutPut vote;
    CHECK(node.core.handle_vote_response(1,{same_epoch.term,true},vote,node.error) && vote.become_leader);
    reject(node.core,same_epoch,node.error);
    node.core.stop();
    reject(node.core,same_epoch,node.error);
    std::cout << "PASS callback rejects unsynchronized tail/overflow/uninitialized/no-view/Candidate/Leader/stopped\n";
}

void test_state_save_failure_releases_nothing() {
    Node node;
    // 移走自己测试目录内的state子目录，让真实StateStore无法打开原路径。
    // LogStore在独立子目录，仍保持可用，便于证明失败发生在写日志之前。
    std::filesystem::rename(node.dir.path+"/state",node.dir.path+"/state-moved");
    auto request = make(0,0,1);
    request.entries = {{1,3,"not saved"}};
    const auto failed = deliver(node,request);
    CHECK(failed.handled && !failed.saved && !failed.reply_ready);
    CHECK(!failed.response.success && !failed.commit_called && !node.core.good());
    empty(failed.committed);
    CHECK(node.log.good() && node.log.last_index() == 0);
    raft::StateStore old_state(node.dir.path+"/state-moved");
    raft::HardState saved;
    CHECK(old_state.load(saved,node.error) && saved.current_term == 2);
    std::cout << "PASS actual state-directory save failure stops executor before log write/commit/success reply\n";
}

void test_saved_log_but_tail_read_failure_releases_nothing() {
    Node node;
    node.view.fail_term_at = 1;
    auto request = make(0,0,1);
    request.entries = {{1,3,"saved but unconfirmed"}};
    const auto failed = deliver(node,request);
    CHECK(failed.handled && !failed.saved && !failed.reply_ready);
    CHECK(!failed.response.success && !failed.commit_called && !node.core.good());
    empty(failed.committed);
    LogEntry actual;
    CHECK(node.log.get(1,actual,node.error));
    same(actual,request.entries.front()); // 文件可能已改变；失败不等于帮你回滚了。
    reject(node.core,request,node.error);
    std::cout << "PASS injected tail-term query failure after actual log save stops before commit/success reply\n";
}

void elect(Node& leader,Node& voter) {
    raft::ElectionOutPut election;
    CHECK(leader.core.start_election(election,leader.error));
    CHECK(leader.state.save(election.hard_state,leader.error));
    raft::VoteOutPut vote;
    CHECK(voter.core.handle_request_vote(leader.id,election.request,vote,voter.error));
    if(vote.hard_state) CHECK(voter.state.save(*vote.hard_state,voter.error));
    CHECK(vote.response.vote_granted);
    raft::VoteCountOutPut counted;
    CHECK(leader.core.handle_vote_response(voter.id,vote.response,counted,leader.error) && counted.become_leader);
}
void count_reply(Node& leader,std::uint64_t peer,const AppendEntriesRequest& sent,const Delivery& out) {
    CHECK(out.reply_ready);
    raft::AppendResponseOut action;
    CHECK(leader.core.handle_append_entries_response(peer,sent,out.response,action,leader.error));
    if(action.hard_state) CHECK(leader.state.save(*action.hard_state,leader.error));
}

void test_real_commit_notice_roundtrip() {
    Node leader(1), follower(2), lagger(3);
    elect(leader,follower);
    for(std::uint64_t index = 1; index <= 2; ++index) {
        // 节点1：先保存自己的新命令。这时只有自己一份，不能提交。
        const auto entry = leader.propose(std::string("SET\0value",9)+std::to_string(index));
        CHECK(entry.index == index);
        empty(leader.advance());
        const auto sent = leader.request(2);
        CHECK(sent.leader_commit == index-1);
        // 节点2：保存新日志后先回成功，但原请求仍只通知了旧提交位置。
        const auto saved = deliver(follower,sent);
        good(saved);
        empty(saved.committed);
        count_reply(leader,2,sent,saved);
        // 节点1：收到节点2确认后，两份达到多数，本机才决定提交这一条。
        range(leader.advance(),index,index);
        // 节点1发出新通知；节点2收到后，才把自己的提交位置跟上。
        const auto heartbeat = leader.request(2);
        CHECK(heartbeat.entries.empty() && heartbeat.leader_commit == index);
        const auto noticed = deliver(follower,heartbeat);
        good(noticed);
        range(noticed.committed,index,index);
        count_reply(leader,2,heartbeat,noticed);
        empty(leader.advance());
        // 节点3晚收到数据，请求已经携带新commit，可在同一次保存后直接跟上。
        const auto late_request = leader.request(3);
        CHECK(late_request.entries.size() == 1 && late_request.leader_commit == index);
        const auto caught_up = deliver(lagger,late_request);
        good(caught_up);
        range(caught_up.committed,index,index);
        count_reply(leader,3,late_request,caught_up);
        empty(leader.advance());
        LogEntry actual;
        CHECK(follower.log.get(index,actual,follower.error));
        same(actual,entry);
        CHECK(lagger.log.get(index,actual,lagger.error));
        same(actual,entry);
        const auto duplicate = deliver(follower,heartbeat);
        good(duplicate);
        empty(duplicate.committed);
    }
    CHECK(leader.log.last_index() == 2 && follower.log.last_index() == 2 && lagger.log.last_index() == 2);
    std::cout << "PASS real stores/3 Core objects: propose-save-ACK-leader commit-notice-follower commit, two rounds\n";
}

void test_m5_existing_term_read_failure_must_not_be_a_conflict() {
    Node node(2,3);
    auto request = make(0,0,2);
    request.entries = {{1,2,"old1"},{2,2,"old2"}};
    // 两条内容本来都一致，第3条应保留。这里只注入视图的第1条term查询失败，
    // 不是把第1条改成另一任期，也不是模拟真实文件层pwrite/fsync失败。
    node.view.fail_term_at = 1;
    const auto failed = deliver(node,request);
    if(failed.handled || failed.response.success || failed.commit_called) {
        std::cerr << "M5 term_at(1)=false: handled=" << failed.handled
                  << " success=" << failed.response.success
                  << " commit_called=" << failed.commit_called
                  << " committed_to=" << failed.committed.committed_to
                  << " log_last=" << node.log.last_index() << '\n';
    }
    // 读不到本地term就报错，不能编造冲突、删除后缀，再回成功。
    CHECK(!failed.handled);
    CHECK(!failed.reply_ready && !failed.response.success && !failed.commit_called);
    CHECK(!node.core.good()); // 测试外层按运行期查询错误停止本轮协议处理。
    CHECK(node.log.last_index() == 3);
    LogEntry last;
    CHECK(node.log.get(3,last,node.error));
    CHECK(last.term == 2 && last.command == "old3");
    std::cout << "PASS M5 existing-entry term query failure stops before conflict actions/reply/commit\n";
}

} // namespace

int main() {
    test_original_role_and_term_fences();
    test_bound_uses_original_request_not_local_tail_or_write_suffix();
    test_leader_limit_and_empty_log();
    test_rejection_does_not_call_commit();
    test_conflict_replacement_and_committed_prefix();
    test_callback_guards_and_unsynchronized_tail();
    test_state_save_failure_releases_nothing();
    test_saved_log_but_tail_read_failure_releases_nothing();
    test_real_commit_notice_roundtrip();
    test_m5_existing_term_read_failure_must_not_be_a_conflict();
    std::cout << "raft follower-commit tests PASS (real stores + test-driven Core calls; no sockets/Worker/KV apply)\n";
}
