#include "raft/core/apply.h"
#include "raft/core/logview.h"

#include <cstdlib>
#include <filesystem>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

using raft::AppendEntriesOut;
using raft::AppendEntriesRequest;
using raft::AppendEntriesResponse;
using raft::LogEntry;
using raft::RaftCore;

// 每个用例一个临时目录 + 真实的 StateStore / LogStore。
// 声明顺序就是析构的逆序：core 先没，然后 view、store、state。
class Env
{
public:
    explicit Env(std::size_t existing)
    {
        char pattern[] = "/tmp/raft-append-persist-XXXXXX";

        const char* made = ::mkdtemp(pattern);
        CHECK(made != nullptr);

        dir = made;

        state = std::make_unique<raft::StateStore>(dir);
        store = std::make_unique<raft::LogStore>(dir);

        CHECK(store->create(error));

        view = std::make_unique<raft::LogStoreView>(*store);
        core = std::make_unique<RaftCore>();

        CHECK(core->attach_log(view.get(),error));

        // 预置本地日志：前两条任期 1，后面任期 2，命令是 oldN
        std::uint64_t last_term = 0;

        for(std::size_t i = 0 ; i < existing ; i++)
        {
            const std::uint64_t term = i < 2 ? 1 : 2;

            const LogEntry entry{store->last_index()+1,term,"old"+std::to_string(i)};

            CHECK(store->append(entry,error));

            last_term = term;
        }

        CHECK(core->init(1,{1,2,3},{last_term,0},existing,last_term,error));
    }

    ~Env()
    {
        core.reset();
        view.reset();
        store.reset();
        state.reset();

        std::error_code ignored;
        std::filesystem::remove_all(dir,ignored);
    }

    std::uintmax_t log_size()const
    {
        std::error_code ignored;
        return std::filesystem::file_size(dir + "/raft.log",ignored);
    }

    std::string dir;
    std::unique_ptr<raft::StateStore> state;
    std::unique_ptr<raft::LogStore> store;
    std::unique_ptr<raft::LogStoreView> view;
    std::unique_ptr<RaftCore> core;
    std::string error;
};

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

// 接收 + 执行的完整一轮
void receive(Env& env,const AppendEntriesRequest& req,AppendEntriesOut& out)
{
    CHECK(env.core->handle_append_entries(req.leader_id,req,out,env.error));
}

void apply(Env& env,const AppendEntriesOut& out,AppendEntriesResponse& resp)
{
    CHECK(raft::apply_append_entries(*env.core,*env.state,*env.store,out,resp,env.error));
}

void test_accept_writes_log()
{
    Env env(0);

    auto req = make_request(3,2,0,0);
    req.entries = {{1,3,"a"},{2,3,"b"},{3,3,"c"}};

    AppendEntriesOut out;
    receive(env,req,out);
    CHECK(out.accept && out.entries.size() == 3);

    AppendEntriesResponse resp;
    apply(env,out,resp);

    CHECK(resp.success && resp.term == 3);
    CHECK(env.store->last_index() == 3);

    LogEntry entry;
    CHECK(env.store->get(3,entry,env.error));
    CHECK(entry.term == 3 && entry.command == "c");

    // 任期也要先落盘
    raft::HardState saved;
    CHECK(env.state->load(saved,env.error));
    CHECK(saved.current_term == 3 && saved.voted_for == 0);

    // 尾部缓存同步：下一次拉票报的尾部应该是 3/3
    raft::ElectionOutPut election;
    CHECK(env.core->start_election(election,env.error));
    CHECK(election.request.last_log_index == 3);
    CHECK(election.request.last_log_term == 3);

    std::cout << "PASS apply appends 3 entries / saves term / syncs core tail\n";
}

void test_reject_saves_term_but_keeps_log()
{
    Env env(3); // 本地 1,2,3；任期 1,1,2

    const auto size_before = env.log_size();

    // 更高任期，但前一条本地没有 → 拒绝
    auto req = make_request(4,2,9,3);

    AppendEntriesOut out;
    receive(env,req,out);
    CHECK(!out.accept && out.hard_state.has_value());

    AppendEntriesResponse resp;
    apply(env,out,resp);

    // 拒绝：success=false，但任期已经是 4
    CHECK(!resp.success && resp.term == 4);

    // 拒绝也必须把新任期落盘，否则重启后任期回退
    raft::HardState saved;
    CHECK(env.state->load(saved,env.error));
    CHECK(saved.current_term == 4 && saved.voted_for == 0);

    // 日志一行没动
    CHECK(env.store->last_index() == 3);
    CHECK(env.log_size() == size_before);

    std::cout << "PASS reject still persists higher term / log untouched\n";
}

void test_conflict_replaces_suffix()
{
    Env env(3); // 1,1,2

    auto req = make_request(3,2,2,1);
    req.entries = {{3,3,"new3"},{4,3,"new4"}};

    AppendEntriesOut out;
    receive(env,req,out);
    CHECK(out.accept && out.truncate_from == 3 && out.entries.size() == 2);

    AppendEntriesResponse resp;
    apply(env,out,resp);
    CHECK(resp.success);

    CHECK(env.store->last_index() == 4);

    LogEntry entry;
    CHECK(env.store->get(3,entry,env.error));
    CHECK(entry.term == 3 && entry.command == "new3"); // 旧的 old2 被替换

    CHECK(env.store->get(4,entry,env.error));
    CHECK(entry.term == 3 && entry.command == "new4");

    std::cout << "PASS conflict truncates suffix then appends leader version\n";
}

void test_duplicate_and_heartbeat_write_nothing()
{
    Env env(4); // 1,1,2,2

    const auto size_before = env.log_size();

    // 第 4 条本地已有且任期相同：内容不同也不算冲突，不覆盖
    auto duplicate = make_request(3,2,3,2);
    duplicate.entries = {{4,2,"different"}};

    AppendEntriesOut out;
    receive(env,duplicate,out);
    CHECK(out.accept && out.truncate_from == 0 && out.entries.empty());

    AppendEntriesResponse resp;
    apply(env,out,resp);
    CHECK(resp.success);
    CHECK(env.store->last_index() == 4);
    CHECK(env.log_size() == size_before);

    // 纯心跳：同样不写盘
    auto heartbeat = make_request(3,2,4,2);

    AppendEntriesOut beat;
    receive(env,heartbeat,beat);
    CHECK(beat.accept && beat.entries.empty());

    AppendEntriesResponse beat_resp;
    apply(env,beat,beat_resp);
    CHECK(beat_resp.success);
    CHECK(env.log_size() == size_before);

    std::cout << "PASS duplicate batch and heartbeat write nothing\n";
}

void test_state_save_failure_stops()
{
    Env env(0);

    // 指向不存在的目录：保存一定失败
    raft::StateStore broken("/tmp/raft-append-missing-directory");

    auto req = make_request(3,2,0,0);
    req.entries = {{1,3,"a"}};

    AppendEntriesOut out;
    receive(env,req,out);
    CHECK(out.accept && out.hard_state.has_value());

    AppendEntriesResponse resp;
    CHECK(!raft::apply_append_entries(*env.core,broken,*env.store,out,resp,env.error));

    // 保存失败：不回成功、节点停止、日志没写（顺序是先存任期再写日志）
    CHECK(!resp.success);
    CHECK(!env.core->good());
    CHECK(env.store->last_index() == 0);

    std::cout << "PASS state save failure stops core before writing log\n";
}

void test_log_write_failure_stops()
{
    Env env(2);

    // 手工造一个编号对不上的动作：本地最后是 2，这里从 5 开始
    AppendEntriesOut out;
    out.term = 2;
    out.accept = true;
    out.entries = {{5,2,"gap"}};

    AppendEntriesResponse resp;
    CHECK(!raft::apply_append_entries(*env.core,*env.state,*env.store,out,resp,env.error));

    CHECK(!resp.success);
    CHECK(!env.core->good());
    CHECK(env.store->last_index() == 2);

    std::cout << "PASS log append failure stops core without success reply\n";
}

void test_reopen_sees_same_log()
{
    Env env(0);

    auto req = make_request(3,2,0,0);
    req.entries = {{1,3,"a"},{2,3,"b"},{3,3,"c"}};

    AppendEntriesOut out;
    receive(env,req,out);

    AppendEntriesResponse resp;
    apply(env,out,resp);
    CHECK(resp.success);

    // 关掉再重新打开同一目录：磁盘上的内容必须一样
    env.core.reset();
    env.view.reset();
    env.store.reset();

    env.store = std::make_unique<raft::LogStore>(env.dir);
    CHECK(env.store->open(env.error));
    CHECK(env.store->last_index() == 3);

    LogEntry entry;
    CHECK(env.store->get(2,entry,env.error));
    CHECK(entry.term == 3 && entry.command == "b");

    std::cout << "PASS reopen after apply sees the same 3 entries\n";
}

} // namespace

int main()
{
    test_accept_writes_log();
    test_reject_saves_term_but_keeps_log();
    test_conflict_replaces_suffix();
    test_duplicate_and_heartbeat_write_nothing();
    test_state_save_failure_stops();
    test_log_write_failure_stops();
    test_reopen_sees_same_log();
    std::cout << "raft append-entries persistence tests PASS (real LogStore/StateStore in /tmp)\n";
}
