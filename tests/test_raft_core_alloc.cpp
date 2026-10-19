#include "raft/core.h"

#include <cstdlib>
#include <iostream>
#include <new>
#include <string>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

// 指定一次分配失败，随后恢复分配，允许错误信息正常构造。
static int fail_after = -1;
static int hits = 0;

void* operator new(std::size_t size) {
    if (fail_after == 0) {
        fail_after = -1;
        ++hits;
        throw std::bad_alloc();
    }
    if (fail_after > 0) --fail_after;
    void* p = std::malloc(size == 0 ? 1 : size);
    if (!p) throw std::bad_alloc();
    return p;
}
void operator delete(void* p) noexcept { std::free(p); }
void operator delete(void* p, std::size_t) noexcept { std::free(p); }

namespace {
void same_output(const raft::ElectionOutPut& a, const raft::ElectionOutPut& b) {
    CHECK(a.hard_state.current_term == b.hard_state.current_term);
    CHECK(a.hard_state.voted_for == b.hard_state.voted_for);
    CHECK(a.request.term == b.request.term && a.request.candidate_id == b.request.candidate_id);
    CHECK(a.request.last_log_index == b.request.last_log_index);
    CHECK(a.request.last_log_term == b.request.last_log_term);
    CHECK(a.targets == b.targets);
}

void test_init_alloc() {
    const std::vector<std::uint64_t> members{1,2,3};
    for (int point = 0; point < 2; ++point) {
        raft::RaftCore core;
        std::string error;
        error.reserve(256);
        hits = 0;
        fail_after = point;
        const bool ok = core.init(1, members, {7,2}, 20,5, error);
        fail_after = -1;
        CHECK(!ok && hits == 1 && error.find("allocation") != std::string::npos);
        CHECK(!core.good() && core.majority() == 0);
        CHECK(core.hard_state().current_term == 0 && core.hard_state().voted_for == 0);
        CHECK(core.init(1, members, {7,2}, 20,5, error));
        CHECK(core.good() && error.empty());
        CHECK(core.hard_state().current_term == 7 && core.hard_state().voted_for == 2);
    }
    std::cout << "PASS init members-copy/votes-reserve allocation failures and retry\n";
}

void test_start_alloc() {
    raft::RaftCore core;
    std::string error;
    error.reserve(256);
    CHECK(core.init(2, {1,2,3}, {7,3}, 20,5, error));
    raft::ElectionOutPut out;
    out.hard_state = {90,91};
    out.request = {92,93,94,95};
    out.targets = {96,97,98};
    for (int round = 0; round < 2; ++round) {
        const auto before = out;
        const auto state = core.hard_state();
        const auto role = core.role();
        hits = 0;
        fail_after = 0;
        const bool ok = core.start_election(out, error);
        fail_after = -1;
        CHECK(!ok && hits == 1 && error.find("allocation") != std::string::npos);
        CHECK(core.good() && core.role() == role);
        CHECK(core.hard_state().current_term == state.current_term);
        CHECK(core.hard_state().voted_for == state.voted_for);
        same_output(out, before);
        CHECK(core.start_election(out, error));
        CHECK(error.empty() && core.role() == raft::Role::Candidate);
        CHECK(core.hard_state().current_term == state.current_term + 1);
        CHECK(core.hard_state().voted_for == 2);
        CHECK((out.targets == std::vector<std::uint64_t>{1,3}));
        // 单测模型在下一轮前视为已经消费本轮持久化动作。
        CHECK(out.hard_state.current_term == out.request.term);
        CHECK(out.hard_state.voted_for == out.request.candidate_id);
    }
    std::cout << "PASS Follower/Candidate target allocation failures; state/out retained; retry\n";
}

void test_single_no_alloc() {
    raft::RaftCore core;
    std::string error;
    CHECK(core.init(1, {1}, {0,0}, 0,0, error));
    raft::ElectionOutPut out;
    hits = 0;
    fail_after = 0;
    const bool ok = core.start_election(out, error);
    fail_after = -1;
    CHECK(ok && hits == 0 && error.empty());
    CHECK(core.role() == raft::Role::Leader && out.targets.empty());
    CHECK(out.hard_state.current_term == 1 && out.hard_state.voted_for == 1);
    std::cout << "PASS single-node start requires no new vector allocation\n";
}
} // namespace

int main() {
    test_init_alloc();
    test_start_alloc();
    test_single_no_alloc();
    std::cout << "raft core one-shot allocation fault tests PASS\n";
}
