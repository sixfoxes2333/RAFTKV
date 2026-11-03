#include "raft/core/core.h"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {
using raft::ElectionOutPut;
using raft::RaftCore;
using raft::Role;

void check_state(const RaftCore& core, std::uint64_t term,
                 std::uint64_t vote, Role role) {
    CHECK(core.hard_state().current_term == term);
    CHECK(core.hard_state().voted_for == vote);
    CHECK(core.role() == role);
}

ElectionOutPut sentinel() {
    ElectionOutPut out;
    out.hard_state = {90, 91};
    out.request = {92, 93, 94, 95};
    out.targets = {96, 97, 98};
    return out;
}

void check_same(const ElectionOutPut& a, const ElectionOutPut& b) {
    CHECK(a.hard_state.current_term == b.hard_state.current_term);
    CHECK(a.hard_state.voted_for == b.hard_state.voted_for);
    CHECK(a.request.term == b.request.term);
    CHECK(a.request.candidate_id == b.request.candidate_id);
    CHECK(a.request.last_log_index == b.request.last_log_index);
    CHECK(a.request.last_log_term == b.request.last_log_term);
    CHECK(a.targets == b.targets);
}

// Core 单测中的外层消费模型，不执行真实保存或发送。
raft::HardState consume_model(const ElectionOutPut& out) {
    CHECK(out.hard_state.current_term == out.request.term);
    CHECK(out.hard_state.voted_for == out.request.candidate_id);
    return out.hard_state;
}

void test_init() {
    static_assert(!std::is_copy_constructible<RaftCore>::value, "no copy");
    static_assert(!std::is_move_constructible<RaftCore>::value, "no move");
    static_assert(!std::is_copy_assignable<RaftCore>::value, "no copy assignment");
    static_assert(!std::is_move_assignable<RaftCore>::value, "no move assignment");
    static_assert(std::is_same<decltype(std::declval<const RaftCore&>().hard_state()),
                               const raft::HardState&>::value, "read-only state borrow");
    std::string error = "old error";
    RaftCore core;
    CHECK(!core.good() && core.majority() == 0);
    check_state(core, 0, 0, Role::Follower);
    CHECK(core.init(2, {3, 2, 1}, {7, 3}, 20, 5, error));
    CHECK(error.empty() && core.good() && core.majority() == 2);
    check_state(core, 7, 3, Role::Follower);
    CHECK(!core.init(2, {2}, {0, 0}, 0, 0, error));
    CHECK(!error.empty() && core.good() && core.majority() == 2);
    check_state(core, 7, 3, Role::Follower);
    core.stop();
    core.stop();
    CHECK(!core.good());
    CHECK(!core.init(2, {3, 2, 1}, {8, 0}, 20, 5, error));
    check_state(core, 7, 3, Role::Follower);
    RaftCore stopped;
    stopped.stop();
    CHECK(!stopped.init(1, {1}, {0, 0}, 0, 0, error));
    std::cout << "PASS init/recovery/reinit/stop/type contract\n";
}

void test_bad_init() {
    struct Bad {
        std::uint64_t self;
        std::vector<std::uint64_t> members;
        raft::HardState state;
        std::uint64_t index;
        std::uint64_t term;
        const char* error;
    };
    const std::vector<Bad> cases{
        {0, {1,2,3}, {0,0}, 0,0, "nonzero self"},
        {1, {}, {0,0}, 0,0, "nonempty members"},
        {1, {1,0,3}, {0,0}, 0,0, "member id must be nonzero"},
        {1, {1,1,3}, {0,0}, 0,0, "duplicate member"},
        {1, {1,2,2}, {0,0}, 0,0, "duplicate member"},
        {1, {2,3}, {0,0}, 0,0, "self id not in members"},
        {1, {1,2,3}, {0,2}, 0,0, "invalid recovered vote"},
        {1, {1,2,3}, {5,9}, 0,0, "invalid recovered vote"},
        {1, {1,2,3}, {5,0}, 0,1, "inconsistent recovered log"},
        {1, {1,2,3}, {5,0}, 1,0, "inconsistent recovered log"},
        {1, {1,2,3}, {5,0}, 1,6, "inconsistent recovered log"}
    };
    for (const auto& c : cases) {
        RaftCore core;
        std::string error = "old error";
        CHECK(!core.init(c.self, c.members, c.state, c.index, c.term, error));
        CHECK(error.find(c.error) != std::string::npos);
        CHECK(!core.good() && core.majority() == 0);
        check_state(core, 0, 0, Role::Follower);
        CHECK(core.init(1, {1,2,3}, {7,2}, 100,6, error));
        CHECK(error.empty() && core.good());
        check_state(core, 7, 2, Role::Follower);
    }
    std::cout << "PASS 11 invalid init inputs/public state/retry\n";
}

void test_start() {
    RaftCore core;
    std::string error;
    auto out = sentinel();
    const auto original = out;
    CHECK(!core.start_election(out, error));
    CHECK(error.find("not ready") != std::string::npos);
    check_same(out, original);
    CHECK(core.init(2, {3,2,1}, {7,3}, 20,5, error));
    error = "old error";
    CHECK(core.start_election(out, error));
    CHECK(error.empty() && core.good());
    check_state(core, 8, 2, Role::Candidate);
    CHECK(out.hard_state.current_term == 8 && out.hard_state.voted_for == 2);
    CHECK(out.request.term == 8 && out.request.candidate_id == 2);
    CHECK(out.request.last_log_index == 20 && out.request.last_log_term == 5);
    CHECK((out.targets == std::vector<std::uint64_t>{3,1}));
    CHECK(consume_model(out).current_term == 8);
    CHECK(core.start_election(out, error));
    check_state(core, 9, 2, Role::Candidate);
    CHECK(out.targets.size() == 2 && out.request.term == 9);
    CHECK(consume_model(out).current_term == 9);
    core.stop();
    const auto before = out;
    CHECK(!core.start_election(out, error));
    CHECK(error.find("not ready") != std::string::npos);
    check_same(out, before);
    check_state(core, 9, 2, Role::Candidate);
    std::cout << "PASS start/self vote/log tail/targets/candidate retry/stopped\n";
}

void test_members() {
    std::string error;
    for (std::size_t n = 1; n <= 5; ++n) {
        std::vector<std::uint64_t> members;
        for (std::size_t id = 1; id <= n; ++id) members.push_back(id);
        RaftCore core;
        ElectionOutPut out;
        CHECK(core.init(n, members, {0,0}, 0,0, error));
        CHECK(core.majority() == n/2+1);
        CHECK(core.start_election(out, error));
        check_state(core, 1, n, n == 1 ? Role::Leader : Role::Candidate);
        CHECK(out.targets.size() == n-1);
        for (std::size_t i = 0; i < out.targets.size(); ++i) CHECK(out.targets[i] == i+1);
        CHECK(out.request.last_log_index == 0 && out.request.last_log_term == 0);
        CHECK(consume_model(out).voted_for == n);
        if (n == 1) {
            const auto before = out;
            CHECK(!core.start_election(out, error));
            CHECK(error.find("already leader") != std::string::npos);
            check_same(out, before);
        }
    }
    RaftCore sparse;
    ElectionOutPut out;
    CHECK(sparse.init(300, {500,100,300}, {9,500}, 10000,8, error));
    CHECK(sparse.start_election(out, error));
    CHECK((out.targets == std::vector<std::uint64_t>{500,100}));
    CHECK(out.request.candidate_id == 300 && out.request.term == 10);
    CHECK(consume_model(out).current_term == 10);
    std::cout << "PASS N=1..5 quorum/single-node logical leader/sparse IDs\n";
}

void test_term_limit() {
    const auto max = std::numeric_limits<std::uint64_t>::max();
    std::string error;
    auto out = sentinel();
    const auto before = out;
    RaftCore exhausted;
    CHECK(exhausted.init(1, {1,2,3}, {max,0}, 0,0, error));
    CHECK(!exhausted.start_election(out, error));
    CHECK(error.find("overflow") != std::string::npos);
    CHECK(exhausted.good());
    check_state(exhausted, max, 0, Role::Follower);
    check_same(out, before);
    RaftCore final_round;
    CHECK(final_round.init(max, {1,2,max}, {max-1,1}, max,max-1, error));
    CHECK(final_round.start_election(out, error));
    check_state(final_round, max, max, Role::Candidate);
    CHECK(out.request.term == max && out.request.last_log_index == max);
    CHECK(out.request.last_log_term == max-1);
    CHECK((out.targets == std::vector<std::uint64_t>{1,2}));
    CHECK(consume_model(out).current_term == max);
    const auto last = out;
    CHECK(!final_round.start_election(out, error));
    check_same(out, last);
    check_state(final_round, max, max, Role::Candidate);
    std::cout << "PASS uint64 term/index/ID limits and no wraparound\n";
}
} // namespace

int main() {
    test_init();
    test_bad_init();
    test_start();
    test_members();
    test_term_limit();
    std::cout << "raft core startup/start-election tests PASS (no Worker/transport)\n";
}
