#include "raft/core/core.h"

#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

// 内存中的单个节点：Core + "已保存"的状态副本。
// 真实系统里这份副本由 StateStore 落在磁盘；这里只记录被要求保存的内容。
struct Node {
    raft::RaftCore core;
    raft::HardState saved{};   // 最近一次被要求持久化的 term/vote
    int save_count = 0;        // 被要求保存的次数
    int send_count = 0;        // 向外发出的 RequestVote 数量

    bool up = true;            // false 表示模拟宕机：不收不发

    Node(std::uint64_t id, std::vector<std::uint64_t> members,
         std::uint64_t term, std::uint64_t vote,
         std::uint64_t index, std::uint64_t lterm) {
        std::string error;
        CHECK(core.init(id, members, {term, vote}, index, lterm, error));
        saved = {term, vote};
    }

    // 处理需要保存的状态：有值就当作写盘成功并记录。
    void persist(const std::optional<raft::HardState>& state) {
        if (state.has_value()) {
            saved = *state;
            ++save_count;
        }
    }
};

// 一轮竞选产生的消息：谁发给谁。
struct VoteMessage {
    std::uint64_t from = 0;
    std::uint64_t to = 0;
    raft::RequestVoteRequest request{};
};

// 一轮竞选收到的回复：谁回给谁。
struct ReplyMessage {
    std::uint64_t from = 0;
    std::uint64_t to = 0;
    raft::RequestVoteResponse response{};
};

struct Cluster {
    // Core 不可复制/移动，节点放在 unique_ptr 里；下标 0 对应节点 ID 1。
    std::vector<std::unique_ptr<Node>> nodes;

    // member_count 决定集群大小：3 节点多数为 2，5 节点多数为 3。
    explicit Cluster(std::uint64_t term, std::uint64_t member_count = 3) {
        std::vector<std::uint64_t> members;
        for (std::uint64_t id = 1; id <= member_count; ++id) members.push_back(id);
        for (std::uint64_t id = 1; id <= member_count; ++id) {
            nodes.push_back(std::make_unique<Node>(id, members, term, 0, 10, term));
        }
    }

    Node& at(std::uint64_t id) { return *nodes[static_cast<std::size_t>(id - 1)]; }

    // 让某个节点发起选举：保存它的输出、扣下要发出的请求。
    std::vector<VoteMessage> start(std::uint64_t id) {
        raft::ElectionOutPut out;
        std::string error;
        CHECK(at(id).core.start_election(out, error));
        CHECK(error.empty());
        at(id).persist(out.hard_state);
        std::vector<VoteMessage> messages;
        for (const auto target : out.targets) {
            messages.push_back({id, target, out.request});
            ++at(id).send_count;
        }
        return messages;
    }

    // 投递一批请求，收集所有回复（跳过宕机节点与发往宕机节点的消息）。
    std::vector<ReplyMessage> deliver(const std::vector<VoteMessage>& messages) {
        std::vector<ReplyMessage> replies;
        for (const auto& message : messages) {
            if (!at(message.from).up || !at(message.to).up) continue;
            raft::VoteOutPut out;
            std::string error;
            CHECK(at(message.to).core.handle_request_vote(
                message.from, message.request, out, error));
            CHECK(error.empty());
            at(message.to).persist(out.hard_state);
            replies.push_back({message.to, message.from, out.response});
        }
        return replies;
    }

    // 投递回复。
    void deliver(const std::vector<ReplyMessage>& replies) {
        for (const auto& reply : replies) {
            if (!at(reply.from).up || !at(reply.to).up) continue;
            raft::VoteCountOutPut out;
            std::string error;
            CHECK(at(reply.to).core.handle_vote_response(
                reply.from, reply.response, out, error));
            CHECK(error.empty());
            at(reply.to).persist(out.hard_state);
        }
    }

    std::vector<std::uint64_t> leaders() const {
        std::vector<std::uint64_t> ids;
        for (std::size_t i = 0; i < nodes.size(); ++i) {
            if (nodes[i]->up && nodes[i]->core.role() == raft::Role::Leader) {
                ids.push_back(i + 1);
            }
        }
        return ids;
    }

    std::uint64_t term_of(std::uint64_t id) {
        return at(id).core.hard_state().current_term;
    }
};

void test_three_node_election() {
    Cluster c(4);
    const auto messages = c.start(1);
    CHECK(messages.size() == 2);
    CHECK(messages[0].to == 2 && messages[1].to == 3);
    CHECK(messages[0].request.term == 5 && messages[0].request.candidate_id == 1);
    CHECK(c.at(1).core.role() == raft::Role::Candidate);
    CHECK(c.leaders().empty());

    const auto replies = c.deliver(messages);
    CHECK(replies.size() == 2);
    for (const auto& reply : replies) CHECK(reply.response.vote_granted);

    c.deliver(replies);
    CHECK(c.at(1).core.role() == raft::Role::Leader);
    CHECK(c.leaders().size() == 1 && c.leaders()[0] == 1);
    CHECK(c.term_of(1) == 5 && c.term_of(2) == 5 && c.term_of(3) == 5);
    CHECK(c.at(2).core.role() == raft::Role::Follower);
    CHECK(c.at(3).core.role() == raft::Role::Follower);
    CHECK(c.at(1).saved.voted_for == 1);
    CHECK(c.at(2).saved.voted_for == 1 && c.at(3).saved.voted_for == 1);
    std::cout << "PASS 3-node election: candidate 1 wins with 3/3 votes, one leader, term 5\n";
}

void test_one_reply_lost() {
    Cluster c(4);
    const auto messages = c.start(1);
    auto replies = c.deliver(messages);
    CHECK(replies.size() == 2);
    replies.pop_back();              // 丢掉一张回复，相当于丢包
    c.deliver(replies);
    CHECK(c.at(1).core.role() == raft::Role::Leader);
    CHECK(c.leaders().size() == 1);
    std::cout << "PASS 3-node election: self vote + one reply is already a majority\n";
}

void test_split_vote_then_next_term() {
    // 三节点要真正分票，必须三个节点都在同一任期竞选：
    // 每个节点把唯一一票投给自己，谁都不给谁，于是谁都拿不到多数。
    Cluster c(4);
    auto first = c.start(1);
    auto second = c.start(2);
    auto third_in_round = c.start(3);
    CHECK(c.term_of(1) == 5 && c.term_of(2) == 5 && c.term_of(3) == 5);

    std::vector<VoteMessage> all = first;
    all.insert(all.end(), second.begin(), second.end());
    all.insert(all.end(), third_in_round.begin(), third_in_round.end());
    const auto replies = c.deliver(all);
    c.deliver(replies);

    for (const auto& reply : replies) CHECK(!reply.response.vote_granted);
    CHECK(c.at(1).core.role() == raft::Role::Candidate);
    CHECK(c.at(2).core.role() == raft::Role::Candidate);
    CHECK(c.at(3).core.role() == raft::Role::Candidate);
    CHECK(c.leaders().empty());
    std::cout << "PASS split vote: all three candidates in term 5, no leader\n";

    // 节点 3 在任期 6 超时竞选，此时能拿到票。
    const auto third = c.start(3);
    CHECK(c.at(3).core.hard_state().current_term == 6);
    const auto replies6 = c.deliver(third);
    c.deliver(replies6);
    CHECK(c.leaders().size() == 1 && c.leaders()[0] == 3);
    CHECK(c.term_of(1) == 6 && c.term_of(2) == 6 && c.term_of(3) == 6);
    std::cout << "PASS next term: node 3 wins term 6, others follow\n";
}

void test_leader_steps_down_on_higher_term() {
    Cluster c(4);
    const auto messages = c.start(1);
    c.deliver(c.deliver(messages));
    CHECK(c.at(1).core.role() == raft::Role::Leader);

    // 节点 2 在任期 6 竞选（比 leader 的任期 5 高），向 leader 拉票。
    const auto challenge = c.start(2);
    CHECK(c.at(2).core.hard_state().current_term == 6);
    const auto replies = c.deliver(challenge);

    CHECK(c.at(1).core.role() == raft::Role::Follower);
    CHECK(c.term_of(1) == 6);
    CHECK(c.at(1).saved.current_term == 6);
    CHECK(c.leaders().empty() || (c.leaders().size() == 1 && c.leaders()[0] != 1));

    bool granted_to_2 = false;
    for (const auto& reply : replies) {
        if (reply.from == 1) {
            CHECK(reply.response.term == 6);
            granted_to_2 = reply.response.vote_granted;
        }
    }
    c.deliver(replies);
    CHECK(granted_to_2);
    CHECK(c.at(2).core.role() == raft::Role::Leader);
    std::cout << "PASS leader steps down on higher term and grants to the challenger\n";
}

void test_duplicate_reply_not_counted_twice() {
    // 五节点：多数票为 3，自票 + 一张回复还不够，便于观察重复票是否重复计。
    Cluster c(4, 5);
    const auto messages = c.start(1);
    auto replies = c.deliver(messages);
    CHECK(replies.size() == 4);

    // 节点 2 的回复投两次：只应算一票（自票 1 + 节点2 = 2 < 3）。
    const std::vector<ReplyMessage> repeated{replies[0], replies[0]};
    c.deliver(repeated);
    CHECK(c.at(1).core.role() == raft::Role::Candidate);
    CHECK(c.leaders().empty());

    // 再投递节点 3 的回复：三票，达到多数。
    c.deliver(std::vector<ReplyMessage>{replies[1]});
    CHECK(c.at(1).core.role() == raft::Role::Leader);
    std::cout << "PASS duplicate reply counted once; distinct third vote elects\n";
}

void test_node_down_cannot_elect() {
    Cluster c(4);
    c.at(3).up = false;    // 节点 3 宕机
    const auto messages = c.start(1);
    const auto replies = c.deliver(messages);   // 只有节点 2 会回
    CHECK(replies.size() == 1);
    c.deliver(replies);
    CHECK(c.at(1).core.role() == raft::Role::Leader);
    std::cout << "PASS 2 of 3 alive still elects a leader\n";

    Cluster c2(4);
    c2.at(2).up = false;
    c2.at(3).up = false;
    const auto only = c2.start(1);
    const auto no_reply = c2.deliver(only);
    CHECK(no_reply.empty());
    c2.deliver(no_reply);
    CHECK(c2.at(1).core.role() == raft::Role::Candidate);
    CHECK(c2.leaders().empty());
    std::cout << "PASS alone node stays candidate (no majority)\n";
}

} // namespace

int main() {
    test_three_node_election();
    test_one_reply_lost();
    test_split_vote_then_next_term();
    test_leader_steps_down_on_higher_term();
    test_duplicate_reply_not_counted_twice();
    test_node_down_cannot_elect();
    std::cout << "in-process 3-node election PASS (no sockets, no real timers)\n";
}
