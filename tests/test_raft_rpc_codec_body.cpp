// M6 第2步：四类正文编解码的测试
//
// 大端拆解全部手写在测试里，不调生产代码：
// 否则"用被测函数验证被测函数"，一起错也看不出来。

#include "raft/rpc_codec.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

using raft::LogEntry;
using raft::RequestVoteRequest;
using raft::RequestVoteResponse;
using raft::AppendEntriesRequest;
using raft::AppendEntriesResponse;

constexpr std::size_t kVoteRequestSize = 32;
constexpr std::size_t kFixedResponseSize = 9;

// ---------- 测试自己的大端工具 ----------

std::string be32(std::uint32_t v)
{
    std::string s(4,'\0');
    s[0] = static_cast<char>((v >> 24) & 0xff);
    s[1] = static_cast<char>((v >> 16) & 0xff);
    s[2] = static_cast<char>((v >> 8) & 0xff);
    s[3] = static_cast<char>(v & 0xff);
    return s;
}

std::string be64(std::uint64_t v)
{
    std::string s(8,'\0');
    for(int i = 7 ; i >= 0 ; i--)
    {
        s[i] = static_cast<char>(v & 0xff);
        v >>= 8;
    }
    return s;
}

std::uint64_t read_be64_at(const std::string& s,std::size_t at)
{
    CHECK(s.size() >= at + 8);
    std::uint64_t v = 0;
    for(std::size_t i = 0 ; i < 8 ; i++)
    {
        v = (v << 8) | static_cast<unsigned char>(s[at+i]);
    }
    return v;
}

std::uint32_t read_be32_at(const std::string& s,std::size_t at)
{
    CHECK(s.size() >= at + 4);
    std::uint32_t v = 0;
    for(std::size_t i = 0 ; i < 4 ; i++)
    {
        v = (v << 8) | static_cast<unsigned char>(s[at+i]);
    }
    return v;
}

// ---------- 1. 投票请求 ----------

void test_vote_request_roundtrip()
{
    const std::uint64_t values[] = {0,1,42,0xffffffffffffffffull};

    for(const auto v : values)
    {
        RequestVoteRequest in;
        in.term = v;
        in.candidate_id = v;
        in.last_log_index = v;
        in.last_log_term = v;

        std::string body;
        std::string error = "stale";
        CHECK(raft::encode_vote_request(in,body,error));
        CHECK(error.empty());

        // 固定 32 字节，字段顺序 term / candidate_id / last_log_index / last_log_term
        CHECK(body.size() == kVoteRequestSize);
        CHECK(read_be64_at(body,0)  == v);
        CHECK(read_be64_at(body,8)  == v);
        CHECK(read_be64_at(body,16) == v);
        CHECK(read_be64_at(body,24) == v);

        RequestVoteRequest out;
        CHECK(raft::decode_vote_request(body,out,error));
        CHECK(out.term == v && out.candidate_id == v);
        CHECK(out.last_log_index == v && out.last_log_term == v);
    }

    std::cout << "PASS vote request: 32 bytes, four fields, 0 and UINT64_MAX round-trip\n";
}

void test_vote_request_size_strict()
{
    RequestVoteRequest in;
    in.term = 7;
    in.candidate_id = 2;
    in.last_log_index = 130;
    in.last_log_term = 5;

    std::string good,error;
    CHECK(raft::encode_vote_request(in,good,error));

    // 少一个字节、多一个字节都不是合法报文
    for(const std::size_t len : {good.size()-1,good.size()+1})
    {
        std::string probe = good;
        if(len > probe.size()) probe.push_back('\0');

        RequestVoteRequest out;
        out.term = 999;              // 哨兵：失败时不能被改
        out.candidate_id = 888;

        CHECK(!raft::decode_vote_request(probe.substr(0,len),out,error));
        CHECK(!error.empty());
        CHECK(out.term == 999 && out.candidate_id == 888);
    }

    std::cout << "PASS vote request rejects 31/33 bytes and keeps out unchanged\n";
}

// ---------- 2. 投票回复 ----------

void test_vote_response_roundtrip_and_bool()
{
    for(const bool granted : {false,true})
    {
        RequestVoteResponse in;
        in.term = 0x0123456789abcdefull;
        in.vote_granted = granted;

        std::string body,error;
        CHECK(raft::encode_vote_response(in,body,error));
        CHECK(body.size() == kFixedResponseSize);
        CHECK(read_be64_at(body,0) == in.term);
        CHECK(static_cast<unsigned char>(body[8]) == (granted ? 1u : 0u));

        RequestVoteResponse out;
        CHECK(raft::decode_vote_response(body,out,error));
        CHECK(out.term == in.term);
        CHECK(out.vote_granted == granted);
    }

    // 布尔字段只认 0 和 1：2 和 255 都是"第三种真假"，必须拒绝
    for(const std::uint8_t raw : {static_cast<std::uint8_t>(2),static_cast<std::uint8_t>(255)})
    {
        std::string body = be64(5);
        body.push_back(static_cast<char>(raw));

        RequestVoteResponse out;
        out.term = 777;

        std::string error;
        CHECK(!raft::decode_vote_response(body,out,error));
        CHECK(!error.empty());
        CHECK(out.term == 777);
    }

    // 长度 8 / 10 也要拒绝
    const std::string eight = be64(5);
    std::string ten = be64(5);
    ten.push_back('\x01');
    ten.push_back('\0');

    RequestVoteResponse out;
    std::string error;
    CHECK(!raft::decode_vote_response(eight,out,error));
    CHECK(!raft::decode_vote_response(ten,out,error));

    std::cout << "PASS vote response: 9 bytes, bool only 0/1, other sizes and 2/255 refused\n";
}

// ---------- 3. 复制请求：正常形状 ----------

void test_append_entries_shapes()
{
    // 心跳：没有条目，正文只有 44 字节定长头
    {
        AppendEntriesRequest in;
        in.term = 3;
        in.leader_id = 1;
        in.prev_log_index = 10;
        in.prev_log_term = 2;
        in.leader_commit = 9;

        std::string body,error;
        CHECK(raft::encode_append_entries(in,body,error));
        CHECK(body.size() == raft::APPEND_HEADER_SIZE);
        CHECK(read_be64_at(body,0)  == 3);
        CHECK(read_be64_at(body,8)  == 1);
        CHECK(read_be64_at(body,16) == 10);
        CHECK(read_be64_at(body,24) == 2);
        CHECK(read_be32_at(body,32) == 0);       // 条目数在偏移 32
        CHECK(read_be64_at(body,36) == 9);       // leader_commit 在偏移 36

        AppendEntriesRequest out;
        CHECK(raft::decode_append_entries(body,out,error));
        CHECK(out.entries.empty());
        CHECK(out.term == 3 && out.leader_id == 1);
        CHECK(out.prev_log_index == 10 && out.prev_log_term == 2);
        CHECK(out.leader_commit == 9);
    }

    // 一条二进制命令 + 一条空命令
    {
        AppendEntriesRequest in;
        in.term = 4;
        in.leader_id = 2;
        in.prev_log_index = 0;
        in.leader_commit = 0;
        in.entries.push_back(LogEntry{0,4,std::string("a\0b\xff",4)});
        in.entries.push_back(LogEntry{0,4,std::string()});

        std::string body,error;
        CHECK(raft::encode_append_entries(in,body,error));
        // 44 + (12+4) + (12+0)
        CHECK(body.size() == 44 + 16 + 12);

        AppendEntriesRequest out;
        CHECK(raft::decode_append_entries(body,out,error));
        CHECK(out.entries.size() == 2);
        CHECK(out.entries[0].term == 4);
        CHECK(out.entries[0].command == std::string("a\0b\xff",4));
        CHECK(out.entries[1].term == 4 && out.entries[1].command.empty());

        // index 不上线：解码后必须留 0，由 Core 按 prev+1+序号 填
        CHECK(out.entries[0].index == 0 && out.entries[1].index == 0);
    }

    std::cout << "PASS append request: heartbeat 44B, binary and empty commands, index stays 0\n";
}

// ---------- 4. 复制请求：条目数两道闸 ----------

void test_append_entries_count_gates()
{
    // 编码侧：4096 条允许，4097 条拒绝
    {
        AppendEntriesRequest ok;
        ok.term = 1;
        ok.leader_id = 1;
        ok.entries.assign(raft::MAX_RPC_ENTRIES,LogEntry{0,1,std::string()});

        std::string body,error;
        CHECK(raft::encode_append_entries(ok,body,error));
        CHECK(body.size() == 44 + raft::MAX_RPC_ENTRIES * 12);
        CHECK(read_be32_at(body,32) == raft::MAX_RPC_ENTRIES);

        AppendEntriesRequest out;
        CHECK(raft::decode_append_entries(body,out,error));
        CHECK(out.entries.size() == raft::MAX_RPC_ENTRIES);

        AppendEntriesRequest bad = ok;
        bad.entries.push_back(LogEntry{0,1,std::string()});
        std::string untouched = "keep-me";
        CHECK(!raft::encode_append_entries(bad,untouched,error));
        CHECK(!error.empty());
        CHECK(untouched == "keep-me");
    }

    // 解码侧闸一：count 超过协议上限。手造 44 字节头，把 count 写成 4097
    {
        std::string body = be64(1) + be64(1) + be64(0) + be64(0) + be32(raft::MAX_RPC_ENTRIES+1) + be64(0);
        CHECK(body.size() == raft::APPEND_HEADER_SIZE);

        AppendEntriesRequest out;
        out.term = 4242;
        std::string error;
        CHECK(!raft::decode_append_entries(body,out,error));
        CHECK(!error.empty());
        CHECK(out.term == 4242 && out.entries.empty());
    }

    // 解码侧闸二：count 没超上限，但剩下的字节根本放不下这么多条
    {
        std::string body = be64(1) + be64(1) + be64(0) + be64(0) + be32(10) + be64(0);
        body += std::string(12,'\0');       // 只够 1 条，却声明 10 条

        AppendEntriesRequest out;
        std::string error;
        CHECK(!raft::decode_append_entries(body,out,error));
        CHECK(!error.empty());
        CHECK(out.entries.empty());
    }

    std::cout << "PASS entry-count gates: 4096 ok, 4097 refused, declared count must fit body\n";
}

// ---------- 5. 复制请求：截断点与多余字节 ----------

void test_append_entries_truncation()
{
    AppendEntriesRequest in;
    in.term = 6;
    in.leader_id = 3;
    in.prev_log_index = 2;
    in.prev_log_term = 1;
    in.leader_commit = 2;
    in.entries.push_back(LogEntry{0,6,std::string("first")});
    in.entries.push_back(LogEntry{0,6,std::string("second")});

    std::string body,error;
    CHECK(raft::encode_append_entries(in,body,error));

    // 从 0 到 size-1 的每个长度都必须失败：任何一处被截断都不该被当成完整报文
    for(std::size_t len = 0 ; len < body.size() ; len++)
    {
        AppendEntriesRequest out;
        out.term = 5555;

        if(raft::decode_append_entries(body.substr(0,len),out,error))
        {
            std::cerr << "  truncated length " << len << " was accepted\n";
            CHECK(false);
        }

        CHECK(out.term == 5555);
    }

    // 尾部多一个字节同样不是"一条完整报文"
    AppendEntriesRequest out;
    CHECK(!raft::decode_append_entries(body + "x",out,error));

    std::cout << "PASS append request: every truncation point refused, trailing byte refused\n";
}

// ---------- 6. 命令长度说谎 ----------

void test_append_entries_command_length_lies()
{
    // 一条条目声明 cmd_len=100，实际只给 4 字节
    std::string body = be64(1) + be64(1) + be64(0) + be64(0) + be32(1) + be64(0);
    body += be64(1);                     // entry term
    body += be32(100);                   // 声称 100 字节
    body += "abcd";                      // 实际 4 字节

    AppendEntriesRequest out;
    std::string error;
    CHECK(!raft::decode_append_entries(body,out,error));
    CHECK(!error.empty());
    CHECK(out.entries.empty());

    std::cout << "PASS append request: command length larger than body is refused\n";
}

// ---------- 7. 复制回复 ----------

void test_append_response()
{
    for(const bool success : {false,true})
    {
        AppendEntriesResponse in;
        in.term = 9;
        in.success = success;

        std::string body,error;
        CHECK(raft::encode_append_response(in,body,error));
        CHECK(body.size() == kFixedResponseSize);

        AppendEntriesResponse out;
        CHECK(raft::decode_append_response(body,out,error));
        CHECK(out.term == 9 && out.success == success);
    }

    for(const std::uint8_t raw : {static_cast<std::uint8_t>(2),static_cast<std::uint8_t>(255)})
    {
        std::string body = be64(9);
        body.push_back(static_cast<char>(raw));

        AppendEntriesResponse out;
        out.term = 111;
        std::string error;
        CHECK(!raft::decode_append_response(body,out,error));
        CHECK(out.term == 111);
    }

    AppendEntriesResponse out;
    std::string error;
    CHECK(!raft::decode_append_response(be64(9),out,error));

    std::cout << "PASS append response: 9 bytes, bool only 0/1, short body refused\n";
}

// ---------- 8. 与帧串起来 ----------

void test_body_through_frame()
{
    // 四类正文都要能塞进第1步的帧，再原样取回来
    {
        RequestVoteRequest in;
        in.term = 5; in.candidate_id = 2; in.last_log_index = 130; in.last_log_term = 5;

        std::string body,frame,error;
        CHECK(raft::encode_vote_request(in,body,error));
        CHECK(raft::encode_frame(raft::RpcType::VoteRequest,body,frame,error));

        raft::RpcType type = raft::RpcType::VoteResponse;
        std::string decoded_body;
        CHECK(raft::decode_frame(frame.data(),frame.size(),type,decoded_body,error));
        CHECK(type == raft::RpcType::VoteRequest);

        RequestVoteRequest out;
        CHECK(raft::decode_vote_request(decoded_body,out,error));
        CHECK(out.term == 5 && out.candidate_id == 2);
        CHECK(out.last_log_index == 130 && out.last_log_term == 5);
    }

    {
        AppendEntriesRequest in;
        in.term = 8; in.leader_id = 1; in.prev_log_index = 40; in.prev_log_term = 7;
        in.leader_commit = 39;
        in.entries.push_back(LogEntry{0,8,std::string(1024,'z')});

        std::string body,frame,error;
        CHECK(raft::encode_append_entries(in,body,error));
        CHECK(raft::encode_frame(raft::RpcType::AppendEntries,body,frame,error));

        raft::RpcType type = raft::RpcType::VoteRequest;
        std::string decoded_body;
        CHECK(raft::decode_frame(frame.data(),frame.size(),type,decoded_body,error));
        CHECK(type == raft::RpcType::AppendEntries);

        AppendEntriesRequest out;
        CHECK(raft::decode_append_entries(decoded_body,out,error));
        CHECK(out.entries.size() == 1);
        CHECK(out.entries[0].command == std::string(1024,'z'));
    }

    std::cout << "PASS body -> frame -> body round trip for vote and append requests\n";
}

} // namespace

int main()
{
    test_vote_request_roundtrip();
    test_vote_request_size_strict();
    test_vote_response_roundtrip_and_bool();
    test_append_entries_shapes();
    test_append_entries_count_gates();
    test_append_entries_truncation();
    test_append_entries_command_length_lies();
    test_append_response();
    test_body_through_frame();

    std::cout << "raft rpc body codec PASS (pure functions; no sockets)\n";
    return 0;
}
