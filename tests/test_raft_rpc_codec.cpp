#include "raft/protocol/rpc_codec.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <limits>
#include <iostream>
#include <string>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

using raft::RpcType;

// 测试自己认定长度头是 4 字节，不引用生产常量：
// 否则把常量写错时，测试会跟着一起错，等于没测。
constexpr std::size_t kLengthField = 4;
constexpr std::size_t kIdField = 8;
constexpr std::size_t kPayloadLimit = 17 * 1024 * 1024;

// 测试自己按大端拆 4 字节，不去调生产代码的读函数：
// 否则"用同一个函数验证它自己"，写错了也会一起错。
std::uint32_t be32_at(const std::string& s,std::size_t at)
{
    CHECK(s.size() >= at + 4);
    return (static_cast<std::uint32_t>(static_cast<unsigned char>(s[at])) << 24) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(s[at+1])) << 16) |
           (static_cast<std::uint32_t>(static_cast<unsigned char>(s[at+2])) << 8) |
            static_cast<std::uint32_t>(static_cast<unsigned char>(s[at+3]));
}

// 手写一个只有长度头的半成品，用来测"对端声称很长的帧"
std::string be32_only(std::uint32_t value)
{
    std::string s(4,'\0');
    s[0] = static_cast<char>((value >> 24) & 0xff);
    s[1] = static_cast<char>((value >> 16) & 0xff);
    s[2] = static_cast<char>((value >> 8) & 0xff);
    s[3] = static_cast<char>(value & 0xff);
    return s;
}

std::uint8_t type_byte(const std::string& frame)
{
    CHECK(frame.size() > kLengthField);
    return static_cast<std::uint8_t>(frame[kLengthField]);
}

const RpcType ALL_TYPES[] = {RpcType::VoteRequest,RpcType::VoteResponse,
                             RpcType::AppendEntries,RpcType::AppendResponse};

void test_roundtrip_all_types()
{
    for(const auto type : ALL_TYPES)
    {
        const std::string body = "body-for-a-rpc";

        std::string frame, error = "stale";
        CHECK(raft::encode_frame(type,body,frame,error));
        CHECK(error.empty());

        // 帧 = 4B 长度 + 1B 类型 + 正文，长度字段把类型也算进去
        CHECK(frame.size() == raft::RPC_HEADER_SIZE + body.size());
        CHECK(be32_at(frame,0) == raft::RPC_TYPE_SIZE + body.size());
        CHECK(type_byte(frame) == static_cast<std::uint8_t>(type));

        RpcType decoded_type = RpcType::VoteRequest;
        std::string decoded_body = "stale";

        CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
        CHECK(error.empty());
        CHECK(decoded_type == type);
        CHECK(decoded_body == body);
    }
    std::cout << "PASS four RPC types round-trip; length field counts the type byte\n";
}

void test_empty_and_binary_body()
{
    for(const auto type : ALL_TYPES)
    {
        std::string frame, error;

        // 空正文也要能发：正文长度正好是 1（只有类型）
        CHECK(raft::encode_frame(type,std::string(),frame,error));
        CHECK(frame.size() == raft::RPC_HEADER_SIZE);
        CHECK(be32_at(frame,0) == 1);

        RpcType decoded_type = RpcType::AppendResponse;
        std::string decoded_body = "not-empty";

        CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
        CHECK(decoded_type == type);
        CHECK(decoded_body.empty());
    }

    // 命令是二进制：NUL、0xFF、换行都不能被当成结束符
    const std::string body("a\0b\xff\n\r\tc",9);
    std::string frame, error;
    CHECK(raft::encode_frame(RpcType::AppendEntries,body,frame,error));
    CHECK(frame.size() == raft::RPC_HEADER_SIZE + body.size());

    RpcType decoded_type = RpcType::VoteRequest;
    std::string decoded_body;
    CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
    CHECK(decoded_type == RpcType::AppendEntries);
    CHECK(decoded_body.size() == body.size());
    CHECK(decoded_body == body);

    std::cout << "PASS empty body and binary body survive the round trip\n";
}

void test_payload_limit()
{
    // 正文长度 = 1 + body，所以 body 到 17MiB-1 时正文正好是上限，还能发
    const std::string at_limit(raft::MAX_RPC_PAYLOAD - raft::RPC_TYPE_SIZE,'x');
    std::string frame, error;

    CHECK(raft::encode_frame(RpcType::AppendEntries,at_limit,frame,error));
    CHECK(be32_at(frame,0) == raft::MAX_RPC_PAYLOAD);
    CHECK(frame.size() == kLengthField + raft::MAX_RPC_PAYLOAD);

    RpcType decoded_type = RpcType::VoteRequest;
    std::string decoded_body;
    CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
    CHECK(decoded_type == RpcType::AppendEntries);
    CHECK(decoded_body.size() == at_limit.size());

    // 再多 1 字节就超限：拒绝，并且不覆盖调用方原来的 out
    const std::string one_more(raft::MAX_RPC_PAYLOAD,'x');
    std::string untouched = "keep-me";

    CHECK(!raft::encode_frame(RpcType::AppendEntries,one_more,untouched,error));
    CHECK(!error.empty());
    CHECK(untouched == "keep-me");

    std::cout << "PASS 17MiB payload accepted; one byte more rejected without touching out\n";
}

void test_peek_frame_size()
{
    std::size_t frame_size = 0;
    std::string error = "stale";

    // 不足 4 字节：连长度头都没收全
    for(std::size_t len = 0; len < kLengthField; ++len)
    {
        const std::string partial(len,'\0');
        CHECK(!raft::peek_frame_size(partial.data(),partial.size(),frame_size,error));
        CHECK(!error.empty());
    }

    // 声明的正文长度为 0：连类型字节都没有，取类型就会越界
    const std::string zero = be32_only(0);
    CHECK(!raft::peek_frame_size(zero.data(),zero.size(),frame_size,error));

    // 正常：正文 1 字节 → 整帧 5 字节
    const std::string smallest = be32_only(1);
    CHECK(raft::peek_frame_size(smallest.data(),smallest.size(),frame_size,error));
    CHECK(frame_size == raft::RPC_HEADER_SIZE);

    // 上限恰好通过
    const std::string at_limit = be32_only(raft::MAX_RPC_PAYLOAD);
    CHECK(raft::peek_frame_size(at_limit.data(),at_limit.size(),frame_size,error));
    CHECK(frame_size == kLengthField + raft::MAX_RPC_PAYLOAD);

    // 只凭 4 字节声称 4GiB：必须在分配之前就拒绝
    const std::string huge = be32_only(0xffffffffu);
    CHECK(!raft::peek_frame_size(huge.data(),huge.size(),frame_size,error));
    CHECK(!error.empty());

    // 超过上限 1 字节也拒绝
    const std::string one_over = be32_only(static_cast<std::uint32_t>(raft::MAX_RPC_PAYLOAD + 1));
    CHECK(!raft::peek_frame_size(one_over.data(),one_over.size(),frame_size,error));

    // 只读 4 字节就能算出整帧长度，多给的字节不影响判断
    std::string frame;
    CHECK(raft::encode_frame(RpcType::VoteResponse,std::string(20,'v'),frame,error));
    const std::string frame_plus_extra = frame + "trailing";
    CHECK(raft::peek_frame_size(frame_plus_extra.data(),frame_plus_extra.size(),frame_size,error));
    CHECK(frame_size == frame.size());

    std::cout << "PASS peek_frame_size rejects short/zero/oversized headers before allocating\n";
}

void test_length_mismatch()
{
    std::string frame, error;
    CHECK(raft::encode_frame(RpcType::AppendEntries,std::string(6,'c'),frame,error));

    RpcType decoded_type = RpcType::VoteRequest;
    std::string decoded_body = "keep-me";

    // 少给 1 字节：还没收全
    CHECK(!raft::decode_frame(frame.data(),frame.size()-1,decoded_type,decoded_body,error));
    CHECK(!error.empty());
    CHECK(decoded_type == RpcType::VoteRequest && decoded_body == "keep-me");

    // 多给 1 字节：这已经不是"一帧"了
    const std::string one_extra = frame + "x";
    CHECK(!raft::decode_frame(one_extra.data(),one_extra.size(),decoded_type,decoded_body,error));
    CHECK(decoded_type == RpcType::VoteRequest && decoded_body == "keep-me");

    // 长度头声明 20，实际只给 10
    const std::string short_body = be32_only(20) + "0123456789";
    CHECK(!raft::decode_frame(short_body.data(),short_body.size(),decoded_type,decoded_body,error));
    CHECK(decoded_body == "keep-me");

    std::cout << "PASS truncated/oversized/two-frame inputs are refused without touching outputs\n";
}

void test_unknown_type()
{
    std::string error;

    // 1..5 是已知类型（5 = Hello）；0、6、255 都不是
    for(const std::uint8_t raw : {static_cast<std::uint8_t>(0),
                                  static_cast<std::uint8_t>(6),
                                  static_cast<std::uint8_t>(255)})
    {
        std::string frame = be32_only(1);
        frame.push_back(static_cast<char>(raw));

        RpcType decoded_type = RpcType::VoteRequest;
        std::string decoded_body = "keep-me";

        CHECK(!raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
        CHECK(!error.empty());
        CHECK(decoded_type == RpcType::VoteRequest);
        CHECK(decoded_body == "keep-me");
    }

    std::cout << "PASS unknown type bytes are refused and leave type/body unchanged\n";
}

void test_sticky_frames_must_be_sliced()
{
    std::string first, second, error;

    CHECK(raft::encode_frame(RpcType::VoteRequest,std::string(12,'1'),first,error));
    CHECK(raft::encode_frame(RpcType::AppendResponse,std::string(3,'2'),second,error));

    const std::string both = first + second;

    RpcType decoded_type = RpcType::VoteRequest;
    std::string decoded_body;

    // 一次喂两帧：必须拒绝，交给调用方先按长度切开
    CHECK(!raft::decode_frame(both.data(),both.size(),decoded_type,decoded_body,error));

    // 按第一帧的长度切出来，第一帧照常解析
    CHECK(raft::decode_frame(both.data(),first.size(),decoded_type,decoded_body,error));
    CHECK(decoded_type == RpcType::VoteRequest);
    CHECK(decoded_body == std::string(12,'1'));

    // 剩下的字节正好是第二帧
    CHECK(raft::decode_frame(both.data()+first.size(),both.size()-first.size(),
                             decoded_type,decoded_body,error));
    CHECK(decoded_type == RpcType::AppendResponse);
    CHECK(decoded_body == std::string(3,'2'));

    std::cout << "PASS concatenated frames are split by the caller, then decoded one by one\n";
}

void test_hello_frame()
{
    // Hello 的正文就是 8 字节的节点 ID，整帧 4 + 1 + 8 = 13 字节
    const std::string body = be32_only(0) + std::string(4,'\0');   // 8 字节占位
    CHECK(body.size() == 8);

    std::string frame,error = "stale";
    CHECK(raft::encode_frame(RpcType::Hello,body,frame,error));
    CHECK(error.empty());
    CHECK(frame.size() == 13);
    CHECK(be32_at(frame,0) == 9);
    CHECK(type_byte(frame) == static_cast<std::uint8_t>(RpcType::Hello));

    RpcType decoded_type = RpcType::VoteRequest;
    std::string decoded_body;

    CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
    CHECK(decoded_type == RpcType::Hello);
    CHECK(decoded_body == body);

    std::cout << "PASS hello frame: type 5 accepted, 13 bytes, body preserved\n";
}

// 测试自己生成 8 字节大端编号，用来构造独立于生产编码函数的输入。
std::string id_bytes(std::uint64_t id)
{
    std::string bytes(kIdField,'\0');
    for(std::size_t i = 0; i < kIdField; ++i)
    {
        bytes[kIdField - 1 - i] = static_cast<char>((id >> (8 * i)) & 0xffu);
    }
    return bytes;
}

// 验证完整业务帧的布局，再按实际接收顺序还原编号和正文。
std::string rpc_roundtrip_body(RpcType type,std::uint64_t id,const std::string& body)
{
    std::string frame,error = "stale";
    CHECK(raft::encode_rpc_frame(type,id,body,frame,error));
    CHECK(error.empty());
    CHECK(frame.size() == kLengthField + 1 + kIdField + body.size());
    CHECK(be32_at(frame,0) == 1 + kIdField + body.size());
    CHECK(type_byte(frame) == static_cast<std::uint8_t>(type));
    CHECK(frame.compare(kLengthField + 1,kIdField,id_bytes(id)) == 0);

    RpcType decoded_type = RpcType::Hello;
    std::string wire_body;
    error = "stale";
    CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,wire_body,error));
    CHECK(error.empty());
    CHECK(decoded_type == type);

    std::uint64_t decoded_id = 0;
    std::string decoded_body = "stale";
    error = "stale";
    CHECK(raft::decode_rpc_body(wire_body,decoded_id,decoded_body,error));
    CHECK(error.empty());
    CHECK(decoded_id == id);
    CHECK(decoded_body == body);
    return decoded_body;
}

// 混合字节编号和手写黄金帧，分别检查编码与解码的 64 位宽度、偏移和字节序。
void test_rpc_id_golden_bytes()
{
    const std::uint64_t id = UINT64_C(0x0102030405060708);
    const std::string literal_id("\x01\x02\x03\x04\x05\x06\x07\x08",8);
    const std::string body("\0\xffX\n",4);
    std::string expected("\0\0\0\x0d\x04",5);
    expected += literal_id;
    expected += body;

    std::string frame,error;
    CHECK(raft::encode_rpc_frame(RpcType::AppendResponse,id,body,frame,error));
    CHECK(frame == expected);

    std::uint64_t decoded_id = 77;
    std::string decoded_body;
    CHECK(raft::decode_rpc_body(literal_id + body,decoded_id,decoded_body,error));
    CHECK(decoded_id == id);
    CHECK(decoded_body == body);
    std::cout << "PASS rpc id golden bytes: 8-byte big-endian id, exact offsets and length\n";
}

// 四类业务帧遍历编号边界、空正文和二进制正文，并检查原地编码/解码。
void test_rpc_id_roundtrip()
{
    const std::uint64_t ids[] = {
        1,UINT64_C(0x100000001),UINT64_C(0x8000000000000000),
        std::numeric_limits<std::uint64_t>::max()
    };
    const std::string bodies[] = {std::string(),std::string("a\0b\xff\n",5)};
    for(const auto type : ALL_TYPES)
    {
        for(const auto id : ids)
        {
            for(const auto& body : bodies)
            {
                CHECK(rpc_roundtrip_body(type,id,body) == body);
            }
        }
    }

    const std::string original("x\0y",3);
    std::string data = original,error;
    CHECK(raft::encode_rpc_frame(RpcType::VoteRequest,9,data,data,error));
    RpcType type = RpcType::Hello;
    CHECK(raft::decode_frame(data.data(),data.size(),type,data,error));
    std::uint64_t id = 0;
    CHECK(raft::decode_rpc_body(data,id,data,error));
    CHECK(id == 9 && data == original);
    std::cout << "PASS rpc id round-trip: four types, u64 boundaries, binary/empty and in-place outputs\n";
}

// 编号为零、Hello 和未知业务类型都不能交给业务帧编码；失败保留 out。
void test_rpc_id_invalid_encode()
{
    const RpcType invalid_types[] = {
        RpcType::Hello,static_cast<RpcType>(0),
        static_cast<RpcType>(6),static_cast<RpcType>(255)
    };
    for(const auto type : invalid_types)
    {
        std::string out = "keep",error = "stale";
        CHECK(!raft::encode_rpc_frame(type,1,"body",out,error));
        CHECK(!error.empty() && error != "stale");
        CHECK(out == "keep");
    }
    for(const auto type : ALL_TYPES)
    {
        std::string out = "keep",error = "stale";
        CHECK(!raft::encode_rpc_frame(type,0,"body",out,error));
        CHECK(!error.empty() && error != "stale");
        CHECK(out == "keep");
    }
    std::cout << "PASS rpc id encode refusal: zero id, Hello and unknown types preserve out\n";
}

// 0..7 字节不够一个编号；全零编号也无效，两项出参均应保留。
void test_rpc_id_invalid_decode()
{
    for(std::size_t len = 0; len < kIdField; ++len)
    {
        std::uint64_t id = 99;
        std::string body = "keep",error = "stale";
        CHECK(!raft::decode_rpc_body(std::string(len,'\xa5'),id,body,error));
        CHECK(!error.empty() && error != "stale");
        CHECK(id == 99 && body == "keep");
    }
    std::uint64_t id = 99;
    std::string body = "keep",error = "stale";
    CHECK(!raft::decode_rpc_body(id_bytes(0) + "data",id,body,error));
    CHECK(!error.empty() && error != "stale");
    CHECK(id == 99 && body == "keep");
    std::cout << "PASS rpc id decode refusal: every short id and zero id preserve both outputs\n";
}

// 17 MiB payload 要同时容纳类型、编号和正文；分别测编码和独立解码的上界。
void test_rpc_id_payload_limit()
{
    CHECK(raft::RPC_ID_SIZE == kIdField);
    CHECK(raft::MAX_RPC_PAYLOAD == kPayloadLimit);
    {
        const std::string body(kPayloadLimit - 1 - kIdField,'x');
        CHECK(rpc_roundtrip_body(RpcType::AppendEntries,
              std::numeric_limits<std::uint64_t>::max(),body) == body);
    }
    {
        const std::string too_large(kPayloadLimit - kIdField,'x');
        std::string out = "keep",error;
        CHECK(!raft::encode_rpc_frame(RpcType::AppendEntries,1,too_large,out,error));
        CHECK(!error.empty() && out == "keep");
    }
    {
        std::string wire_body(kPayloadLimit,'x');
        wire_body.replace(0,kIdField,id_bytes(1));
        std::uint64_t id = 99;
        std::string body = "keep",error;
        CHECK(!raft::decode_rpc_body(wire_body,id,body,error));
        CHECK(!error.empty());
        CHECK(id == 99 && body == "keep");
    }
    std::cout << "PASS rpc id payload budget: exact 17MiB accepted, one byte over refused\n";
}

// 编号拆除后，四类既有正文解码函数仍应得到原字段，不能把编号误当 term。
void test_rpc_id_existing_body_codecs()
{
    std::string body,error;
    raft::RequestVoteRequest vote;
    vote.term = 3;
    vote.candidate_id = 1;
    vote.last_log_index = 7;
    vote.last_log_term = 2;
    CHECK(raft::encode_vote_request(vote,body,error));
    CHECK(body.size() == 32);
    raft::RequestVoteRequest decoded_vote;
    CHECK(raft::decode_vote_request(rpc_roundtrip_body(RpcType::VoteRequest,42,body),
                                   decoded_vote,error));
    CHECK(decoded_vote.term == 3 && decoded_vote.candidate_id == 1);
    CHECK(decoded_vote.last_log_index == 7 && decoded_vote.last_log_term == 2);

    raft::RequestVoteResponse vote_reply;
    vote_reply.term = 3;
    vote_reply.vote_granted = true;
    CHECK(raft::encode_vote_response(vote_reply,body,error));
    CHECK(body.size() == 9);
    raft::RequestVoteResponse decoded_vote_reply;
    CHECK(raft::decode_vote_response(rpc_roundtrip_body(RpcType::VoteResponse,42,body),
                                    decoded_vote_reply,error));
    CHECK(decoded_vote_reply.term == 3 && decoded_vote_reply.vote_granted);

    raft::AppendEntriesRequest append;
    append.term = 3;
    append.leader_id = 1;
    append.prev_log_index = 7;
    append.prev_log_term = 2;
    append.leader_commit = 6;
    raft::LogEntry entry;
    entry.index = 8;
    entry.term = 3;
    entry.command = std::string("k\0v",3);
    append.entries.push_back(entry);
    CHECK(raft::encode_append_entries(append,body,error));
    CHECK(body.size() == 44 + 12 + 3);
    raft::AppendEntriesRequest decoded_append;
    CHECK(raft::decode_append_entries(rpc_roundtrip_body(RpcType::AppendEntries,43,body),
                                     decoded_append,error));
    CHECK(decoded_append.term == 3 && decoded_append.leader_id == 1);
    CHECK(decoded_append.prev_log_index == 7 && decoded_append.prev_log_term == 2);
    CHECK(decoded_append.leader_commit == 6 && decoded_append.entries.size() == 1);
    CHECK(decoded_append.entries[0].index == 0);
    CHECK(decoded_append.entries[0].term == 3);
    CHECK(decoded_append.entries[0].command == entry.command);

    raft::AppendEntriesResponse append_reply;
    append_reply.term = 3;
    append_reply.success = true;
    CHECK(raft::encode_append_response(append_reply,body,error));
    CHECK(body.size() == 9);
    raft::AppendEntriesResponse decoded_append_reply;
    CHECK(raft::decode_append_response(rpc_roundtrip_body(RpcType::AppendResponse,43,body),
                                      decoded_append_reply,error));
    CHECK(decoded_append_reply.term == 3 && decoded_append_reply.success);
    std::cout << "PASS rpc id integration: all four original body codecs keep their layouts\n";
}

// Hello 与两个带不同编号的相同回复粘在一起，调用方按长度切帧并分开处理。
void test_rpc_id_sticky_with_hello()
{
    std::string hello_body,hello,reply_body,first,second,error;
    CHECK(raft::encode_hello(1,hello_body,error));
    CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello,error));
    CHECK(hello.size() == 13);
    raft::AppendEntriesResponse reply;
    reply.term = 3;
    reply.success = true;
    CHECK(raft::encode_append_response(reply,reply_body,error));
    CHECK(raft::encode_rpc_frame(RpcType::AppendResponse,42,reply_body,first,error));
    CHECK(raft::encode_rpc_frame(RpcType::AppendResponse,43,reply_body,second,error));
    const std::string all = hello + first + second;

    std::size_t at = 0;
    for(std::size_t n = 0; n < 3; ++n)
    {
        std::size_t size = 0;
        CHECK(raft::peek_frame_size(all.data() + at,all.size() - at,size,error));
        CHECK(size <= all.size() - at);
        RpcType type = RpcType::Hello;
        std::string wire_body;
        CHECK(raft::decode_frame(all.data() + at,size,type,wire_body,error));
        if(n == 0)
        {
            std::uint64_t peer = 0;
            CHECK(type == RpcType::Hello && size == 13);
            CHECK(raft::decode_hello(wire_body,peer,error));
            CHECK(peer == 1);
        }
        else
        {
            std::uint64_t id = 0;
            std::string decoded_body;
            CHECK(type == RpcType::AppendResponse);
            CHECK(raft::decode_rpc_body(wire_body,id,decoded_body,error));
            CHECK(id == (n == 1 ? 42u : 43u));
            CHECK(decoded_body == reply_body);
        }
        at += size;
    }
    CHECK(at == all.size());
    std::cout << "PASS rpc id stream slicing: unchanged Hello, identical replies keep ids 42 and 43\n";
}

void test_error_is_reset_on_success()
{
    std::string frame, error = "previous failure";
    CHECK(raft::encode_frame(RpcType::VoteRequest,std::string(4,'a'),frame,error));
    CHECK(error.empty());

    error = "previous failure";
    RpcType decoded_type = RpcType::VoteRequest;
    std::string decoded_body;
    CHECK(raft::decode_frame(frame.data(),frame.size(),decoded_type,decoded_body,error));
    CHECK(error.empty());

    error = "previous failure";
    std::size_t frame_size = 0;
    CHECK(raft::peek_frame_size(frame.data(),frame.size(),frame_size,error));
    CHECK(error.empty());

    std::cout << "PASS a successful call clears an error left by an earlier failure\n";
}

} // namespace

int main()
{
    test_roundtrip_all_types();
    test_empty_and_binary_body();
    test_payload_limit();
    test_peek_frame_size();
    test_length_mismatch();
    test_unknown_type();
    test_hello_frame();
    test_sticky_frames_must_be_sliced();
    test_error_is_reset_on_success();
    test_rpc_id_golden_bytes();
    test_rpc_id_roundtrip();
    test_rpc_id_invalid_encode();
    test_rpc_id_invalid_decode();
    test_rpc_id_payload_limit();
    test_rpc_id_existing_body_codecs();
    test_rpc_id_sticky_with_hello();

    std::cout << "raft rpc frame codec PASS (pure functions; no sockets)\n";
    return 0;
}
