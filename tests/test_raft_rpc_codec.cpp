#include "raft/rpc_codec.h"

#include <cstddef>
#include <cstdint>
#include <cstdlib>
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

    std::cout << "raft rpc frame codec PASS (pure functions; no sockets)\n";
    return 0;
}
