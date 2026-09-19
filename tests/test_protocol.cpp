// test_protocol.cpp —— 请求/响应协议编解码的单元测试（M6a）
//
// 【协议格式】
//   请求 payload：[1B op][4B key_len][4B val_len][key][value]      固定头 9B
//                 op = 1 SET / 2 GET / 3 DEL；GET、DEL 的 val_len 必须是 0
//   响应 payload：[1B status][4B val_len][value]                   固定头 5B
//                 status = 0 OK / 1 NOT_FOUND / 2 ERROR
//   所有多字节字段都是【大端】，与存储记录保持同一种风格
//   （注意：协议格式和存储记录格式是两套东西 —— 协议不需要 CRC，两者各自演化）
//
// 【当前预期】
//   protocol.h / protocol.cpp 还没写 → 本测试编译/链接失败（红）
//   实现后应当全绿
#include "net/protocol.h"
#include <cstdint>
#include <iostream>
#include <string>

static int g_failed = 0;

#define CHECK(cond)                                                             \
    do {                                                                        \
        if (cond)                                                               \
            std::cout << "  ok   : " #cond << std::endl;                        \
        else {                                                                  \
            std::cout << "  FAIL : " #cond "   (line " << __LINE__ << ")"       \
                      << std::endl;                                              \
            g_failed++;                                                         \
        }                                                                       \
    } while (0)

static std::string hex(const std::string &s)
{
    static const char *digits = "0123456789abcdef";
    std::string out;

    for(unsigned char c : s)
    {
        out += digits[c >> 4];
        out += digits[c & 0x0F];
        out += ' ';
    }

    if(!out.empty())
    {
        out.pop_back();
    }

    return out;
}

int main()
{
    // -------------------------------------------------------------------------
    std::cout << "[1] SET 请求编码（大端 + 固定头 9B）" << std::endl;
    {
        kv::Request req;
        req.op = kv::Op::Set;
        req.key = "a";
        req.value = "1";

        std::string bytes = kv::encode_request(req);
        CHECK(bytes.size() == 11);   //9 + 1 + 1
        CHECK(hex(bytes) == "01 00 00 00 01 00 00 00 01 61 31");
    }

    std::cout << "[2] GET / DEL 请求编码（val_len 必须为 0）" << std::endl;
    {
        kv::Request req;
        req.op = kv::Op::Get;
        req.key = "abc";

        std::string bytes = kv::encode_request(req);
        CHECK(bytes.size() == 12);   //9 + 3
        CHECK(hex(bytes) == "02 00 00 00 03 00 00 00 00 61 62 63");

        req.op = kv::Op::Del;
        req.key = "k";
        bytes = kv::encode_request(req);
        CHECK(bytes.size() == 10);   //9 + 1
        CHECK(hex(bytes) == "03 00 00 00 01 00 00 00 00 6b");
    }

    std::cout << "[3] 请求往返（encode → parse）" << std::endl;
    {
        kv::Request in;
        in.op = kv::Op::Set;
        in.key = "hello";
        in.value = "world";

        std::string bytes = kv::encode_request(in);

        kv::Request out;
        CHECK(kv::parse_request(bytes.data(),bytes.size(),out));
        CHECK(out.op == kv::Op::Set);
        CHECK(out.key == "hello");
        CHECK(out.value == "world");
    }

    std::cout << "[4] 请求坏输入必须被拒绝" << std::endl;
    {
        kv::Request req;
        kv::Request out;

        // 4a 长度不足固定头
        req.op = kv::Op::Set;
        req.key = "a";
        req.value = "1";
        std::string good = kv::encode_request(req);
        CHECK(!kv::parse_request(good.data(),8,out));

        // 4b op 非法
        std::string bad_op = good;
        bad_op[0] = 0;
        CHECK(!kv::parse_request(bad_op.data(),bad_op.size(),out));
        bad_op[0] = 4;
        CHECK(!kv::parse_request(bad_op.data(),bad_op.size(),out));
        bad_op[0] = static_cast<char>(0xFF);
        CHECK(!kv::parse_request(bad_op.data(),bad_op.size(),out));

        // 4c key_len == 0（空 key 非法）
        kv::Request empty_key;
        empty_key.op = kv::Op::Set;
        empty_key.key = "";
        empty_key.value = "v";
        //encode 允许产出空 key（校验在 parse 一侧），这里手工构造
        std::string zk = kv::encode_request(empty_key);
        CHECK(!kv::parse_request(zk.data(),zk.size(),out));

        // 4d key_len 超过 MAX_KEY
        std::string big_key = good;
        big_key[1] = static_cast<char>(0xFF);
        big_key[2] = static_cast<char>(0xFF);
        big_key[3] = static_cast<char>(0xFF);
        big_key[4] = static_cast<char>(0xFF);
        CHECK(!kv::parse_request(big_key.data(),big_key.size(),out));

        // 4e val_len 超过 MAX_VALUE
        std::string big_val = good;
        big_val[5] = static_cast<char>(0xFF);
        big_val[6] = static_cast<char>(0xFF);
        big_val[7] = static_cast<char>(0xFF);
        big_val[8] = static_cast<char>(0xFF);
        CHECK(!kv::parse_request(big_val.data(),big_val.size(),out));

        // 4f 声明的长度比实际字节多 / 少：必须拒绝（不能"按声明切"）
        CHECK(!kv::parse_request(good.data(),good.size() - 1,out));
        std::string extra = good + "x";
        CHECK(!kv::parse_request(extra.data(),extra.size(),out));

        // 4g GET / DEL 带了 value：非法
        kv::Request get_with_val;
        get_with_val.op = kv::Op::Get;
        get_with_val.key = "a";
        get_with_val.value = "oops";
        std::string gwv = kv::encode_request(get_with_val);
        CHECK(!kv::parse_request(gwv.data(),gwv.size(),out));
    }

    // -------------------------------------------------------------------------
    std::cout << "[5] 响应编码/解码" << std::endl;
    {
        kv::Response resp;
        resp.status = kv::Status::Ok;
        resp.value = "v1";

        std::string bytes = kv::encode_response(resp);
        CHECK(bytes.size() == 7);   //5 + 2
        CHECK(hex(bytes) == "00 00 00 00 02 76 31");

        kv::Response out;
        CHECK(kv::parse_response(bytes.data(),bytes.size(),out));
        CHECK(out.status == kv::Status::Ok);
        CHECK(out.value == "v1");

        //OK 但没有 value（SET / DEL 的响应）
        resp.value.clear();
        bytes = kv::encode_response(resp);
        CHECK(bytes.size() == 5);
        CHECK(hex(bytes) == "00 00 00 00 00");
        CHECK(kv::parse_response(bytes.data(),bytes.size(),out));
        CHECK(out.status == kv::Status::Ok);
        CHECK(out.value.empty());

        //NOT_FOUND
        resp.status = kv::Status::NotFound;
        bytes = kv::encode_response(resp);
        CHECK(hex(bytes) == "01 00 00 00 00");
        CHECK(kv::parse_response(bytes.data(),bytes.size(),out));
        CHECK(out.status == kv::Status::NotFound);

        //ERROR
        resp.status = kv::Status::Error;
        bytes = kv::encode_response(resp);
        CHECK(hex(bytes) == "02 00 00 00 00");
        CHECK(kv::parse_response(bytes.data(),bytes.size(),out));
        CHECK(out.status == kv::Status::Error);
    }

    std::cout << "[6] 响应坏输入必须被拒绝" << std::endl;
    {
        kv::Response resp;
        resp.status = kv::Status::Ok;
        resp.value = "v1";
        std::string good = kv::encode_response(resp);

        kv::Response out;

        // 6a 长度不足固定头
        CHECK(!kv::parse_response(good.data(),4,out));

        // 6b status 非法
        std::string bad_status = good;
        bad_status[0] = 3;
        CHECK(!kv::parse_response(bad_status.data(),bad_status.size(),out));

        // 6c 声明长度与实际不符
        CHECK(!kv::parse_response(good.data(),good.size() - 1,out));
        std::string extra = good + "x";
        CHECK(!kv::parse_response(extra.data(),extra.size(),out));
    }

    // -------------------------------------------------------------------------
    if(g_failed == 0)
    {
        std::cout << "\nall protocol tests passed" << std::endl;
        return 0;
    }

    std::cout << "\n" << g_failed << " protocol check(s) FAILED" << std::endl;
    return 1;
}
