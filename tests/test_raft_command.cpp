// M8：实际日志命令编解码；黄金字节不调用生产编码工具生成。
#include "raft/command.h"
#include "raft/logstore.h"
#include "base/limits.h"
#include <cstdint>
#include <cstdlib>
#include <iostream>
#include <string>
#include <vector>

#define CHECK(x) do { if(!(x)) { std::cerr << __FILE__ << ':' << __LINE__ \
    << ": FAIL " #x << '\n'; std::exit(1); } } while(false)

namespace {
using raft::Command;

//故意不用 be32.h，为非法输入独立构造长度字段。
std::string wire(unsigned char op,std::uint32_t key,std::uint32_t value,
                 const std::string& payload)
{
    std::string result(1,static_cast<char>(op));
    for(auto number : {key,value})
        for(int shift = 24; shift >= 0; shift -= 8)
            result.push_back(static_cast<char>((number >> shift) & 255));
    return result + payload;
}

//失败必须保留包括 noop 标志在内的旧结果。
void reject_decode(const std::string& bytes)
{
    Command out;
    out.noop = true;
    out.request = {kv::Op::Del,"keep",std::string("v\0x",3)};
    std::string error;
    CHECK(!raft::decode_command(bytes,out,error));
    CHECK(!error.empty());
    CHECK(out.noop && out.request.op == kv::Op::Del);
    CHECK(out.request.key == "keep" && out.request.value == std::string("v\0x",3));
}

//编码失败也不能用半成品覆盖调用者的缓冲。
void reject_encode(const kv::Request& request)
{
    std::string out("keep\0bytes",10),error;
    const auto original = out;
    CHECK(!raft::encode_kv_command(request,out,error));
    CHECK(!error.empty() && out == original);
}

//核对固定黄金值和 no-op 的精确单字节语法。
void golden()
{
    const auto noop = raft::make_noop_command();
    CHECK(noop == std::string(1,'\0'));
    Command decoded;
    decoded.request = {kv::Op::Set,"old","value"};
    std::string error = "stale";
    CHECK(raft::decode_command(noop,decoded,error));
    CHECK(error.empty() && decoded.noop);
    CHECK(decoded.request.key.empty() && decoded.request.value.empty());
    const unsigned char bytes[] = {
        1,0,0,0,3,0,0,0,2,'a',0,'b',0,255
    };
    const std::string expected(reinterpret_cast<const char*>(bytes),sizeof(bytes));
    kv::Request request{kv::Op::Set,std::string("a\0b",3),std::string("\0\xff",2)};
    std::string out = "old";
    CHECK(raft::encode_kv_command(request,out,error));
    CHECK(error.empty() && out == expected);
    CHECK(raft::decode_command(expected,decoded,error));
    CHECK(!decoded.noop && decoded.request.op == request.op);
    CHECK(decoded.request.key == request.key && decoded.request.value == request.value);
    std::cout << "PASS command: noop and binary SET independent golden bytes\n";
}

//三个操作的 payload 不带客户端帧的外层 4 字节长度。
void operations()
{
    for(const auto op : {kv::Op::Set,kv::Op::Get,kv::Op::Del})
    {
        kv::Request request{op,"k",""};
        std::string out,error;
        CHECK(raft::encode_kv_command(request,out,error));
        CHECK(out == wire(static_cast<unsigned char>(op),1,0,"k"));
        Command decoded;
        decoded.noop = true;
        CHECK(raft::decode_command(out,decoded,error));
        CHECK(!decoded.noop && decoded.request.op == op && decoded.request.value.empty());
    }
    std::cout << "PASS command: SET/GET/DEL and empty SET value\n";
}

//字段边界与操作码在产生字符串前拒绝。
void invalid_requests()
{
    reject_encode({static_cast<kv::Op>(0),"k",""});
    reject_encode({static_cast<kv::Op>(4),"k",""});
    reject_encode({static_cast<kv::Op>(255),"k",""});
    for(const auto op : {kv::Op::Set,kv::Op::Get,kv::Op::Del})
        reject_encode({op,"",""});
    reject_encode({kv::Op::Set,std::string(kv::MAX_KEY + 1,'k'),""});
    reject_encode({kv::Op::Set,"k",std::string(kv::MAX_VALUE + 1,'v')});
    reject_encode({kv::Op::Get,"k","v"});
    reject_encode({kv::Op::Del,"k","v"});
    std::string out,error;
    CHECK(raft::encode_kv_command({kv::Op::Get,std::string(kv::MAX_KEY,'k'),""},out,error));
    Command decoded;
    CHECK(raft::decode_command(out,decoded,error) && decoded.request.key.size() == kv::MAX_KEY);
    std::cout << "PASS command: operation/field limits and encode output preservation\n";
}

//检查每个截断点、尾随字节、谎报长度以及 no-op 与普通请求的分界。
void malformed()
{
    reject_decode("");
    reject_decode(std::string("\0\0",2));
    reject_decode(std::string("\0payload",8));
    for(const auto op : {1,2,3})
    {
        const auto bytes = wire(static_cast<unsigned char>(op),1,0,"k");
        for(std::size_t n = 0; n < bytes.size(); ++n)
            reject_decode(bytes.substr(0,n));
        reject_decode(bytes + "x");
    }
    const auto set = wire(1,2,3,"abxyz");
    for(std::size_t n = 0; n < set.size(); ++n) reject_decode(set.substr(0,n));
    for(const auto op : {4,255}) reject_decode(wire(static_cast<unsigned char>(op),1,0,"k"));
    reject_decode(wire(1,0,0,""));
    reject_decode(wire(1,1,0,""));
    reject_decode(wire(1,2,0,"k"));
    reject_decode(wire(1,1,2,"kv"));
    reject_decode(wire(1,UINT32_MAX,0,"k"));
    reject_decode(wire(1,1,UINT32_MAX,"k"));
    reject_decode(wire(1,static_cast<std::uint32_t>(kv::MAX_KEY + 1),0,""));
    reject_decode(wire(1,1,static_cast<std::uint32_t>(kv::MAX_VALUE + 1),"k"));
    reject_decode(wire(2,1,1,"kv"));
    reject_decode(wire(3,1,1,"kv"));
    std::cout << "PASS command: all truncation points, trailing bytes and malformed lengths\n";
}

//16MiB是整个command上限，不是单独value的上限。
void total_budget()
{
    const auto limit = raft::LogStore::MAX_COMMAND_SIZE;
    kv::Request request{kv::Op::Set,"k",std::string(limit - kv::REQUEST_HEADER_SIZE - 1,'v')};
    std::string out,error;
    CHECK(raft::encode_kv_command(request,out,error) && out.size() == limit);
    Command decoded;
    CHECK(raft::decode_command(out,decoded,error));
    CHECK(decoded.request.value == request.value);
    request.value.push_back('x');
    reject_encode(request);
    out.push_back('x');
    reject_decode(out);
    reject_encode({kv::Op::Set,"k",std::string(kv::MAX_VALUE,'v')});
    std::cout << "PASS command: exact 16MiB total and one byte over\n";
}

//输入和输出引用同一字符串时，局部构造仍应先完成再发布。
void aliases()
{
    kv::Request request{kv::Op::Set,"key","value"};
    const auto expected = wire(1,3,5,"keyvalue");
    std::string error = "old";
    CHECK(raft::encode_kv_command(request,request.key,error));
    CHECK(error.empty() && request.key == expected);
    Command decoded;
    decoded.request.key = expected;
    CHECK(raft::decode_command(decoded.request.key,decoded,error));
    CHECK(!decoded.noop && decoded.request.key == "key" && decoded.request.value == "value");
    decoded.request.value = raft::make_noop_command();
    CHECK(raft::decode_command(decoded.request.value,decoded,error));
    CHECK(decoded.noop && decoded.request.key.empty() && decoded.request.value.empty());
    std::cout << "PASS command: in-place encode/decode publication\n";
}
}

int main()
{
    golden();
    operations();
    invalid_requests();
    malformed();
    total_budget();
    aliases();
    std::cout << "raft command PASS (6 groups; codec only, no KV apply)\n";
}
