// M6 第5步：非阻塞 transport
//
// 全部用真实 loopback socket，没有假网络。两个 transport 在同一进程里跑，
// 但它们之间走的是真的 TCP，不是直接调用对方的方法。
//
// 端口用 bind(0) 让内核分配，避免和 9006 或别的测试撞。

#include "raft/protocol/rpc_codec.h"
#include "raft/transport/transport.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <string>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

// 链接器包装仅作用于本测试：定点让一次 send 失败，真实连接和其余收发保持原样。
namespace {
bool fail_next_send = false;
bool force_async_connect = false;
int injected_send_failures = 0;
}

extern "C" ssize_t __real_send(int,const void*,std::size_t,int);
extern "C" int __real_connect(int,const sockaddr*,socklen_t);

// 用 EPIPE 触发生产 transport 的关闭连接路径。
extern "C" ssize_t __wrap_send(int fd,const void* data,std::size_t length,int flags)
{
    if(fail_next_send)
    {
        fail_next_send = false;
        ++injected_send_failures;
        errno = EPIPE;
        return -1;
    }
    return __real_send(fd,data,length,flags);
}

// 即便内核立即完成连接，也让夹具走 connect_ready，避免依赖连接完成的时机。
extern "C" int __wrap_connect(int fd,const sockaddr* address,socklen_t length)
{
    const int result = __real_connect(fd,address,length);
    if(force_async_connect && result == 0)
    {
        errno = EINPROGRESS;
        return -1;
    }
    return result;
}

namespace {

using raft::AppendEntriesRequest;
using raft::LogEntry;
using raft::RaftConfig;
using raft::RaftTransport;
using raft::RequestVoteRequest;
using raft::RequestVoteResponse;
using raft::AppendEntriesResponse;
using raft::RpcType;

// ---------- 找空闲端口 ----------

std::uint16_t free_port()
{
    const int fd = ::socket(AF_INET,SOCK_STREAM,0);
    CHECK(fd >= 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = 0;

    CHECK(::bind(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)) == 0);

    socklen_t len = sizeof(addr);
    CHECK(::getsockname(fd,reinterpret_cast<sockaddr*>(&addr),&len) == 0);
    ::close(fd);

    return ntohs(addr.sin_port);
}

// ---------- 由字符串造配置（顺便复用第4步的解析） ----------

RaftConfig make_config(const std::string& id,const std::string& peers)
{
    RaftConfig config;
    std::string error;

    if(!raft::parse_raft_config(id,peers,"/tmp/raft-transport-test",config,error))
    {
        std::cerr << "config error: " << error << '\n';
        CHECK(false);
    }

    return config;
}

// 每次驱动网络事件时检查本地错误，避免套件忽略失败后继续等待。
void poll_checked(RaftTransport& transport,int timeout_ms)
{
    std::string error;
    const bool ok = transport.poll(timeout_ms,error);
    if(!ok)
    {
        std::cerr << "transport poll error: " << error << '\n';
    }
    CHECK(ok);
}

// ---------- 轮询直到条件成立 ----------

template <typename Predicate>
bool pump_until(RaftTransport& a,RaftTransport& b,Predicate done,int budget_ms)
{
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);

    while(std::chrono::steady_clock::now() < deadline)
    {
        if(done())
        {
            return true;
        }

        poll_checked(a,5);
        poll_checked(b,5);

        if(done())
        {
            return true;
        }
    }

    return done();
}

void pump_for(RaftTransport& a,RaftTransport& b,int ms)
{
    poll_checked(a,ms);
    poll_checked(b,0);
}

template <typename Predicate>
bool poll_until(RaftTransport& t,Predicate done,int budget_ms)
{
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);

    while(std::chrono::steady_clock::now() < deadline)
    {
        if(done())
        {
            return true;
        }

        poll_checked(t,5);
    }

    return done();
}

// ---------- 裸 socket：模拟"不听话的对端" ----------

int raw_connect(std::uint16_t port)
{
    const int fd = ::socket(AF_INET,SOCK_STREAM,0);
    CHECK(fd >= 0);

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    addr.sin_port = htons(port);

    CHECK(::connect(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)) == 0);
    return fd;
}

void raw_send(int fd,const std::string& bytes)
{
    std::size_t sent = 0;

    while(sent < bytes.size())
    {
        const ssize_t n = ::send(fd,bytes.data() + sent,bytes.size() - sent,MSG_NOSIGNAL);
        CHECK(n > 0);
        sent += static_cast<std::size_t>(n);
    }
}

// 等对端把连接关掉：读到 0 或出错都算
bool raw_sees_close(int fd,int budget_ms)
{
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(budget_ms);

    while(std::chrono::steady_clock::now() < deadline)
    {
        pollfd item{};
        item.fd = fd;
        item.events = POLLIN;

        const int ready = ::poll(&item,1,10);

        if(ready <= 0)
        {
            continue;
        }

        char buffer[256];
        const ssize_t n = ::recv(fd,buffer,sizeof(buffer),0);

        if(n <= 0)
        {
            return true;      // 0 = EOF，<0 = 重置
        }

        // 对端还在发东西（例如它自己的 Hello），继续等
    }

    return false;
}

// 一条能发 Hello、之后不读的假 peer
struct StuckPeer
{
    int listen_fd = -1;
    std::vector<int> conns;
    std::uint16_t port = 0;

    // 只建监听并记下端口：此时对方还没启动，不能在这里等
    void listen()
    {
        listen_fd = ::socket(AF_INET,SOCK_STREAM,0);
        CHECK(listen_fd >= 0);

        int on = 1;
        CHECK(::setsockopt(listen_fd,SOL_SOCKET,SO_REUSEADDR,&on,sizeof(on)) == 0);

        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;

        CHECK(::bind(listen_fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)) == 0);
        CHECK(::listen(listen_fd,4) == 0);

        socklen_t len = sizeof(addr);
        CHECK(::getsockname(listen_fd,reinterpret_cast<sockaddr*>(&addr),&len) == 0);
        port = ntohs(addr.sin_port);
    }

    // ID 比 3 小的节点都会主动拨过来（1 和 2 都拨），所以这里全部接受，
    // 每条都回 Hello，然后一个字节都不读。
    void accept_many(int max_conns,std::uint64_t id,int budget_ms)
    {
        std::string body,frame,error;
        CHECK(raft::encode_hello(id,body,error));
        CHECK(raft::encode_frame(RpcType::Hello,body,frame,error));

        const auto deadline = std::chrono::steady_clock::now() +
                              std::chrono::milliseconds(budget_ms);

        int accepted = 0;

        while(accepted < max_conns && std::chrono::steady_clock::now() < deadline)
        {
            pollfd item{};
            item.fd = listen_fd;
            item.events = POLLIN;

            if(::poll(&item,1,50) != 1)
            {
                continue;
            }

            const int fd = ::accept(listen_fd,nullptr,nullptr);

            if(fd < 0)
            {
                continue;
            }

            conns.push_back(fd);
            raw_send(fd,frame);
            ++accepted;
        }

        CHECK(accepted == max_conns);
    }

    ~StuckPeer()
    {
        for(const int fd : conns)
        {
            ::close(fd);
        }

        if(listen_fd >= 0) ::close(listen_fd);
    }
};

// ---------- 1. 握手 ----------

void test_handshake()
{
    const std::uint16_t port_a = free_port();
    const std::uint16_t port_b = free_port();

    RaftConfig config_a = make_config("1",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));
    RaftConfig config_b = make_config("2",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));

    int down_a = 0;
    int down_b = 0;

    RaftTransport a(config_a,[](std::uint64_t,RpcType,std::uint64_t,const std::string&){},
                    [&](std::uint64_t){ ++down_a; });
    RaftTransport b(config_b,[](std::uint64_t,RpcType,std::uint64_t,const std::string&){},
                    [&](std::uint64_t){ ++down_b; });

    std::string error;
    CHECK(b.start(error));
    CHECK(a.start(error));

    // 只有 ID 小的一方主动拨，所以 A 连 B
    CHECK(pump_until(a,b,[&]{ return a.connected(2) && b.connected(1); },2000));
    CHECK(down_a == 0 && down_b == 0);

    a.stop();
    b.stop();

    std::cout << "PASS handshake: both sides know each other by id, only the lower id dialed\n";
}

// ---------- 2. 四类帧往返 ----------

void test_four_rpc_types()
{
    const std::uint16_t port_a = free_port();
    const std::uint16_t port_b = free_port();

    RaftConfig config_a = make_config("1",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));
    RaftConfig config_b = make_config("2",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));

    std::vector<RpcType> got_types;
    std::vector<std::string> got_bodies;
    std::vector<std::uint64_t> got_peers;
    std::vector<std::uint64_t> got_ids;

    RaftTransport a(config_a,[](std::uint64_t,RpcType,std::uint64_t,const std::string&){},[](std::uint64_t){});
    RaftTransport b(config_b,
        [&](std::uint64_t peer,RpcType type,std::uint64_t rpc_id,const std::string& body)
        {
            got_peers.push_back(peer);
            got_types.push_back(type);
            got_bodies.push_back(body);
            got_ids.push_back(rpc_id);
        },
        [](std::uint64_t){});

    std::string error;
    CHECK(b.start(error));
    CHECK(a.start(error));
    CHECK(pump_until(a,b,[&]{ return a.connected(2); },2000));

    // 四类正文都用合法值，否则 transport 的校验会先挡掉
    RequestVoteRequest vote_request;
    vote_request.term = 5;
    vote_request.candidate_id = 1;
    vote_request.last_log_index = 130;
    vote_request.last_log_term = 5;

    RequestVoteResponse vote_response;
    vote_response.term = 5;
    vote_response.vote_granted = true;

    AppendEntriesRequest append_request;
    append_request.term = 5;
    append_request.leader_id = 1;
    append_request.prev_log_index = 130;
    append_request.prev_log_term = 5;
    append_request.leader_commit = 129;
    append_request.entries.push_back(LogEntry{0,5,std::string("binary\0cmd",10)});

    AppendEntriesResponse append_response;
    append_response.term = 5;
    append_response.success = false;

    const std::pair<RpcType,std::string> frames[] = {
        {RpcType::VoteRequest,     [&]{ std::string s,e; raft::encode_vote_request(vote_request,s,e); return s; }()},
        {RpcType::VoteResponse,    [&]{ std::string s,e; raft::encode_vote_response(vote_response,s,e); return s; }()},
        {RpcType::AppendEntries,   [&]{ std::string s,e; raft::encode_append_entries(append_request,s,e); return s; }()},
        {RpcType::AppendResponse,  [&]{ std::string s,e; raft::encode_append_response(append_response,s,e); return s; }()},
    };

    std::uint64_t next_id = 42;
    for(const auto& item : frames)
    {
        CHECK(a.send(2,item.first,next_id++,item.second,error));
        pump_for(a,b,20);
    }

    CHECK(pump_until(a,b,[&]{ return got_types.size() == 4; },2000));
    CHECK(got_ids.size() == 4);
    CHECK(got_types.size() == 4);
    CHECK(got_bodies.size() == 4);
    CHECK(got_peers.size() == 4);

    for(std::size_t i = 0 ; i < 4 ; i++)
    {
        CHECK(got_types[i] == frames[i].first);
        CHECK(got_bodies[i] == frames[i].second);   // 字节级一致
        CHECK(got_peers[i] == 1);                   // 对端看到的是 A 的 ID
        CHECK(got_ids[i] == 42 + i);
    }

    a.stop();
    b.stop();

    std::cout << "PASS four rpc types: bodies arrive byte-identical, peer id resolved\n";
}

// ---------- 3. 半帧：不能提前派发 ----------

void test_half_frame_is_not_dispatched()
{
    const std::uint16_t port_b = free_port();
    const std::uint16_t port_a = free_port();

    RaftConfig config_b = make_config("2",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));

    int calls = 0;
    RaftTransport b(config_b,[&](std::uint64_t,RpcType,std::uint64_t,const std::string&){ ++calls; },[](std::uint64_t){});

    std::string error;
    CHECK(b.start(error));

    RequestVoteResponse response;
    response.term = 7;
    response.vote_granted = true;

    std::string body,frame;
    CHECK(raft::encode_vote_response(response,body,error));
    CHECK(raft::encode_rpc_frame(RpcType::VoteResponse,101,body,frame,error));

    const int fd = raw_connect(port_b);

    std::string hello_body,hello_frame;
    CHECK(raft::encode_hello(1,hello_body,error));
    CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello_frame,error));
    raw_send(fd,hello_frame);

    poll_checked(b,20);
    CHECK(calls == 0);       // 握手帧不算业务帧

    // 先只发前 3 个字节：连长度头都不完整
    raw_send(fd,frame.substr(0,3));
    poll_checked(b,30);
    CHECK(calls == 0);

    // 补到差一字节
    raw_send(fd,frame.substr(3,frame.size() - 4));
    poll_checked(b,30);
    CHECK(calls == 0);

    // 最后一个字节，这一帧才完整
    raw_send(fd,frame.substr(frame.size() - 1));
    CHECK(poll_until(b,[&]{ return calls == 1; },2000));
    CHECK(calls == 1);

    ::close(fd);
    b.stop();

    std::cout << "PASS half frame: nothing dispatched until the last byte arrives\n";
}

// ---------- 4. 一个 write 里塞两帧 ----------

void test_two_frames_in_one_write()
{
    const std::uint16_t port_b = free_port();
    const std::uint16_t port_a = free_port();

    RaftConfig config_b = make_config("2",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));

    std::vector<std::uint64_t> terms;
    std::vector<std::uint64_t> ids;
    RaftTransport b(config_b,
        [&](std::uint64_t,RpcType type,std::uint64_t rpc_id,const std::string& body)
        {
            CHECK(type == RpcType::VoteResponse);
            RequestVoteResponse parsed;
            std::string error;
            CHECK(raft::decode_vote_response(body,parsed,error));
            terms.push_back(parsed.term);
            ids.push_back(rpc_id);
        },
        [](std::uint64_t){});

    std::string error;
    CHECK(b.start(error));

    RequestVoteResponse first;
    first.term = 11;
    first.vote_granted = false;

    RequestVoteResponse second;
    second.term = 12;
    second.vote_granted = true;

    std::string b1,f1,b2,f2,hello_body,hello_frame;
    CHECK(raft::encode_vote_response(first,b1,error));
    CHECK(raft::encode_rpc_frame(RpcType::VoteResponse,201,b1,f1,error));
    CHECK(raft::encode_vote_response(second,b2,error));
    CHECK(raft::encode_rpc_frame(RpcType::VoteResponse,202,b2,f2,error));
    CHECK(raft::encode_hello(1,hello_body,error));
    CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello_frame,error));

    const int fd = raw_connect(port_b);

    // 三次 send 合成一次写：Hello + 两帧
    raw_send(fd,hello_frame + f1 + f2);

    // 一次 poll 只处理"当前已到达"的字节：loopback 也可能只到一半，必须轮询
    CHECK(poll_until(b,[&]{ return terms.size() >= 2; },2000));
    CHECK(terms.size() == 2);
    CHECK(terms[0] == 11 && terms[1] == 12);   // 顺序保持
    CHECK(ids.size() == 2 && ids[0] == 201 && ids[1] == 202);

    ::close(fd);
    b.stop();

    std::cout << "PASS sticky frames: two frames in one write are split and delivered in order\n";
}

// ---------- 5. 坏帧必须断开 ----------

void test_bad_frames_are_refused()
{
    const std::uint16_t port_b = free_port();
    const std::uint16_t port_a = free_port();

    RaftConfig config_b = make_config("2",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));

    int calls = 0;
    RaftTransport b(config_b,[&](std::uint64_t,RpcType,std::uint64_t,const std::string&){ ++calls; },[](std::uint64_t){});

    std::string error;
    CHECK(b.start(error));

    // 5a：长度头声称 4GiB
    {
        const int fd = raw_connect(port_b);
        std::string hello_body,hello_frame;
        CHECK(raft::encode_hello(1,hello_body,error));
        CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello_frame,error));
        raw_send(fd,hello_frame);

        const char huge[4] = {'\xff','\xff','\xff','\xff'};
        raw_send(fd,std::string(huge,4));

        // 一边驱动服务端，一边等它把连接关掉
        CHECK(poll_until(b,[&]{ return raw_sees_close(fd,5); },2000));
        CHECK(calls == 0);
        ::close(fd);
    }

    // 5b：第一帧不是 Hello
    {
        const int fd = raw_connect(port_b);

        RequestVoteResponse response;
        response.term = 3;
        response.vote_granted = true;

        std::string body,frame;
        CHECK(raft::encode_vote_response(response,body,error));
        CHECK(raft::encode_rpc_frame(RpcType::VoteResponse,101,body,frame,error));
        raw_send(fd,frame);

        CHECK(poll_until(b,[&]{ return raw_sees_close(fd,5); },2000));
        CHECK(calls == 0);
        ::close(fd);
    }

    // 5c：Hello 谎报成服务端自己的 ID
    {
        const int fd = raw_connect(port_b);

        std::string body,frame;
        CHECK(raft::encode_hello(2,body,error));      // 2 就是 B 自己
        CHECK(raft::encode_frame(RpcType::Hello,body,frame,error));
        raw_send(fd,frame);

        CHECK(poll_until(b,[&]{ return raw_sees_close(fd,5); },2000));
        ::close(fd);
    }

    // 5d：语义非法的请求 —— term 3 的请求带 term 4 的条目
    {
        const int fd = raw_connect(port_b);

        std::string hello_body,hello_frame;
        CHECK(raft::encode_hello(1,hello_body,error));
        CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello_frame,error));
        raw_send(fd,hello_frame);

        AppendEntriesRequest poison;
        poison.term = 3;
        poison.leader_id = 1;
        poison.prev_log_index = 0;
        poison.prev_log_term = 0;
        poison.entries.push_back(LogEntry{0,4,"poison"});

        std::string body,frame;
        CHECK(raft::encode_append_entries(poison,body,error));
        CHECK(raft::encode_rpc_frame(RpcType::AppendEntries,105,body,frame,error));
        raw_send(fd,frame);

        CHECK(poll_until(b,[&]{ return raw_sees_close(fd,5); },2000));
        CHECK(calls == 0);                      // 校验层先挡下，不进回调
        ::close(fd);
    }

    b.stop();

    std::cout << "PASS bad frames: oversize, non-hello first, self id, invalid request all close\n";
}

// ---------- 6. 对端关闭要通知 ----------

void test_peer_down_is_reported()
{
    const std::uint16_t port_a = free_port();
    const std::uint16_t port_b = free_port();

    RaftConfig config_a = make_config("1",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));
    RaftConfig config_b = make_config("2",
        "1@127.0.0.1:" + std::to_string(port_a) + ",2@127.0.0.1:" + std::to_string(port_b));

    std::vector<std::uint64_t> down;
    RaftTransport a(config_a,[](std::uint64_t,RpcType,std::uint64_t,const std::string&){},
                    [&](std::uint64_t peer){ down.push_back(peer); });
    RaftTransport b(config_b,[](std::uint64_t,RpcType,std::uint64_t,const std::string&){},[](std::uint64_t){});

    std::string error;
    CHECK(b.start(error));
    CHECK(a.start(error));
    CHECK(pump_until(a,b,[&]{ return a.connected(2); },2000));

    // B 主动关停：A 应当发现连接没了，并收到一次 peer_down
    b.stop();

    const auto deadline = std::chrono::steady_clock::now() + std::chrono::milliseconds(2000);

    while(std::chrono::steady_clock::now() < deadline && down.empty())
    {
        poll_checked(a,10);
    }

    CHECK(down.size() == 1);
    CHECK(down[0] == 2);
    CHECK(!a.connected(2));

    a.stop();

    std::cout << "PASS peer down: remote stop is noticed and reported once\n";
}

// ---------- 7. 慢 peer 不拖累别的 peer ----------

void test_slow_peer_does_not_block_others()
{
    const std::uint16_t port_a = free_port();
    const std::uint16_t port_b = free_port();

    StuckPeer stuck;
    stuck.listen();                // 先占好端口，等 A 启动后再接受

    const std::string peers =
        "1@127.0.0.1:" + std::to_string(port_a) +
        ",2@127.0.0.1:" + std::to_string(port_b) +
        ",3@127.0.0.1:" + std::to_string(stuck.port);

    RaftConfig config_a = make_config("1",peers);
    RaftConfig config_b = make_config("2",peers);

    int got_at_b = 0;
    RaftTransport a(config_a,[](std::uint64_t,RpcType,std::uint64_t,const std::string&){},[](std::uint64_t){});
    RaftTransport b(config_b,[&](std::uint64_t,RpcType,std::uint64_t,const std::string&){ ++got_at_b; },[](std::uint64_t){});

    std::string error;
    CHECK(b.start(error));
    CHECK(a.start(error));       // A 在这里就拨了 3

    stuck.accept_many(2,3,3000); // A 和 B 都会连过来；握手后不再读

    CHECK(pump_until(a,b,[&]{ return a.connected(3) && a.connected(2); },3000));

    // 向不再读取的 peer 发送 4MiB，同时检查另一条连接仍能收消息。
    AppendEntriesRequest big;
    big.term = 5;
    big.leader_id = 1;
    big.prev_log_index = 0;
    big.prev_log_term = 0;
    big.entries.push_back(LogEntry{0,5,std::string(4 * 1024 * 1024,'x')});

    std::string big_body,big_frame;
    CHECK(raft::encode_append_entries(big,big_body,error));
    CHECK(raft::encode_rpc_frame(RpcType::AppendEntries,100,big_body,big_frame,error));

    CHECK(a.send(3,RpcType::AppendEntries,100,big_body,error));

    // 另一条连接照常工作
    RequestVoteResponse response;
    response.term = 5;
    response.vote_granted = true;

    std::string small_body;
    CHECK(raft::encode_vote_response(response,small_body,error));
    CHECK(a.send(2,RpcType::VoteResponse,101,small_body,error));

    CHECK(pump_until(a,b,[&]{ return got_at_b > 0; },2000));
    CHECK(got_at_b == 1);

    a.stop();
    b.stop();

    std::cout << "PASS slow peer: a 4MiB send to a non-reading peer does not stop the other peer\n";
}

// connect_ready 发 Hello 失败后会删除 Connection，随后只能用事先保存的 fd 查表。
void test_connect_hello_send_failure()
{
    StuckPeer extra;
    extra.listen();
    const auto port_a = free_port();
    auto port_b = free_port();
    while(port_b == port_a) port_b = free_port();
    const std::string peers = "1@127.0.0.1:" + std::to_string(port_a) +
                             ",2@127.0.0.1:" + std::to_string(port_b) +
                             ",3@127.0.0.1:" + std::to_string(extra.port);
    RaftTransport a(make_config("1",peers),
        [](std::uint64_t,RpcType,std::uint64_t,const std::string&){},
        [](std::uint64_t){});
    RaftTransport b(make_config("2",peers),
        [](std::uint64_t,RpcType,std::uint64_t,const std::string&){},
        [](std::uint64_t){});
    std::string error;
    CHECK(b.start(error));
    force_async_connect = true;
    CHECK(a.start(error));
    force_async_connect = false;

    const int before = injected_send_failures;
    fail_next_send = true;
    CHECK(poll_until(a,[&]{ return injected_send_failures == before + 1; },2000));
    CHECK(!fail_next_send);
    CHECK(!a.connected(2));
    a.stop();
    b.stop();
    std::cout << "PASS lifetime: failed Hello send closes connection without a stale reference\n";
}

// 模拟 Worker 在收包回调里回包失败：连接已被 send 删除，read_ready 必须结束处理。
void test_callback_reply_send_failure()
{
    const auto port_a = free_port();
    auto port_b = free_port();
    while(port_b == port_a) port_b = free_port();
    const std::string peers = "1@127.0.0.1:" + std::to_string(port_a) +
                             ",2@127.0.0.1:" + std::to_string(port_b);
    int calls = 0;
    int down = 0;
    RaftTransport* receiver = nullptr;
    RaftTransport b(make_config("2",peers),
        [&](std::uint64_t peer,RpcType type,std::uint64_t id,const std::string& body)
        {
            CHECK(peer == 1 && type == RpcType::VoteRequest && id == 42);
            RequestVoteRequest request;
            std::string reply_body,error;
            CHECK(raft::decode_vote_request(body,request,error));
            CHECK(raft::encode_vote_response(
                  RequestVoteResponse{request.term,true},reply_body,error));
            const int before = injected_send_failures;
            fail_next_send = true;
            CHECK(!receiver->send(peer,RpcType::VoteResponse,id,reply_body,error));
            CHECK(injected_send_failures == before + 1);
            CHECK(!error.empty());
            ++calls;
        },
        [&](std::uint64_t peer) { CHECK(peer == 1); ++down; });
    receiver = &b;
    std::string error;
    CHECK(b.start(error));
    RequestVoteRequest request;
    request.term = 3;
    request.candidate_id = 1;
    std::string body;
    CHECK(raft::encode_vote_request(request,body,error));
    const int fd = raw_connect(port_b);
    std::string hello_body,hello,first,second;
    CHECK(raft::encode_hello(1,hello_body,error));
    CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello,error));
    CHECK(raft::encode_rpc_frame(RpcType::VoteRequest,42,body,first,error));
    CHECK(raft::encode_rpc_frame(RpcType::VoteRequest,43,body,second,error));
    // 第一帧回调关闭连接时，输入缓冲中还有第二帧。
    raw_send(fd,hello + first + second);
    CHECK(poll_until(b,[&]{ return calls == 1; },2000));
    CHECK(down == 1);
    CHECK(!b.connected(1));
    CHECK(!fail_next_send);
    ::close(fd);
    b.stop();
    std::cout << "PASS lifetime: callback reply failure closes connection without read_ready UAF\n";
}

} // namespace

int main(int argc,char** argv)
{
    if(argc == 2 && std::string(argv[1]) == "--connect-send-failure")
    {
        test_connect_hello_send_failure();
        return 0;
    }
    if(argc == 2 && std::string(argv[1]) == "--callback-send-failure")
    {
        test_callback_reply_send_failure();
        return 0;
    }
    test_handshake();
    test_four_rpc_types();
    test_half_frame_is_not_dispatched();
    test_two_frames_in_one_write();
    test_bad_frames_are_refused();
    test_peer_down_is_reported();
    test_slow_peer_does_not_block_others();
    test_connect_hello_send_failure();
    test_callback_reply_send_failure();

    std::cout << "raft transport PASS (real loopback sockets)\n";
    return 0;
}
