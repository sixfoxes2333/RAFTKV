// M7：实际 Worker 后台线程、真实 TCP 和持久化失败围栏；不执行 KV。
#include "raft/worker.h"
#include "raft/rpc_codec.h"

#include <arpa/inet.h>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdlib>
#include <dirent.h>
#include <filesystem>
#include <iostream>
#include <memory>
#include <poll.h>
#include <string>
#include <sys/socket.h>
#include <sys/epoll.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace {
std::string fixture_path;
std::atomic<bool> fail_epoll_once{false};
std::atomic<int> epoll_failures{0};
}

extern "C" int __real_epoll_wait(int,epoll_event*,int,int);

// 仅本测试包装一次本地 epoll 错误，检查 Worker 是否退出并报告原因。
extern "C" int __wrap_epoll_wait(int fd,epoll_event* events,int count,int timeout)
{
    if(fail_epoll_once.exchange(false))
    {
        ++epoll_failures;
        errno = EIO;
        return -1;
    }
    return __real_epoll_wait(fd,events,count,timeout);
}
#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr \
              << " fixture=" << fixture_path << '\n'; std::exit(1); \
} } while (false)

namespace {
using Clock = std::chrono::steady_clock;
using raft::RaftWorker;
using raft::RpcType;

// 每个用例独享数据目录，失败时路径会打印在 CHECK 中。
struct TempDir
{
    std::string path;
    TempDir()
    {
        char name[] = "/tmp/raft-worker-live.XXXXXX";
        CHECK(::mkdtemp(name) != nullptr);
        path = name;
        fixture_path = path;
    }
    ~TempDir()
    {
        std::error_code ignored;
        std::filesystem::remove_all(path,ignored);
    }
};

// 端口在启动前保持绑定，减少测试自己取到重复端口的机会。
struct Port
{
    int fd = -1;
    std::uint16_t number = 0;
    Port()
    {
        fd = ::socket(AF_INET,SOCK_STREAM | SOCK_CLOEXEC,0);
        CHECK(fd >= 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        CHECK(::bind(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address)) == 0);
        socklen_t length = sizeof(address);
        CHECK(::getsockname(fd,reinterpret_cast<sockaddr*>(&address),&length) == 0);
        number = ntohs(address.sin_port);
    }
    void release() { if(fd >= 0) { ::close(fd); fd = -1; } }
    ~Port() { release(); }
};

// id=1 可选单节点；其余测试用 id=3，被配置中的低 ID 裸 peer 主动连接。
struct Fixture
{
    TempDir dir;
    Port ports[3];
    raft::RaftConfig config(int id,bool single = false) const
    {
        std::string peers;
        for(int i = 0; i < (single ? 1 : 3); ++i)
        {
            if(i != 0) peers += ',';
            peers += std::to_string(i + 1) + "@127.0.0.1:" + std::to_string(ports[i].number);
        }
        raft::RaftConfig out;
        std::string error;
        CHECK(raft::parse_raft_config(std::to_string(id),peers,dir.path,out,error));
        return out;
    }
};

// 按实际状态等待，不把固定 sleep 当作启动/选举成功。
template<class Predicate>
bool wait_for(Predicate predicate,int milliseconds = 8000)
{
    const auto deadline = Clock::now() + std::chrono::milliseconds(milliseconds);
    do
    {
        if(predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(5));
    } while(Clock::now() < deadline);
    return predicate();
}

// 裸 socket 仅作为合法成员或坏输入夹具，连接和读写都有截止时间。
struct RawPeer
{
    int fd = -1;
    std::string input;
    explicit RawPeer(std::uint16_t port)
    {
        fd = ::socket(AF_INET,SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,0);
        CHECK(fd >= 0);
        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);
        const int result = ::connect(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address));
        CHECK(result == 0 || errno == EINPROGRESS);
        pollfd event{fd,POLLOUT,0};
        CHECK(::poll(&event,1,2000) > 0);
        int code = 0;
        socklen_t length = sizeof(code);
        CHECK(::getsockopt(fd,SOL_SOCKET,SO_ERROR,&code,&length) == 0 && code == 0);
    }
    ~RawPeer() { if(fd >= 0) ::close(fd); }
    void send_all(const std::string& bytes)
    {
        std::size_t at = 0;
        const auto deadline = Clock::now() + std::chrono::seconds(3);
        while(at != bytes.size() && Clock::now() < deadline)
        {
            const auto sent = ::send(fd,bytes.data() + at,bytes.size() - at,MSG_NOSIGNAL);
            if(sent > 0) { at += static_cast<std::size_t>(sent); continue; }
            if(sent < 0 && errno == EINTR) continue;
            CHECK(sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
            pollfd event{fd,POLLOUT,0};
            CHECK(::poll(&event,1,50) >= 0);
        }
        CHECK(at == bytes.size());
    }
    //读取一条真实 TCP 帧；超时返回 false，EOF 或坏帧直接使测试失败。
    bool receive_packet(RpcType& type,std::uint64_t& id,std::string& body,int timeout_ms)
    {
        const auto deadline = Clock::now() + std::chrono::milliseconds(timeout_ms);
        std::string error;
        while(Clock::now() < deadline)
        {
            if(input.size() >= 4)
            {
                std::size_t size = 0;
                CHECK(raft::peek_frame_size(input.data(),input.size(),size,error));
                if(size != 0 && input.size() >= size)
                {
                    std::string wire;
                    CHECK(raft::decode_frame(input.data(),size,type,wire,error));
                    input.erase(0,size);
                    id = 0;
                    if(type == RpcType::Hello) body = std::move(wire);
                    else CHECK(raft::decode_rpc_body(wire,id,body,error));
                    return true;
                }
            }
            char buffer[4096];
            const auto count = ::recv(fd,buffer,sizeof(buffer),0);
            if(count > 0)
            {
                input.append(buffer,static_cast<std::size_t>(count));
                continue;
            }
            if(count < 0 && errno == EINTR) continue;
            CHECK(count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
            pollfd event{fd,POLLIN,0};
            CHECK(::poll(&event,1,10) >= 0);
        }
        return false;
    }

    std::string until_closed()
    {
        std::string bytes;
        const auto deadline = Clock::now() + std::chrono::seconds(3);
        while(Clock::now() < deadline)
        {
            char buffer[4096];
            const auto count = ::recv(fd,buffer,sizeof(buffer),0);
            if(count > 0) { bytes.append(buffer,static_cast<std::size_t>(count)); continue; }
            if(count == 0 || (count < 0 && errno == ECONNRESET)) return bytes;
            if(count < 0 && errno == EINTR) continue;
            CHECK(count < 0 && (errno == EAGAIN || errno == EWOULDBLOCK));
            pollfd event{fd,POLLIN,0};
            CHECK(::poll(&event,1,50) >= 0);
        }
        CHECK(false);
        return bytes;
    }
};

// 将 Hello 和一条业务帧合成输入流；业务正文由原 codec 生成。
std::string incoming(std::uint64_t peer,RpcType type,const std::string& body)
{
    std::string hello_body,hello,frame,error;
    CHECK(raft::encode_hello(peer,hello_body,error));
    CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello,error));
    CHECK(raft::encode_rpc_frame(type,UINT64_C(0x100000001),body,frame,error));
    return hello + frame;
}

// 读取已经关闭 Worker 的磁盘状态，检查保存结果，而不只看内存快照。
raft::HardState disk_state(const std::string& directory)
{
    raft::StateStore store(directory);
    raft::HardState out;
    std::string error;
    CHECK(store.load(out,error));
    return out;
}

// 启动等待监听就绪；多个读线程只能通过快照读取状态。
void test_single_node_and_snapshot_readers()
{
    Fixture fixture;
    RaftWorker worker(fixture.config(1,true));
    std::string error;
    CHECK(!worker.start(error));
    CHECK(worker.init(error));
    CHECK(!worker.status().running);
    fixture.ports[0].release();
    CHECK(worker.start(error));
    CHECK(worker.status().running && worker.status().updated_ms != 0);
    CHECK(!worker.start(error));
    CHECK(worker.status().running);
    std::atomic<bool> done{false};
    std::atomic<int> bad{0};
    std::vector<std::thread> readers;
    for(int i = 0; i < 4; ++i)
    {
        readers.emplace_back([&]
        {
            std::uint64_t term = 0;
            while(!done.load())
            {
                const auto status = worker.status();
                if(status.term < term || status.commit > status.last ||
                   (status.running && status.role == raft::Role::Leader && status.term == 0))
                {
                    ++bad;
                }
                term = status.term;
                std::this_thread::sleep_for(std::chrono::milliseconds(1));
            }
        });
    }
    CHECK(wait_for([&] { const auto s = worker.status(); return s.running && s.role == raft::Role::Leader; }));
    worker.stop();
    worker.stop();
    done.store(true);
    for(auto& reader : readers) reader.join();
    CHECK(bad.load() == 0);
    const auto status = worker.status();
    const auto state = disk_state(fixture.dir.path);
    CHECK(!status.running && status.error.empty());
    CHECK(status.term == state.current_term && state.voted_for == 1);
    CHECK(status.last == 0 && status.commit == 0);
    CHECK(!worker.start(error));
    std::cout << "PASS worker live: single-node durable election, concurrent snapshots and idempotent stop\n";
}

// 监听失败通过 start 和快照返回，不能卡在启动等待；析构关闭失败路径资源。
void test_listener_failure()
{
    Fixture fixture;
    CHECK(::listen(fixture.ports[0].fd,4) == 0);
    {
        RaftWorker worker(fixture.config(1,true));
        std::string error;
        CHECK(worker.init(error));
        CHECK(!worker.start(error));
        CHECK(error.find("bind") != std::string::npos);
        CHECK(!worker.status().running && !worker.status().error.empty());
        worker.stop();
    }
    fixture.ports[0].release();
    RaftWorker retry(fixture.config(1,true));
    std::string error;
    CHECK(retry.init(error) && retry.start(error));
    retry.stop();
    std::cout << "PASS worker live: listener failure wakes start and releases resources\n";
}

// 本地 epoll 失败不能变成一个持续发布 running=true 的空循环。
void test_poll_failure_stops_worker()
{
    Fixture fixture;
    RaftWorker worker(fixture.config(1,true));
    std::string error;
    CHECK(worker.init(error));
    fixture.ports[0].release();
    CHECK(worker.start(error));
    const int before = epoll_failures.load();
    fail_epoll_once.store(true);
    CHECK(wait_for([&] { return !worker.status().running; }));
    worker.stop();
    CHECK(epoll_failures.load() == before + 1);
    CHECK(!fail_epoll_once.load());
    CHECK(worker.status().error.find("epoll_wait") != std::string::npos);
    std::cout << "PASS worker live: local epoll failure stops Worker and publishes the error\n";
}

// 两类更高任期回复都没有对应在途编号，仍必须保存新任期。
void test_higher_term_without_rpc_context()
{
    for(const auto type : {RpcType::VoteResponse,RpcType::AppendResponse})
    {
        Fixture fixture;
        RaftWorker worker(fixture.config(3));
        std::string error,body;
        CHECK(worker.init(error));
        fixture.ports[2].release();
        CHECK(worker.start(error));
        if(type == RpcType::VoteResponse)
            CHECK(raft::encode_vote_response(raft::RequestVoteResponse{99,false},body,error));
        else
            CHECK(raft::encode_append_response(raft::AppendEntriesResponse{99,false},body,error));
        RawPeer peer(fixture.ports[2].number);
        peer.send_all(incoming(1,type,body));
        CHECK(wait_for([&] { return worker.status().term >= 99; }));
        CHECK(worker.status().running);
        worker.stop();
        const auto state = disk_state(fixture.dir.path);
        CHECK(state.current_term >= 99);
        CHECK(state.current_term == worker.status().term);
        if(state.current_term == 99) CHECK(state.voted_for == 0);
    }
    std::cout << "PASS worker live: higher vote/append reply term persisted without matching rpc id\n";
}

// 错误来源、身份、任期上界和回复正文不得把 Core 推到输入里的任期 99。
void test_bad_rpc_does_not_poison_core()
{
    for(int kind = 0; kind < 7; ++kind)
    {
        Fixture fixture;
        RaftWorker worker(fixture.config(3));
        std::string body,error;
        RpcType type = RpcType::VoteRequest;
        std::uint64_t peer_id = 1;
        if(kind <= 1)
        {
            raft::RequestVoteRequest request;
            request.term = 99;
            request.candidate_id = kind == 0 ? 2 : 1;
            if(kind == 1) { request.last_log_index = 1; request.last_log_term = 100; }
            CHECK(raft::encode_vote_request(request,body,error));
        }
        else if(kind == 2)
        {
            type = RpcType::AppendEntries;
            raft::AppendEntriesRequest request;
            request.term = 99; request.leader_id = 1;
            request.prev_log_index = 1; request.prev_log_term = 100;
            CHECK(raft::encode_append_entries(request,body,error));
        }
        else if(kind == 3)
        {
            type = RpcType::VoteResponse;
            CHECK(raft::encode_vote_response(raft::RequestVoteResponse{99,false},body,error));
            body[8] = 2;
        }
        else if(kind == 4)
        {
            type = RpcType::AppendResponse;
            body.assign(8,'x');
        }
        else if(kind == 5)
        {
            type = RpcType::VoteResponse;
            peer_id = 4;
            CHECK(raft::encode_vote_response(raft::RequestVoteResponse{99,true},body,error));
        }
        else
        {
            type = RpcType::AppendEntries;
            raft::AppendEntriesRequest request;
            request.term = 99; request.leader_id = 1;
            request.entries.push_back(raft::LogEntry{
                0,99,std::string(raft::LogStore::MAX_COMMAND_SIZE + 1,'x')});
            CHECK(raft::encode_append_entries(request,body,error));
        }
        CHECK(worker.init(error));
        fixture.ports[2].release();
        CHECK(worker.start(error));
        RawPeer peer(fixture.ports[2].number);
        peer.send_all(incoming(peer_id,type,body));
        const auto until = Clock::now() + std::chrono::milliseconds(150);
        while(Clock::now() < until)
        {
            const auto status = worker.status();
            CHECK(status.running && status.term < 99 && status.last == 0);
            std::this_thread::sleep_for(std::chrono::milliseconds(5));
        }
        worker.stop();
        CHECK(disk_state(fixture.dir.path).current_term < 99);
    }
    std::cout << "PASS worker live: invalid sources, identities, term bounds and response bodies ignored\n";
}

// 真实临时路径失败：单节点虽然在 Core 内逻辑胜选，也不能发布可用 Leader。
void test_self_vote_save_failure()
{
    Fixture fixture;
    RaftWorker worker(fixture.config(1,true));
    std::string error;
    CHECK(worker.init(error));
    CHECK(::mkdir((fixture.dir.path + "/raft.state.tmp").c_str(),0700) == 0);
    fixture.ports[0].release();
    CHECK(worker.start(error));
    bool active_leader = false;
    CHECK(wait_for([&]
    {
        const auto status = worker.status();
        active_leader = active_leader || (status.running && status.role == raft::Role::Leader);
        return !status.running;
    }));
    CHECK(!active_leader);
    CHECK(worker.status().error.find("raft.state.tmp") != std::string::npos);
    worker.stop();
    const auto state = disk_state(fixture.dir.path);
    CHECK(state.current_term == 0 && state.voted_for == 0);
    std::cout << "PASS worker live: self-vote save failure stops before usable leadership\n";
}

// 有效复制请求的状态保存失败后，关闭连接且不发送 AppendResponse，不追加日志。
void test_follower_save_failure_no_ack()
{
    Fixture fixture;
    RaftWorker worker(fixture.config(3));
    std::string error,body;
    CHECK(worker.init(error));
    CHECK(::mkdir((fixture.dir.path + "/raft.state.tmp").c_str(),0700) == 0);
    fixture.ports[2].release();
    CHECK(worker.start(error));
    raft::AppendEntriesRequest request;
    request.term = 99; request.leader_id = 1;
    request.entries.push_back(raft::LogEntry{0,99,"must-not-ack"});
    CHECK(raft::encode_append_entries(request,body,error));
    RawPeer peer(fixture.ports[2].number);
    peer.send_all(incoming(1,RpcType::AppendEntries,body));
    CHECK(wait_for([&] { return !worker.status().running; }));
    CHECK(worker.status().term == 99);
    CHECK(worker.status().error.find("raft.state.tmp") != std::string::npos);
    const auto received = peer.until_closed();
    std::size_t at = 0;
    while(at < received.size())
    {
        std::size_t size = 0;
        CHECK(raft::peek_frame_size(received.data() + at,received.size() - at,size,error));
        CHECK(size <= received.size() - at);
        RpcType type = RpcType::Hello;
        std::string frame_body;
        CHECK(raft::decode_frame(received.data() + at,size,type,frame_body,error));
        CHECK(type == RpcType::Hello);
        at += size;
    }
    worker.stop();
    const auto state = disk_state(fixture.dir.path);
    CHECK(state.current_term == 0 && state.voted_for == 0);
    CHECK(std::filesystem::file_size(fixture.dir.path + "/raft.log") == 0);
    std::cout << "PASS worker live: failed follower state save releases neither append reply nor log\n";
}

// 反复启动/停止之后检查 fd 是否增长；先运行一轮排除运行库首次初始化。
std::size_t fd_count()
{
    DIR* directory = ::opendir("/proc/self/fd");
    CHECK(directory != nullptr);
    std::size_t count = 0;
    while(const dirent* entry = ::readdir(directory)) if(entry->d_name[0] != '.') ++count;
    CHECK(::closedir(directory) == 0);
    return count;
}

//同一条 TCP 上超时换编号；旧普通回复不能消费新请求，更高任期仍须处理。
void test_rpc_timeout_and_late_replies()
{
    Fixture fixture;
    std::string error;
    {
        raft::StateStore state(fixture.dir.path);
        CHECK(state.save(raft::HardState{1,0},error));
        raft::LogStore log(fixture.dir.path);
        CHECK(log.create(error));
        CHECK(log.append_batch({{1,1,"one"},{2,1,"two"},{3,1,"three"}},error));
    }
    RaftWorker worker(fixture.config(3));
    CHECK(worker.init(error));
    fixture.ports[2].release();
    CHECK(worker.start(error));
    RawPeer peer(fixture.ports[2].number);
    std::string hello_body,hello;
    CHECK(raft::encode_hello(1,hello_body,error));
    CHECK(raft::encode_frame(RpcType::Hello,hello_body,hello,error));
    peer.send_all(hello);

    RpcType type = RpcType::Hello;
    std::uint64_t id = 0;
    std::string body;
    CHECK(peer.receive_packet(type,id,body,3000) && type == RpcType::Hello);
    CHECK(peer.receive_packet(type,id,body,3000) && type == RpcType::VoteRequest);
    raft::RequestVoteRequest vote;
    CHECK(raft::decode_vote_request(body,vote,error));
    const auto vote_id = id;

    //回包仍经正式 codec/socket，Worker 自己判断编号和计票。
    const auto reply = [&](RpcType rpc_type,std::uint64_t rpc_id,const std::string& payload)
    {
        std::string frame;
        CHECK(raft::encode_rpc_frame(rpc_type,rpc_id,payload,frame,error));
        peer.send_all(frame);
    };
    raft::RequestVoteResponse granted{vote.term,true};
    CHECK(raft::encode_vote_response(granted,body,error));
    reply(RpcType::VoteResponse,vote_id + 10000,body);
    CHECK(!peer.receive_packet(type,id,body,150));
    CHECK(worker.status().role == raft::Role::Candidate);
    CHECK(raft::encode_vote_response(granted,body,error));
    reply(RpcType::VoteResponse,vote_id,body);
    CHECK(wait_for([&] { return worker.status().role == raft::Role::Leader; }));

    CHECK(peer.receive_packet(type,id,body,2000) && type == RpcType::AppendEntries);
    const auto old_id = id;
    raft::AppendEntriesRequest original;
    CHECK(raft::decode_append_entries(body,original,error));
    //选主后立即提出本任期 no-op，所以这次复制带上第 4 条
    CHECK(original.prev_log_index >= 3 && original.term > 0);

    //不回复，等待同一连接上的超时替换请求。
    CHECK(peer.receive_packet(type,id,body,2000) && type == RpcType::AppendEntries);
    const auto replacement_id = id;
    CHECK(replacement_id > old_id);
    raft::AppendEntriesRequest replacement;
    CHECK(raft::decode_append_entries(body,replacement,error));
    CHECK(replacement.term == original.term &&
          replacement.prev_log_index == original.prev_log_index);
    raft::AppendEntriesResponse nack{original.term,false};
    CHECK(raft::encode_append_response(nack,body,error));
    reply(RpcType::AppendResponse,old_id,body);
    CHECK(!peer.receive_packet(type,id,body,150));

    //新编号的 NACK 才能后退，证明迟到旧 NACK 未消费新在途上下文。
    CHECK(raft::encode_append_response(nack,body,error));
    reply(RpcType::AppendResponse,replacement_id,body);
    CHECK(peer.receive_packet(type,id,body,1000) && type == RpcType::AppendEntries);
    raft::AppendEntriesRequest backed_off;
    CHECK(raft::decode_append_entries(body,backed_off,error));
    //新编号的 NACK 才会触发后退；具体退多少由 Core 决定
    CHECK(backed_off.prev_log_index < original.prev_log_index);
    raft::AppendEntriesResponse late_success{original.term,true};
    CHECK(raft::encode_append_response(late_success,body,error));
    reply(RpcType::AppendResponse,old_id,body);
    CHECK(!peer.receive_packet(type,id,body,220));

    //同一个旧编号携带更高任期，仍必须持久化退位。
    late_success.term = 77;
    CHECK(raft::encode_append_response(late_success,body,error));
    reply(RpcType::AppendResponse,old_id,body);
    CHECK(wait_for([&]
    {
        const auto status = worker.status();
        return status.running && status.term == 77 && status.role == raft::Role::Follower;
    }));
    worker.stop();
    const auto saved = disk_state(fixture.dir.path);
    CHECK(saved.current_term == 77 && saved.voted_for == 0);
    CHECK(worker.status().commit == 0 && worker.status().last == 4);
    std::cout << "PASS worker live: vote IDs, long-connection timeout replacement, late NACK/success and higher-term stepdown\n";
}

void test_repeated_thread_cleanup()
{
    Fixture fixture;
    fixture.ports[0].release();
    const auto once = [&]
    {
        RaftWorker worker(fixture.config(1,true));
        std::string error;
        CHECK(worker.init(error) && worker.start(error));
        worker.stop();
    };
    once();
    const auto before = fd_count();
    for(int i = 0; i < 8; ++i) once();
    CHECK(fd_count() == before);
    std::cout << "PASS worker live: repeated thread/listener shutdown has no fd growth\n";
}
}

int main()
{
    test_single_node_and_snapshot_readers();
    test_listener_failure();
    test_poll_failure_stops_worker();
    test_higher_term_without_rpc_context();
    test_bad_rpc_does_not_poison_core();
    test_self_vote_save_failure();
    test_follower_save_failure_no_ack();
    test_repeated_thread_cleanup();
    test_rpc_timeout_and_late_replies();
    std::cout << "raft worker live PASS (actual threads and TCP; no KV apply)\n";
}
