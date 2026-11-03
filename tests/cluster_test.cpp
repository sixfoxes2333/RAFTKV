// 三进程集群验收（纯 C++）：选主、经 Raft 提交的 SET/GET、SIGKILL leader 后仍可读。
//
// 只做端到端必须的那几件事：拉起真实 kvserver 进程、按协议说话、观察状态行。
// 不做网络故障注入、不做磁盘故障注入——那些需要外部注入器，本测试不假装覆盖。
#include <arpa/inet.h>
#include <cerrno>
#include <chrono>
#include <csignal>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <functional>
#include <iostream>
#include <map>
#include <netinet/in.h>
#include <sstream>
#include <string>
#include <sys/prctl.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

#define CHECK(x) do { if(!(x)) { std::cerr << __FILE__ << ':' << __LINE__ \
    << ": FAIL " #x << '\n'; std::exit(1); } } while(false)

namespace
{

using Clock = std::chrono::steady_clock;

constexpr int SET = 1;
constexpr int GET = 2;
constexpr int STATUS_OK = 0;
constexpr int STATUS_NOT_FOUND = 1;
constexpr int STATUS_ERROR = 2;

bool wait_until(const std::function<bool()>& predicate,int milliseconds)
{
    const auto deadline = Clock::now() + std::chrono::milliseconds(milliseconds);

    do
    {
        if(predicate()) return true;
        std::this_thread::sleep_for(std::chrono::milliseconds(20));
    }
    while(Clock::now() < deadline);

    return predicate();
}

//绑到 0 号端口拿一个当前空闲的端口，再关掉交给子进程
std::uint16_t free_port()
{
    const int fd = ::socket(AF_INET,SOCK_STREAM | SOCK_CLOEXEC,0);
    CHECK(fd >= 0);

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    CHECK(::bind(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address)) == 0);

    socklen_t length = sizeof(address);
    CHECK(::getsockname(fd,reinterpret_cast<sockaddr*>(&address),&length) == 0);
    const std::uint16_t port = ntohs(address.sin_port);
    ::close(fd);
    return port;
}

std::string read_file(const std::string& path)
{
    std::ifstream in(path,std::ios::binary);
    return std::string(std::istreambuf_iterator<char>(in),{});
}

struct Node
{
    int id = 0;
    std::uint16_t client_port = 0;
    std::uint16_t raft_port = 0;
    std::string directory;
    std::string log_path;
    pid_t pid = -1;
};

//解析子进程状态行，取当前这一轮启动的 role/pid
struct Sample
{
    bool valid = false;
    std::string role;
    int pid = 0;
};

Sample parse_status(const std::string& text,int node_id)
{
    Sample sample;
    std::istringstream stream(text);
    std::string line;

    while(std::getline(stream,line))
    {
        if(line.rfind("RAFTSTATUS ",0) != 0) continue;

        std::map<std::string,std::string> fields;
        std::istringstream parts(line.substr(11));
        std::string token;

        while(parts >> token)
        {
            const auto equal = token.find('=');

            if(equal != std::string::npos)
            {
                fields[token.substr(0,equal)] = token.substr(equal + 1);
            }
        }

        if(fields["id"] != std::to_string(node_id)) continue;
        if(fields["running"] != "1") continue;

        sample.valid = true;
        sample.role = fields["role"];
        sample.pid = std::atoi(fields["pid"].c_str());
    }

    return sample;
}

bool process_alive(pid_t pid)
{
    if(pid <= 0) return false;
    return ::kill(pid,0) == 0 || errno == EPERM;
}

//一条阻塞客户端连接，请求/回复都按 4 字节长度头走
struct Client
{
    int fd = -1;

    bool connect_to(std::uint16_t port)
    {
        fd = ::socket(AF_INET,SOCK_STREAM | SOCK_CLOEXEC,0);
        if(fd < 0) return false;

        sockaddr_in address{};
        address.sin_family = AF_INET;
        address.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        address.sin_port = htons(port);

        timeval timeout{5,0};
        ::setsockopt(fd,SOL_SOCKET,SO_RCVTIMEO,&timeout,sizeof(timeout));
        ::setsockopt(fd,SOL_SOCKET,SO_SNDTIMEO,&timeout,sizeof(timeout));

        if(::connect(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address)) != 0)
        {
            ::close(fd);
            fd = -1;
            return false;
        }

        return true;
    }

    bool read_exact(char* data,std::size_t len)
    {
        std::size_t done = 0;

        while(done < len)
        {
            const ssize_t n = ::recv(fd,data + done,len - done,0);

            if(n <= 0) return false;

            done += static_cast<std::size_t>(n);
        }

        return true;
    }

    //返回 kv::Status，value 带出 GET 的结果
    int call(int op,const std::string& key,const std::string& value,std::string& out)
    {
        std::string body;
        body.push_back(static_cast<char>(op));

        for(auto number : {static_cast<std::uint32_t>(key.size()),
                           static_cast<std::uint32_t>(value.size())})
        {
            for(int shift = 24;shift >= 0;shift -= 8)
            {
                body.push_back(static_cast<char>((number >> shift) & 0xFF));
            }
        }

        body += key;
        body += value;

        std::string frame;
        const auto length = static_cast<std::uint32_t>(body.size());

        for(int shift = 24;shift >= 0;shift -= 8)
        {
            frame.push_back(static_cast<char>((length >> shift) & 0xFF));
        }

        frame += body;

        if(::send(fd,frame.data(),frame.size(),MSG_NOSIGNAL) !=
           static_cast<ssize_t>(frame.size()))
        {
            return STATUS_ERROR;
        }

        char header[4];

        if(!read_exact(header,sizeof(header))) return STATUS_ERROR;

        std::uint32_t reply = 0;

        for(unsigned char byte : header) reply = (reply << 8) | byte;

        std::string payload(reply,'\0');

        if(!read_exact(payload.data(),payload.size())) return STATUS_ERROR;

        const auto status = static_cast<unsigned char>(payload[0]);
        std::uint32_t value_len = 0;

        for(int i = 1;i < 5;++i)
        {
            value_len = (value_len << 8) | static_cast<unsigned char>(payload[static_cast<std::size_t>(i)]);
        }

        out = payload.substr(5,value_len);
        return status;
    }

    void close()
    {
        if(fd >= 0)
        {
            ::close(fd);
            fd = -1;
        }
    }
};

struct Cluster
{
    std::string root;
    std::string binary;
    std::vector<Node> nodes;
    std::vector<pid_t> children;

    Cluster(const std::string& binary_path,const std::string& root_path)
        :root(root_path),binary(binary_path)
    {
        nodes.resize(3);

        for(int i = 0;i < 3;++i)
        {
            Node& node = nodes[static_cast<std::size_t>(i)];
            node.id = i + 1;
            node.client_port = free_port();
            node.raft_port = free_port();
            node.directory = root + "/n" + std::to_string(node.id);
            node.log_path = node.directory + "/server.log";
            CHECK(::mkdir(node.directory.c_str(),0700) == 0);
        }
    }

    std::string peers() const
    {
        std::string text;

        for(const auto& node : nodes)
        {
            if(!text.empty()) text += ',';
            text += std::to_string(node.id) + "@127.0.0.1:" + std::to_string(node.raft_port);
        }

        return text;
    }

    bool start(std::size_t index)
    {
        Node& node = nodes[index];
        const pid_t pid = ::fork();
        CHECK(pid >= 0);

        if(pid == 0)
        {
            const int fd = ::open(node.log_path.c_str(),O_WRONLY | O_CREAT | O_TRUNC,0600);

            if(fd >= 0)
            {
                ::dup2(fd,STDOUT_FILENO);
                ::dup2(fd,STDERR_FILENO);
                ::close(fd);
            }

            //父进程（测试）一旦退出，节点必须跟着退出，否则会留下监听端口
            ::prctl(PR_SET_PDEATHSIG,SIGKILL);
            if(::getppid() == 1) ::_exit(0);

            ::setenv("KV_PORT",std::to_string(node.client_port).c_str(),1);
            ::setenv("KV_RAFT_ID",std::to_string(node.id).c_str(),1);
            ::setenv("KV_RAFT_PEERS",peers().c_str(),1);
            ::setenv("KV_RAFT_DATA",node.directory.c_str(),1);

            ::execl(binary.c_str(),binary.c_str(),node.log_path.c_str(),static_cast<char*>(nullptr));
            ::_exit(127);
        }

        node.pid = pid;
        children.push_back(pid);
        instance_children = children;
        return true;
    }

    std::string text(std::size_t index) const
    {
        return read_file(nodes[index].log_path);
    }

    //必须看到当前子进程打印的 READY，避免把上一轮日志当成就绪
    bool ready(std::size_t index) const
    {
        const auto needle = "READY id=" + std::to_string(nodes[index].id) +
                            " pid=" + std::to_string(nodes[index].pid);
        return ready_started && text(index).find(needle) != std::string::npos;
    }

    Sample sample(std::size_t index) const
    {
        return parse_status(text(index),nodes[index].id);
    }

    //只看活着的节点：死掉的节点日志里还留着旧的 leader 行，不能算数
    int leader(int milliseconds)
    {
        const bool ok = wait_until([&]
        {
            int alive = 0;
            int leaders = 0;

            for(std::size_t i = 0;i < nodes.size();++i)
            {
                if(!process_alive(nodes[i].pid)) continue;

                ++alive;

                const auto current = sample(i);

                if(!current.valid) return false;
                if(current.role == "leader") ++leaders;
            }

            return alive >= 2 && leaders == 1;
        },milliseconds);

        if(!ok) return 0;

        for(std::size_t i = 0;i < nodes.size();++i)
        {
            if(!process_alive(nodes[i].pid)) continue;
            if(sample(i).role == "leader") return nodes[i].id;
        }

        return 0;
    }

    std::size_t index_of(int node_id) const
    {
        for(std::size_t i = 0;i < nodes.size();++i)
        {
            if(nodes[i].id == node_id) return i;
        }

        return 0;
    }

    void wait_ready(int milliseconds)
    {
        ready_started = true;
        CHECK(wait_until([&]
        {
            for(std::size_t i = 0;i < nodes.size();++i)
            {
                if(!ready(i)) return false;
            }

            return true;
        },milliseconds));
    }

    void stop_all()
    {
        for(const auto pid : children)
        {
            if(process_alive(pid)) ::kill(pid,SIGTERM);
        }

        //给正常停机一点时间，超时就强杀，绝不留着进程退出
        const auto deadline = Clock::now() + std::chrono::seconds(3);
        bool pending = true;

        while(pending && Clock::now() < deadline)
        {
            pending = false;

            for(const auto pid : children)
            {
                if(::waitpid(pid,nullptr,WNOHANG) == 0) pending = true;
            }

            if(pending) std::this_thread::sleep_for(std::chrono::milliseconds(20));
        }

        for(const auto pid : children)
        {
            if(process_alive(pid)) ::kill(pid,SIGKILL);
            ::waitpid(pid,nullptr,0);
        }

        children.clear();
        instance_children.clear();
    }

    bool ready_started = false;

    //atexit 兜底用：当前还登记着的子进程
    static std::vector<pid_t> live_children()
    {
        return instance_children;
    }

    static inline std::vector<pid_t> instance_children;
};

//等待某端口能接受连接；leader 刚换人时客户端会拿到 Error，所以只探活
bool port_open(std::uint16_t port)
{
    Client client;

    if(!client.connect_to(port)) return false;

    client.close();
    return true;
}

const std::string kBinary = "/home/sixfoxes/projects/KV/build/kvserver";

}

int main(int argc,char** argv)
{
    const std::string binary = (argc > 1) ? argv[1] : kBinary;
    CHECK(::access(binary.c_str(),X_OK) == 0);

    char root_template[] = "/tmp/kv-cluster-test.XXXXXX";
    CHECK(::mkdtemp(root_template) != nullptr);

    Cluster cluster(binary,root_template);

    //CHECK 走的是 std::exit，不会调用局部对象析构；这里兜底清理子进程
    std::atexit([]
    {
        for(const auto pid : Cluster::live_children())
        {
            if(process_alive(pid)) ::kill(pid,SIGKILL);
            ::waitpid(pid,nullptr,0);
        }
    });

    //先起三个节点，等它们都完成启动握手
    for(std::size_t i = 0;i < cluster.nodes.size();++i) cluster.start(i);
    cluster.wait_ready(10000);

    const int first_leader = cluster.leader(20000);
    CHECK(first_leader != 0);
    std::cout << "PASS cluster: three real processes elected one leader (node "
              << first_leader << ")\n";

    //经 Raft 提交一次写，再从同一个 leader 读回来
    const auto leader_index = cluster.index_of(first_leader);
    CHECK(port_open(cluster.nodes[leader_index].client_port));

    Client client;
    CHECK(client.connect_to(cluster.nodes[leader_index].client_port));

    std::string value;
    int status = STATUS_ERROR;

    CHECK(wait_until([&]
    {
        status = client.call(SET,"name","sixfoxes",value);
        return status == STATUS_OK;
    },20000));

    CHECK(client.call(GET,"name","",value) == STATUS_OK);
    CHECK(value == "sixfoxes");
    std::cout << "PASS cluster: SET/GET committed through Raft and served by the leader\n";

    //杀掉 leader：已确认的写必须还在
    const pid_t killed = cluster.nodes[leader_index].pid;
    ::kill(killed,SIGKILL);
    ::waitpid(killed,nullptr,0);

    const int next_leader = cluster.leader(25000);
    CHECK(next_leader != 0);
    CHECK(next_leader != first_leader);

    Client restarted;
    CHECK(restarted.connect_to(cluster.nodes[cluster.index_of(next_leader)].client_port));

    CHECK(wait_until([&]
    {
        status = restarted.call(GET,"name","",value);
        return status == STATUS_OK;
    },20000));
    CHECK(value == "sixfoxes");
    std::cout << "PASS cluster: SIGKILL leader; new leader served the confirmed write\n";

    restarted.close();
    client.close();
    cluster.stop_all();

    std::cout << "cluster PASS (3 real kvserver processes; no injected faults)\n";
    return 0;
}
