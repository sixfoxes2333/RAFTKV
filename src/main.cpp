#include "net/reactor.h"
#include "app/storage_worker.h"
#include "net/protocol.h"
#include "raft/worker.h"
#include<sys/socket.h>
#include<netinet/in.h>
#include<netinet/tcp.h>
#include<fcntl.h>
#include<cerrno>
#include<iostream>
#include<cstring>
#include<arpa/inet.h>
#include<memory>
#include<cstdlib>
#include <charconv>
#include <pthread.h>
#include <signal.h>
#include <sys/signalfd.h>
#include <sys/timerfd.h>


int g_idle_fd = -1;//预留占位 fd

bool g_fatal = false; //全局致命标志

raft::RaftWorker *g_raft = nullptr; //Raft 模式的后台线程入口
kv::StorageWorker *g_worker = nullptr; //后台存储线程 Reactor 投递任务

bool g_raft_mode = false; //启动时确定，运行期间不切换

uint64_t g_next_sequence = 0; //任务序号

const int MAX_MESSAGE = 16 * 1024 *1024; //防止恶意长度炸内存

//设置非阻塞模式
bool setnonblocking(int fd)
{
    int old = fcntl(fd,F_GETFL);

    if(old < 0)
    {
        return false;
    }
    if(fcntl(fd,F_SETFL,old | O_NONBLOCK))
    {
        return false;
    }

    return true;
}

void update_out_watermark(Connection* c);

//短写续写
void handle_write(Connection* c)
{
    while(c->out_.readable() > 0)
    {   //send 返回成功写入的字节数
        //MSG_NOSIGNAL 对端已关闭时不产生 SIGPIPE (不然进程会被默认信号杀掉)
        ssize_t n = send(c->fd(),c->out_.peek(),c->out_.readable(),MSG_NOSIGNAL);
        if(n > 0)
        {
            c->out_.retrieve(n);
        }
        else if(n < 0 && errno == EINTR)
        {
            //被信号打断 什么都没写，重试即可
            continue;
        }
        else if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            //缓冲区满，等待 EPOLLOUT
            c->enable_write();
            return;
        }
        else if(n == 0) //理论上不会发生
        {
            return;
        }
        else
        {
            //真错误
            c->reactor()->request_close(c);
            return;
        }
    }

    update_out_watermark(c);

    if(c->peer_closed() && c->pending_requests() == 0)
    {
        c->reactor()->request_close(c); //对端不会再发，则本体也不再发送
        return;
    }

    c->disable_write(); //掩码回到 EPOLLIN | EPOLLRDHUP

}

//待发队列水位高则暂停读，低则恢复读
void update_out_watermark(Connection* c)
{
    if(c->peer_closed())
    {
        return; //读方向关
    }

    if(!c->read_paused() && c->out_.readable() >= OUT_HIGH_WATER)
    {
        c->pause_read(); //停止接收新请求
    }
    else if(c->read_paused() && c->out_.readable() <= OUT_LOW_WATER)
    {
        c->resume_read();
    }

}


//发送一条响应
void send_response(Connection* c,const kv::Response& resp)
{
    std::string body = kv::encode_response(resp);

    //上限 对端一直不读，待发数据无界增长
    if(c->out_.readable() + 4 + body.size() > OUT_HARD_CAP)
    {
        std::cerr << "send queue overflow fd = " << c->fd() << std::endl;
        c->reactor()->request_close(c);
        return;
    }

    uint32_t be = htonl(static_cast<uint32_t>(body.size()));

    c->out_.append(reinterpret_cast<const char*>(&be),4);
    c->out_.append(body.data(),body.size());

    update_out_watermark(c);
    handle_write(c);
}

//错误响应
void send_error(Connection* c)
{
    kv::Response resp;
    resp.status = kv::Status::Error;
    send_response(c,resp);
}


//处理消息
void handle_message(Connection *c , const char *payload , size_t len)
{
    if(g_raft_mode)
    {
        //Raft 模式：请求先排队，由后台提出日志；结果经 eventfd 回来
        kv::Request parsed;

        if(!kv::parse_request(payload,len,parsed))
        {
            //解析失败当场回；若还有在途请求，顺序会被打乱，直接断开
            send_error(c);

            if(c->pending_requests() > 0)
            {
                c->reactor()->request_close(c);
            }

            return;
        }

        raft::RaftWorker::ClientRequest task;
        task.fd = c->fd();
        task.connection_id = c->connection_id();
        task.sequence = ++g_next_sequence;
        task.bytes = parsed.key.size() + parsed.value.size();
        task.request = std::move(parsed);

        if(c->inflight_bytes() + task.bytes > kv::MAX_CONN_QUEUE_BYTES)
        {
            send_error(c);
            c->reactor()->request_close(c);
            return;
        }

        if(!g_raft->submit_client(std::move(task)))
        {
            if(c->pending_requests() > 0)
            {
                send_error(c);
                c->reactor()->request_close(c);
                return;
            }

            send_error(c);
            return;
        }

        c->add_pending_request();
        c->add_inflight_bytes(len);
        return;
    }

    kv::Request req;


    kv::StorageTask task;
    task.fd = c->fd(); //回信地址
    task.connection_id = c->connection_id(); //连接代数
    task.sequence = ++g_next_sequence; //投递序号
    
    task.bytes = len;

    //解析失败，占位
    if(!kv::parse_request(payload,len,req))
    {
        task.parse_failed = true;
    }
    else
    {
        task.bytes = req.key.size() + req.value.size();
        task.request = std::move(req);
    }


    //单连接字节预算
    if(c->inflight_bytes() + task.bytes > kv::MAX_CONN_QUEUE_BYTES)
    {
        send_error(c);
        c->reactor()->request_close(c);
        return;
    }


    //投递失败
    if(!g_worker->submit(std::move(task)))
    {
        if(c->pending_requests() > 0)
        {
            send_error(c);
            c->reactor()->request_close(c);
            return;
        }

        send_error(c);
        return;
    }

    c->add_pending_request();
    c->add_inflight_bytes(task.bytes);

}


//检测切分
void process_frames(Connection *c)
{
    while(c->in_.readable() >= 4) //长度头就占四字节
    {
        uint32_t len;
        memcpy(&len,c->in_.peek(),4); //取前 4 字节
        len = ntohl(len); //大端转换为主机序

        //恶意长度
        if(len > MAX_MESSAGE)
        {
            std::cout << "bad len " << len << " from fd = " << c->fd() << std::endl;
            c->reactor()->request_close(c);
            return;
        }
        
        //半包 等待下次事件
        if(c->in_.readable() < 4 + len)
        {
            break;
        }

        //完整一条消息
        handle_message(c,c->in_.peek() + 4,len);

        //回调可能已申请关闭 (比如 发送失败) 
        if(c->closing())
        {
            return;
        }

        c->in_.retrieve(4+len);
        
    }
}



//读侧回调
void handle_read(Connection* c)
{
    char buf[64*1024]; //一次读 64 KB

    //把内核接收缓冲读空 (读到 EAGAIN 为止)
    while(true)
   { 
        //read 返回实际读取字节数
        ssize_t len = read(c->fd(),buf,sizeof(buf));
        if(len > 0)
        {
            if(c->reactor()->in_bytes() + static_cast<size_t>(len) > kv::MAX_IN_BUFFER_BYTES)
            {
                std::cerr << "in buffer budget exceeded (" << c->reactor()->in_bytes() << " + " << len << " > " << kv::MAX_IN_BUFFER_BYTES << "),close fd = " << c->fd() << std::endl;
                c->reactor()->request_close(c);
                return;
            }

            c->in_.append(buf,len);
            c->reactor()->add_in_bytes(static_cast<size_t>(len));
            c->touch_read_progress();

            size_t before = c->in_.readable();

            //检测切分
            process_frames(c);

            c->reactor()->release_in_bytes(before - c->in_.readable());
            
            if(c->closing())
            {
                return;
            }

            continue;
        }
        else if(len == 0)
        {
            std::cout << "\n peer closed fd = " << c->fd() << " \n" <<std::endl; //对端半关闭，不再有新数据
            
            c->mark_peer_closed();

            //没有待发数据
            if(c->out_.readable() == 0 && c->pending_requests() == 0)
            {
                c->reactor()->request_close(c);
            }
            else
            {
                //还有回复未发送
                //关掉读，再等 EPOLLOUT 把数据发完
                c->close_read();

                //有数据要发才关注可写
                if(c->out_.readable() > 0)
                {
                    c->enable_write();
                }
            }

            return;
            
        }
        else
        {
            if(errno == EINTR)
            {
                continue;
            }
            if(errno == EAGAIN || errno == EWOULDBLOCK)
            {
                return;
            }
            c->reactor()->request_close(c);
            return;
        }

    }
}

//建档回调
void handle_accept(Connection *lc)
{
    struct sockaddr_in client;

    while(true)
    {
        socklen_t len = sizeof(client);

        int connfd = accept4(lc->fd(),(struct sockaddr*)&client,&len,SOCK_NONBLOCK | SOCK_CLOEXEC);

        if(connfd < 0)
        {
            if(errno == EINTR)
            {
                continue;
            }

            if(errno == EAGAIN || errno == EWOULDBLOCK)
            {
                break;
            }

            if(errno == EMFILE || errno == ENFILE)
            {
                //fd 用完了，不管的话就会一直空转
                //用预留的 fd 腾一个位置
                //取出队首连接并关闭，再重新占位
                std::cerr << "\n accept : out of file descriptors \n" << std::endl;

                close(g_idle_fd);
                int temp = accept4(lc->fd(),nullptr,nullptr,0);

                if(temp >= 0)
                {
                    close(temp);
                }

                g_idle_fd = open("/dev/null",O_RDONLY | O_CLOEXEC);

                break;
            }

            std::cerr << "\n accept failed : " << strerror(errno) << " \n" << std::endl;
            break;

        }

        // //测试注入 : 缩小发送缓冲，强制触发短写

        // int sndbuf = 4096;
        // setsockopt(connfd,SOL_SOCKET,SO_SNDBUF,&sndbuf,sizeof(sndbuf));

        // //测试完注释掉

        //关闭 nagle
        int nodelay = 1;
        if(setsockopt(connfd,IPPROTO_TCP,TCP_NODELAY,&nodelay,sizeof(nodelay)) < 0)
        {
            std::cerr << "setsockopt TCP_NODELAY failed : " << strerror(errno) << std::endl;
            close(connfd);
            continue;
        }

        auto c = std::make_unique<Connection>(connfd,lc->reactor());

        c->on_read = handle_read;
        c->on_write = handle_write;

        if(!lc->reactor()->add_connection(connfd,std::move(c),CONN_BASE_EVENTS))
        {
            std::cerr << "register connection fdd = " << connfd << "failed,drop it" << std::endl;
            continue;
        }
        
        std::cout << "\n new conn fd = " << connfd << " from " << inet_ntoa(client.sin_addr) << ":" << ntohs(client.sin_port) << " \n" << std::endl;
    }

    
}

//查连接 编码并回包
void handle_storage_events(Connection* w)
{
    auto done = g_worker->take_completed();
   
    for(auto &item : done)
    {
        //按照 fd 与 连接代数找回连接
        Connection* c = w->reactor()->find_connection(item.fd,item.connection_id);

        if(c == nullptr)
        {
            continue;
        }

        if(c->closing())
        {
            continue;
        }

        //减掉在途计数 再打包
        c->finish_pending_request();
        c->release_inflight_bytes(item.bytes); // 新增归还请求占的字节

        send_response(c,item.response);

    }

    //worker 不可用
    if(!g_worker->ready())
    {
        std::cerr << "storage worker unavailable,shutting down " << std::endl;
        g_fatal = true;
        w->reactor()->stop();
    }

}
//Raft 后台有结果：按连接身份和序号顺序回包
void handle_raft_events(Connection* w)
{
    auto done = g_raft->take_client_results();

    for(auto& item : done)
    {
        Connection* c = w->reactor()->find_connection(item.fd,item.connection_id);

        if(c == nullptr)
        {
            continue;
        }

        //先后退在途计数，再回包；顺序由后台按日志顺序产生
        c->finish_pending_request();
        c->release_inflight_bytes(item.bytes);

        if(c->closing())
        {
            continue;
        }

        send_response(c,item.response);
    }

    //后台已经退出：整个服务器不能继续假装可用
    const auto status = g_raft->status();

    if(!status.running)
    {
        std::cerr << "raft worker unavailable : " << status.error << std::endl;
        g_fatal = true;
        w->reactor()->stop();
    }
}


//main 启动时读取客户端端口，未配置则沿用 9006
bool load_client_port(std::uint16_t& out,std::string& error)
{
    error.clear();

    std::uint32_t value = 9006;

    if(const char* text = std::getenv("KV_PORT"))
    {
        const char* end = text + std::strlen(text);
        const auto result = std::from_chars(text,end,value);

        if(result.ec != std::errc{} || result.ptr != end || value == 0 || value > 65535)
        {
            error = "invalid KV_PORT (expect 1..65535)";
            return false;
        }
    }

    out = static_cast<std::uint16_t>(value);
    return true;

}

//输出状态行时，把 core 的角色转换为可识别脚本
const char* raft_role_name(raft::Role role)
{
    switch(role)
    {
        case raft::Role::Follower: return "follower";
        case raft::Role::Candidate: return "candidate";
        case raft::Role::Leader: return "leader";
    }

    return "unknown";
}

//主线程输出后台快照
void print_raft_status(std::uint64_t id,const raft::RaftWorker::Status& status)
{
    std::cout << "RAFTSTATUS id=" << id
                << " role=" << raft_role_name(status.role)
                << " term=" << status.term
                << " commit=" << status.commit
                << " last=" << status.last
                << " applied=" << status.applied
                << " pid=" << ::getpid()
                << " running=" << (status.running ? 1 : 0)
                << " updated_ms=" << status.updated_ms
                << " serving=" << (status.serving ? 1 : 0)
                << std::endl;
}

//main 注册控制 fd ，信号使 reactor 退出，定时器检查 raft 后台状态
bool install_control_events(Reactor& reactor,const sigset_t& signals,raft::RaftWorker* worker,std::uint64_t id,std::string& error)
{
    error.clear();

    int fd = signalfd(-1,&signals,SFD_NONBLOCK | SFD_CLOEXEC);

    if(fd < 0)
    {
        error = std::string("signalfd : ") + std::strerror(errno);
        return false;
    }

    auto signal_conn = std::make_unique<Connection>(fd,&reactor);

    //信号已被阻塞，由主线程通过 signalfd 读取并停止事件循环
    signal_conn->on_read = [](Connection* c)
    {
        signalfd_siginfo info{};
        ssize_t n;
        do
        {
            n = ::read(c->fd(),&info,sizeof(info));
        }
        while(n < 0 && errno == EINTR);
        
        if(n == static_cast<ssize_t>(sizeof(info)))
        {
            c->reactor()->stop();
            return;
        }

        if(n < 0 && errno == EAGAIN)
        {
            return;
        }

        std::cerr << "signalfd read failed" << std::endl;
        g_fatal = true;
        c->reactor()->stop();
    };

    if(!reactor.add_connection(fd,std::move(signal_conn),EPOLLIN))
    {
        error = "register signal fd failed";
        return false;
    }

    if(worker == nullptr)
    {
        return true;
    }

    fd = timerfd_create(CLOCK_MONOTONIC,TFD_NONBLOCK | TFD_CLOEXEC);

    if(fd < 0)
    {
        error = std::string("timefd_create : ") + std::strerror(errno);
        return false;
    }

    auto timer_conn = std::make_unique<Connection>(fd,&reactor);

    itimerspec timer{};
    timer.it_value.tv_nsec = 200 * 1000 * 1000;
    timer.it_interval = timer.it_value;

    if(timerfd_settime(fd,0,&timer,nullptr) < 0)
    {
        error = std::string("timerfd_settime : ") + std::strerror(errno);
        return false;
    }

    //每 200 ms 读取一次加锁快照
    timer_conn->on_read = [worker,id](Connection* c)
    {
        std::uint64_t ticks = 0;
        ssize_t n;
        do
        {
            n = ::read(c->fd(),&ticks,sizeof(ticks));
        }
        while(n < 0 && errno == EINTR);

        if(n < 0 && errno == EAGAIN)
        {
            return;
        }

        if(n != static_cast<ssize_t>(sizeof(ticks)))
        {
            std::cerr << "timerfd read failed" << std::endl;
            g_fatal = true;
            c->reactor()->stop();
            return;
        }

        const auto status = worker->status();
        print_raft_status(id,status);

        if(!status.running)
        {
            std::cerr << "raft worker unavailable : " << status.error << std::endl;
            g_fatal = true;
            c->reactor()->stop();
        }

    };

    if(!reactor.add_connection(fd,std::move(timer_conn),EPOLLIN))
    {
        error = "register raft timer fd failed";
        return false;
    }

    return true;

}



int main(int argc,char* argv[])
{
    std::string error;
    std::uint16_t client_port = 9006;


    if(!load_client_port(client_port,error))
    {
        std::cerr << error << std::endl;
        return 1;
    }

    //出现任意 raft 配置则检查全部必填项
    g_raft_mode = std::getenv("KV_RAFT_ID") != nullptr || std::getenv("KV_RAFT_PEERS") != nullptr || std::getenv("KV_RAFT_DATA") != nullptr;

    raft::RaftConfig raft_config;

    if(g_raft_mode)
    {
        if(!raft::load_raft_config_from_env(raft_config,error))
        {
            std::cerr << error << std::endl;
            return 1;
        }

        if(client_port == raft_config.self.port)
        {
            std::cerr << "client port equals local raft port" << std::endl;
            return 1;
        }
    }

    //必须在线程创建前阻塞
    sigset_t signals;
    sigemptyset(&signals);
    sigaddset(&signals,SIGINT);
    sigaddset(&signals,SIGTERM);

    const int signal_error = pthread_sigmask(SIG_BLOCK,&signals,nullptr);

    if(signal_error != 0)
    {
        std::cerr << "pthread_sigmask : " << std::strerror(signal_error) << std::endl;
        return 1;
    }

    //后台对象先于 Reactor 创建，销毁时 Reacotr 先释放连接
    std::unique_ptr<kv::StorageWorker>storage_worker;
    std::unique_ptr<raft::RaftWorker>raft_worker;

    if(g_raft_mode)
    {
        raft_worker = std::make_unique<raft::RaftWorker>(raft_config);

        if(!raft_worker->init(error) || !raft_worker->start(error))
        {
            std::cerr << "raft startup failed : " << error << std::endl;
            return 1;
        }

        std::cout << "mode = raft " << std::endl;

        g_raft = raft_worker.get();
    }
    else
    {
        const char* db_path = (argc > 1) ? argv[1] : "kvdb.data";
        auto sync_mode  = kv::StorageWorker::SyncMode::Always;
        const char* sync_mode_name = "always";

        if(const char* p = std::getenv("KV_SYNC_MODE"))
        {
            const std::string mode = p;

            if(mode == "always")
            {
                sync_mode = kv::StorageWorker::SyncMode::Always;
            }
            else if(mode == "everysec")
            {
                sync_mode = kv::StorageWorker::SyncMode::Everysec;
                sync_mode_name = "everysec";
            }
            else if(mode == "off")
            {
                sync_mode = kv::StorageWorker::SyncMode::Off;
                sync_mode_name = "off";
            }
            else
            {
                std::cerr << "invalid KV_SYNC_MODE : " << mode << "(expect always/everysec/off)" << std::endl;
                return 1;
            }

        }

        std::cout << "sync mode" << sync_mode_name << std::endl;

        storage_worker = std::make_unique<kv::StorageWorker>(db_path,1024,sync_mode);

        if(!storage_worker->ready())
        {
            std::cerr << "storage worker init failed : " << db_path << std::endl;
            return 1;
        }

        g_worker = storage_worker.get();

    }

    //先交给 Connection 持有 fd

    Reactor reactor;

    int frame_timeout_ms = 30000;

    if(const char* p = std::getenv("KV_FRAME_TIMEOUT_MS"))
    {
        frame_timeout_ms = std::atoi(p);
    }

    reactor.set_frame_timeout_ms(frame_timeout_ms);

    const int listenfd = socket(AF_INET,SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,0);

    if(listenfd < 0)
    {
        std::cerr << "socket failed" << std::endl;

        return 1;
    }

    auto listener = std::make_unique<Connection>(listenfd,&reactor);

    int opt = 1;

    if(setsockopt(listenfd,SOL_SOCKET,SO_REUSEADDR,&opt,sizeof(opt)) < 0)
    {
        std::cerr << "setsockopt SO_REUSEADDR failed" << std::strerror(errno) << std::endl;
        return 1;
    }

    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = htonl(INADDR_ANY);
    addr.sin_port = htons(client_port);

    if(bind(listenfd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)) < 0)
    {
        std::cerr << "bind failed : " << std::strerror(errno) << std::endl;
        return 1;
    }

    if(listen(listenfd,SOMAXCONN) < 0)
    {
        std::cerr << "listen failed : " << std::strerror(errno) << std::endl;
        return 1;
    }

    listener->on_read = handle_accept;
    listener->on_write = nullptr;

    if(!reactor.add_connection(listenfd,std::move(listener),CONN_BASE_EVENTS))
    {
        std::cerr << "register listen fd failed,cannot serve" << std::endl;
        return 1;
    }


    //Raft 模式的完成通知：后台把结果经 eventfd 交给这个 Reactor
    if(raft_worker)
    {
        const int fd = fcntl(raft_worker->wakeup_fd(),F_DUPFD_CLOEXEC,0);

        if(fd < 0)
        {
            std::cerr << "dup raft wakeup fd failed : "
                      << std::strerror(errno) << std::endl;
            return 1;
        }

        auto raft_conn = std::make_unique<Connection>(fd,&reactor);
        raft_conn->on_read = handle_raft_events;
        raft_conn->on_write = nullptr;

        if(!reactor.add_connection(fd,std::move(raft_conn),EPOLLIN))
        {
            std::cerr << "register raft wakeup fd failed" << std::endl;
            return 1;
        }
    }


    if(storage_worker)
    {
        const int wake_fd = fcntl(storage_worker->wakeup_fd(),F_DUPFD_CLOEXEC,0);

        if(wake_fd < 0)
        {
            std::cerr << "dup wakeup fd failed : " << std::strerror(errno) << std::endl;
            return 1;
        }

        auto wake_conn = std::make_unique<Connection>(wake_fd,&reactor);
        wake_conn->on_read = handle_storage_events;
        wake_conn->on_write = nullptr;

        if(!reactor.add_connection(wake_fd,std::move(wake_conn),EPOLLIN))
        {
            std::cerr << "register wakeup fd failed,cannot reply" << std::endl;
            return 1;
        }
    }

    if(!install_control_events(reactor,signals,raft_worker.get(),raft_config.self_id,error))
    {
        std::cerr << error << std::endl;
        return 1;
    }

    g_idle_fd = open("/dev/null",O_RDONLY | O_CLOEXEC);

    if(g_idle_fd < 0)
    {
        std::cerr << "open /dev/null failed : " << std::strerror(errno) << std::endl;
        return 1;
    }

    if(raft_worker)
    {
        const auto status = raft_worker->status();

        if(!status.running)
        {
            std::cerr << "raft stpped before ready : " << status.error << std::endl;
            ::close(g_idle_fd);
            g_idle_fd = -1;
            return 1;
        }

        std::cout << "READY id=" << raft_config.self_id << " pid=" << ::getpid() << std::endl;

        print_raft_status(raft_config.self_id,status);
    }

    const bool ok = reactor.loop();

    //主线程 join ，避免后台仍然运行时释放其要使用的对象
    if(raft_worker)
    {
        raft_worker->stop();

        const auto status = raft_worker->status();
        if(!status.error.empty())
        {
            std::cerr << "raft shutdown : " << status.error << std::endl;
            g_fatal = true;
        }
    }

    ::close(g_idle_fd);
    g_idle_fd = -1;

    return (ok && !g_fatal) ? 0 : 1;
}