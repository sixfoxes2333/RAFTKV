#include "raft/transport.h"
#include "raft/rpc_codec.h"
#include "raft/rpc_validate.h"
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <utility>

namespace raft
{

namespace
{
    //断线后间隔多久回拨
    constexpr int DIAL_RETRY_MS = 500;

    //一帧最多多大
    constexpr std::size_t MAX_IN_BUFFER = RPC_HEADER_SIZE + MAX_RPC_PAYLOAD;

    //把一帧追加到待发缓冲
    bool queue_frame(RpcType type,const std::string& body,std::string& out,std::string& error)
    {
        std::string frame;

        if(!encode_frame(type,body,frame,error))
        {
            return false;
        }

        out += frame;
        return true;
    }

    bool fill_address(const std::string& host,std::uint16_t port,sockaddr_in& out)
    {
        std::memset(&out,0,sizeof(out));

        out.sin_family = AF_INET;
        out.sin_port = htons(port);

        //inet_pton 1 成功 0 非合法 -1 参数错误
        return ::inet_pton(AF_INET,host.c_str(),&out.sin_addr) == 1;
    }

    std::string sys_error(const std::string what)
    {
        return std::string("raft transport : ") + what + " failed : " + std::strerror(errno);
    }
}

RaftTransport::RaftTransport(const RaftConfig& config,MessageHandler on_message,PeerDownHandler on_peer_down) : config_(config),on_message_(std::move(on_message)),on_peer_down_(std::move(on_peer_down))
{

}

RaftTransport::~RaftTransport()
{
    stop();
}

bool RaftTransport::start(std::string& error)
{
    error.clear();

    if(running_)
    {
        error = "raft transport : already started";
        return false;
    }

    epfd_ = ::epoll_create1(EPOLL_CLOEXEC);

    if(epfd_ < 0)
    {
        error = sys_error("epoll_create1");
        return false;
    }

    if(!open_listener(error))
    {
        ::close(epfd_);
        epfd_ = -1;
        return false;
    }

    running_ = true;

    //启动先拨一轮
    dial_due_peers();
    return true;
}

bool RaftTransport::open_listener(std::string& error)
{
    listen_fd_ = ::socket(AF_INET,SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,0);

    if(listen_fd_ < 0)
    {
        error = sys_error("socket");
        return false;
    }

    int on = 1;

    if(::setsockopt(listen_fd_,SOL_SOCKET,SO_REUSEADDR,&on,sizeof(on)) < 0)
    {
        error = sys_error("SO_REUSEADDR");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    sockaddr_in addr;

    if(!fill_address(config_.self.host,config_.self.port,addr))
    {
        error = "raft transport : bad listen address : " + config_.self.host;
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if(::bind(listen_fd_,reinterpret_cast<sockaddr*>(&addr),sizeof(addr)) < 0)
    {
        error = sys_error("bind " + config_.self.host + ":" + std::to_string(config_.self.port));
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    if(::listen(listen_fd_,SOMAXCONN) < 0)
    {
        error = sys_error("listen");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    epoll_event event{};
    event.events = EPOLLIN;
    event.data.fd = listen_fd_;

    if(::epoll_ctl(epfd_,EPOLL_CTL_ADD,listen_fd_,&event) < 0)
    {
        error = sys_error("epoll_ctl add listener");
        ::close(listen_fd_);
        listen_fd_ = -1;
        return false;
    }

    return true;
}

void RaftTransport::dial_due_peers()
{
    const auto now = std::chrono::steady_clock::now();

    for(const auto & peer : config_.peers)
    {
        if(peer_fd_.count(peer.id) != 0)
        {
            continue;
        }

        if(peer.id < config_.self_id)
        {
            continue;
        }

        //若有出站连接在等握手，不重复拨
        bool in_flight = false;

        for(const auto& item:conns_)
        {
            if(!item.second.inbound && item.second.expect_peer == peer.id)
            {
                in_flight = true;
                break;
            }
        }

        if(in_flight)
        {
            continue;
        }

        const auto due = next_dial_.find(peer.id);

        if(due != next_dial_.end() && now < due->second)
        {
            continue;
        }

        dial_peer(peer);
    }
}

void RaftTransport::dial_peer(const PeerAddress& peer)
{
    sockaddr_in addr;

    if(!fill_address(peer.host,peer.port,addr))
    {
        next_dial_[peer.id] = std::chrono::steady_clock::now() + std::chrono::milliseconds(DIAL_RETRY_MS);
        return;
    }

    const int fd = ::socket(AF_INET,SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC,0);

    if(fd < 0)
    {
        next_dial_[peer.id] = std::chrono::steady_clock::now() + std::chrono::milliseconds(DIAL_RETRY_MS);
        return;
    }

    const int rc = ::connect(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr));

    Connection conn;
    conn.fd = fd;
    conn.inbound = false;
    conn.expect_peer = peer.id;

    if(rc == 0)
    {
        conns_.emplace(fd,std::move(conn));

        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = fd;

        if(::epoll_ctl(epfd_,EPOLL_CTL_ADD,fd,&event) < 0)
        {
            close_conn(fd,false);
            return;
        }

        send_hello(conns_.at(fd));
        return;
    }

    if(errno != EINPROGRESS)
    {
        ::close(fd);
        next_dial_[peer.id] = std::chrono::steady_clock::now() + std::chrono::milliseconds(DIAL_RETRY_MS);
        return;
    }

    conns_.emplace(fd,std::move(conn));

    epoll_event event{};
    event.events = EPOLLIN | EPOLLOUT;
    event.data.fd = fd;

    if(::epoll_ctl(epfd_,EPOLL_CTL_ADD,fd,&event) < 0)
    {
        ::close(fd);
        conns_.erase(fd);
        next_dial_[peer.id] = std::chrono::steady_clock::now() + std::chrono::milliseconds(DIAL_RETRY_MS);
    }
}

void RaftTransport::connect_ready(Connection& conn)
{
    int so_error = 0;
    socklen_t len = sizeof(so_error);

    if(::getsockopt(conn.fd,SOL_SOCKET,SO_ERROR,&so_error,&len) < 0 || so_error != 0)
    {
        close_conn(conn.fd,false);
        return;
    }


    const int fd = conn.fd;
    send_hello(conn);

    const auto it = conns_.find(fd);

    if(it == conns_.end())
    {
        return;
    }

    epoll_event event{};
    event.events = EPOLLIN;
    if(it->second.want_write)
    {
        event.events |= EPOLLOUT;
    }
    event.data.fd = it->first;

    if(::epoll_ctl(epfd_,EPOLL_CTL_MOD,it->first,&event) < 0)
    {
        close_conn(it->first,false);
    }

}

void RaftTransport::accept_ready()
{
    for(; ;)
    {
        sockaddr_in addr{};

        socklen_t len = sizeof(addr);

        const int fd = ::accept4(listen_fd_,reinterpret_cast<sockaddr*>(&addr),&len,SOCK_NONBLOCK | SOCK_CLOEXEC);

        if(fd < 0)
        {
            if(errno == EINTR)
            {
                continue;
            }

            return;
        }

        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = fd;

        if(::epoll_ctl(epfd_,EPOLL_CTL_ADD,fd,&event) < 0)
        {
            ::close(fd);
            continue;
        }

        Connection conn;
        conn.fd = fd;
        conn.inbound = true;
        conns_.emplace(fd,std::move(conn));

        busy_ = true;

        send_hello(conns_.at(fd));
    }
}

void RaftTransport::send_hello(Connection& conn)
{
    std::string body,error;

    if(!encode_hello(config_.self_id,body,error) || !queue_frame(RpcType::Hello,body,conn.out,error))
    {
        close_conn(conn.fd,true);
        return;
    }

    pump_write(conn);
}

bool RaftTransport::pump_write(Connection& conn)
{
    while(!conn.out.empty())
    {
        const ssize_t n = ::send(conn.fd,conn.out.data(),conn.out.size(),MSG_NOSIGNAL);

        if(n > 0)
        {
            conn.out.erase(0,static_cast<std::size_t>(n));
            continue;
        }

        if(n < 0 && errno == EINTR)
        {
            continue;
        }

        if(n < 0 && (errno == EAGAIN || errno == EWOULDBLOCK))
        {
            //缓冲区满 等续写
            if(!conn.want_write)
            {
                epoll_event event{};
                event.events = EPOLLIN | EPOLLOUT;
                event.data.fd = conn.fd;

                if(::epoll_ctl(epfd_,EPOLL_CTL_MOD,conn.fd,&event) < 0)
                {
                    close_conn(conn.fd,true);
                    return false;
                }

                conn.want_write = true;
            }

            return true;
        }

        close_conn(conn.fd,true);
        return false;
    }

    //写空收事件
    if(conn.want_write)
    {
        epoll_event event{};
        event.events = EPOLLIN;
        event.data.fd = conn.fd;

        if(::epoll_ctl(epfd_,EPOLL_CTL_MOD,conn.fd,&event) < 0)
        {
            close_conn(conn.fd,true);
            return false;
        }

        conn.want_write = false;
    }

    return true;

}

void RaftTransport::write_ready(Connection& conn)
{
    pump_write(conn);
}

void RaftTransport::close_conn(int fd,bool notify)
{
    const auto it = conns_.find(fd);

    if(it == conns_.end())
    {
        return;
    }


    const std::uint64_t peer = it->second.peer;
    const bool inbound = it->second.inbound;
    const std::uint64_t expect = it->second.expect_peer;

    ::epoll_ctl(epfd_,EPOLL_CTL_DEL,fd,nullptr);
    ::close(fd);
    conns_.erase(it);

    if(peer != 0)
    {
        const auto mapped = peer_fd_.find(peer);

        if(mapped != peer_fd_.end() && mapped->second == fd)
        {
            peer_fd_.erase(mapped);
        }
    }

    const std::uint64_t retry_id = (peer != 0) ? peer : expect;

    if(retry_id != 0 && !inbound)
    {
        next_dial_[retry_id] = std::chrono::steady_clock::now() + std::chrono::milliseconds(DIAL_RETRY_MS);
    }

    if(notify && peer != 0 && on_peer_down_)
    {
        on_peer_down_(peer);
    }
}

void RaftTransport::stop()
{
    for(auto& item:conns_)
    {
        ::epoll_ctl(epfd_,EPOLL_CTL_DEL,item.first,nullptr);
        ::close(item.first);
    }

    conns_.clear();
    peer_fd_.clear();
    next_dial_.clear();

    if(listen_fd_ >= 0)
    {
        ::close(listen_fd_);
        listen_fd_ = -1;
    }
    
    if(epfd_ >= 0)
    {
        ::close(epfd_);
        epfd_ = -1;
    }

    running_ =  false;
}

bool RaftTransport::connected(std::uint64_t peer)const
{
    return peer_fd_.count(peer) != 0;
}

bool RaftTransport::send(std::uint64_t peer,RpcType type,std::uint64_t rpc_id,const std::string& body,std::string& error)
{
    error.clear();

    const auto found = peer_fd_.find(peer);

    if(found == peer_fd_.end())
    {
        error = "raft transport : peer not connected";
        return false;
    }

    const auto it = conns_.find(found->second);

    if(it == conns_.end())
    {
        error = "raft transport : peer connection is gone";
        return false;
    }

    std::string frame;

    if(!encode_rpc_frame(type,rpc_id,body,frame,error))
    {
        return false;
    }

    it->second.out += frame;

    // 写失败会关掉连接
    if(!pump_write(it->second))
    {
        error = "raft transport : connection closed while sending";
        return false;
    }

    return true;
}


bool RaftTransport::poll(int timeout_ms,std::string& error)
{
    error.clear();
    busy_ = false;

    if(!running_)
    {
        error = "raft transport : not running";
        return false;
    }

    dial_due_peers();

    epoll_event events[64];

    const int n=  ::epoll_wait(epfd_,events,64,timeout_ms);

    if(n < 0)
    {
        if(errno == EINTR)
        {
            return true;
        }

        const int code = errno;
        error = std::string("raft transport : epoll_wait failed : ") + std::strerror(code);
        return false;
    }

    for(int i = 0 ; i < n ; i++)
    {
        const int fd = events[i].data.fd;
        const std::uint32_t what = events[i].events;

        if(fd == listen_fd_)
        {
            accept_ready();
            continue;
        }

        auto it = conns_.find(fd);

        if(it == conns_.end())
        {
            continue;
        }

        busy_ = true;

        if(!it->second.inbound && it->second.peer == 0 && (what & EPOLLOUT))
        {
            connect_ready(it->second);
            continue;
        }

        if(what & (EPOLLERR | EPOLLHUP))
        {
            close_conn(fd,true);
            continue;
        }

        if(what & EPOLLIN)
        {
            read_ready(it->second);

            if(conns_.find(fd) == conns_.end())
            {
                continue;
            }
        }

        if(what & EPOLLOUT)
        {
            const auto again = conns_.find(fd);

            if(again != conns_.end())
            {
                pump_write(again->second);
            }
        }


    }

    return true;

}

void RaftTransport::read_ready(Connection& conn)
{
    const int fd = conn.fd;

    char buffer[64*1024];

    for(; ;)
    {
        const ssize_t got = ::recv(fd,buffer,sizeof(buffer),0);

        if(got > 0)
        {
            conn.in.append(buffer,static_cast<std::size_t>(got));

            if(conn.in.size() > MAX_IN_BUFFER)
            {
                close_conn(fd,true);
                return;
            }

            continue;
        }

        if(got == 0)
        {
            close_conn(fd,true);
            return;
        }

        if(errno == EINTR)
        {
            continue;
        }

        if(errno == EAGAIN || errno == EWOULDBLOCK)
        {
            break;
        }

        close_conn(fd,true);
        return;
    }


    std::size_t at = 0;

    while(conn.in.size() - at >= RPC_LENGTH_SIZE)
    {
        std::size_t frame_szie = 0;
        std::string error;

        if(!peek_frame_size(conn.in.data() + at,conn.in.size() - at,frame_szie,error))
        {
            close_conn(fd,true);
            return;
        }

        if(conn.in.size() - at < frame_szie)
        {
            break;
        }

        RpcType type = RpcType::Hello;
        std::string body;

        if(!decode_frame(conn.in.data() + at,frame_szie,type,body,error))
        {
            close_conn(fd,true);
            return;
        }

        at += frame_szie;

        if(!handle_frame(conn,type,body,error))
        {
            close_conn(fd,true);
            return;
        }

        if(conns_.find(fd) == conns_.end())
        {
            return;
        }

    }

    if(at != 0)
    {
        conn.in.erase(0,at);
    }

}

bool RaftTransport::handle_frame(Connection& conn,RpcType type,const std::string& body,std::string& error)
{
    if(conn.peer == 0)
    {
        if(type != RpcType::Hello)
        {
            error = "raft transport : first frame is not hello";
            return false;
        }

        std::uint64_t id = 0;

        if(!decode_hello(body,id,error))
        {
            return false;
        }

        if(conn.expect_peer != 0 && conn.expect_peer != id)
        {
            error = "raft transport : hello id dose not match the dialed peer";
            return false;
        }

        if(id == config_.self_id)
        {
            error = "raft transport : peer claims our own id";
            return false;
        }

        if(peer_fd_.count(id) != 0)
        {
            error = "raft transport : duplicate connection for on peer";
            return false;
        }

        conn.peer = id;
        peer_fd_[id] = conn.fd;
        next_dial_.erase(id);
        return true;
    }

    if(type == RpcType::Hello)
    {
        error = "raft transport : duplicate hello";
        return false;
    }

    // Hello 已处理完，业务帧先拆出编号，再解码原正文。
    std::uint64_t rpc_id = 0;
    std::string rpc_body;

    if(!decode_rpc_body(body,rpc_id,rpc_body,error))
    {
        return false;
    }

    if(type == RpcType::VoteRequest)
    {
        RequestVoteRequest request;

        if(!decode_vote_request(rpc_body,request,error) || !validate_vote_request(request,error))
        {
            return false;
        }
    }
    else if(type == RpcType::AppendEntries)
    {
        AppendEntriesRequest request;

        if(!decode_append_entries(rpc_body,request,error) || !validate_append_entries(request,error))
        {
            return false;
        }
    }

    if(!on_message_)
    {
        error = "raft transport : no message handler";
        return false;
    }

    on_message_(conn.peer,type,rpc_id,rpc_body);
    return true;

}







}//namespace raft