// tests/kvbench.cpp —— C++ 压测客户端（用于"服务器真实上限"与口径修正）
//
// 【为什么需要它，而不是继续用 bench.py】
//   Python 客户端的开销会污染测量结果，本期已经踩过两次：
//     · Python 线程受 GIL 限制：GET 在 C=16 看起来从 20k 掉到 4k（其实是客户端退化）
//     · 闭环客户端一个 RTT 一条请求：单 worker 看起来只有 38k 上限（其实是往返限制）
//   所以压测客户端必须"尽量便宜"：
//     · 每线程一条连接，无锁
//     · 请求字节**在预热阶段一次性编码好**，热路径只做 send/recv
//     · 延迟用真实逐请求测量（发送时刻入队 → 按 FIFO 出队配对），
//       而不是"批次耗时 ÷ 条数"这种推导值
//
// 【它同时解决的两件事】
//   · #9 分位数口径错：所有线程的样本**合并后**排序再算 P50/P90/P99/P999，
//     不是"各连接分位数取平均"（后者可能是完全错误的数字）
//   · #12 需要一个能判断"客户端是否成为瓶颈"的工具：本工具报告客户端 CPU 时间
//
// 【用法】
//   ./build/kvbench --conns 8 --pipeline 16 --op get --duration 3
//   ./build/kvbench --conns 1 --pipeline 1 --op set --value-size 100 --duration 3
//   ./build/kvbench --conns 64 --pipeline 16 --op mix --write-ratio 20
//   ./build/kvbench --nodelay 0 --op get --pipeline 16     # 对比 Nagle 的影响（客户端侧）
//
//   最后一行固定输出：RESULT op=... conns=... pipeline=... qps=... p50_us=... ...
//   便于脚本抓取（tests/net_nodelay_test.py 就靠它做围栏）
//
// 【注意】
//   GET 负载需要 key 已存在；先用 --op set 预热一遍键空间（--keyspace 决定规模）
//
// 【边界】
//   请求模板是"每个 key 一份"放在内存里，所以 --keyspace 默认只到 1 万；
//   要压百万级键空间，应改成"按 key 现编码"（代价是客户端开销上升）。

#include "base/limits.h"
#include "net/protocol.h"

#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <sys/resource.h>
#include <sys/socket.h>
#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <iostream>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

namespace
{

using Clock = std::chrono::steady_clock;

uint64_t now_us()
{
    return static_cast<uint64_t>(
        std::chrono::duration_cast<std::chrono::microseconds>(
            Clock::now().time_since_epoch()).count());
}

struct Options
{
    std::string host = "127.0.0.1";
    int port = 9006;
    int conns = 1;
    int pipeline = 1;
    std::string op = "set";     //set / get / mix
    int value_size = 100;
    int keyspace = 10000;
    int write_ratio = 20;       //mix 模式里写的百分比
    double duration = 3.0;      //测量时长（秒）
    double warmup = 1.0;        //预热时长（秒，不计入统计）
    int nodelay = 1;            //客户端是否设 TCP_NODELAY（1=设，0=不设）
    bool verbose = false;
};

//把一条请求编码成完整帧：[4B 总长][payload]，payload 由 protocol 层负责
std::string make_frame(const kv::Request &req)
{
    std::string body = kv::encode_request(req);
    uint32_t be_length = htonl(static_cast<uint32_t>(body.size()));

    std::string frame;
    frame.reserve(4 + body.size());
    frame.append(reinterpret_cast<const char*>(&be_length),4);
    frame.append(body);
    return frame;
}

int connect_to(const Options &opt)
{
    int fd = socket(AF_INET,SOCK_STREAM,0);

    if(fd < 0)
    {
        return -1;
    }

    if(opt.nodelay)
    {
        //客户端发送侧也关掉 Nagle：多条流水线请求不该被攒起来
        int one = 1;
        setsockopt(fd,IPPROTO_TCP,TCP_NODELAY,&one,sizeof(one));
    }

    struct sockaddr_in addr;
    memset(&addr,0,sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(opt.port));
    inet_pton(AF_INET,opt.host.c_str(),&addr.sin_addr);

    if(connect(fd,reinterpret_cast<struct sockaddr*>(&addr),sizeof(addr)) < 0)
    {
        close(fd);
        return -1;
    }

    return fd;
}

bool send_all(int fd,const char *data,size_t len)
{
    size_t done = 0;

    while(done < len)
    {
        ssize_t n = send(fd,data + done,len - done,MSG_NOSIGNAL);

        if(n > 0)
        {
            done += static_cast<size_t>(n);
            continue;
        }

        if(n < 0 && errno == EINTR)
        {
            continue;
        }

        return false; //EAGAIN 不会出现（阻塞 socket），其它都是真错误
    }

    return true;
}

bool recv_all(int fd,char *data,size_t len)
{
    size_t done = 0;

    while(done < len)
    {
        ssize_t n = recv(fd,data + done,len - done,0);

        if(n > 0)
        {
            done += static_cast<size_t>(n);
            continue;
        }

        if(n == 0)
        {
            return false; //对端关闭
        }

        if(errno == EINTR)
        {
            continue;
        }

        return false;
    }

    return true;
}

//读一条响应帧；status 写入 out_status（0=Ok 1=NotFound 2=Error）
bool recv_response(int fd,uint8_t &out_status,size_t &out_value_len,std::string &scratch)
{
    char header[4];

    if(!recv_all(fd,header,4))
    {
        return false;
    }

    uint32_t be;
    memcpy(&be,header,4);
    uint32_t len = ntohl(be);

    if(len < kv::RESPONSE_HEADER_SIZE || len > kv::MAX_MESSAGE)
    {
        return false; //帧长非法：直接判定协议损坏
    }

    if(scratch.size() < len)
    {
        scratch.resize(len);
    }

    if(!recv_all(fd,&scratch[0],len))
    {
        return false;
    }

    out_status = static_cast<uint8_t>(scratch[0]);

    uint32_t vlen_be;
    memcpy(&vlen_be,scratch.data() + 1,4);
    uint32_t vlen = ntohl(vlen_be);

    if(len != kv::RESPONSE_HEADER_SIZE + vlen)
    {
        return false; //声明长度与实际不符
    }

    out_value_len = vlen;
    return true;
}

struct ThreadResult
{
    uint64_t ops = 0;
    uint64_t errors = 0;
    std::vector<uint32_t> lat_us; //本线程的逐请求延迟（微秒）
    std::string first_error;
};

void record_error(ThreadResult &r,const std::string &msg)
{
    r.errors++;

    if(r.first_error.empty())
    {
        r.first_error = msg;
    }
}

void worker(const Options &opt,int id,const std::vector<std::string> &set_frames,
            const std::vector<std::string> &get_frames,ThreadResult &result)
{
    int fd = connect_to(opt);

    if(fd < 0)
    {
        record_error(result,"connect failed: " + std::string(strerror(errno)));
        return;
    }

    const int depth = opt.pipeline;
    const int n_keys = static_cast<int>(set_frames.size());

    std::vector<uint64_t> send_ts(depth,0);
    std::string scratch;
    scratch.reserve(64 + static_cast<size_t>(opt.value_size));

    const Clock::time_point t_start = Clock::now();
    const Clock::time_point warmup_end = t_start + std::chrono::duration_cast<Clock::duration>(
        std::chrono::duration<double>(opt.warmup));
    const Clock::time_point measure_end = warmup_end + std::chrono::duration_cast<Clock::duration>(
        std::chrono::duration<double>(opt.duration));

    uint64_t index = static_cast<uint64_t>(id);

    for(int phase = 0; phase < 2; phase++)
    {
        const bool measuring = (phase == 1);
        const Clock::time_point deadline = measuring ? measure_end : warmup_end;

        while(Clock::now() < deadline)
        {
            //① 先发一整批（流水线深度决定"同时在途"多少条）
            for(int k = 0; k < depth; k++)
            {
                const bool is_write = (opt.op == "set") ||
                    (opt.op == "mix" && (index % 100) < static_cast<uint64_t>(opt.write_ratio));

                const std::string &frame = is_write ? set_frames[index % n_keys]
                                                    : get_frames[index % n_keys];

                send_ts[k] = now_us();

                if(!send_all(fd,frame.data(),frame.size()))
                {
                    record_error(result,"send failed: " + std::string(strerror(errno)));
                    close(fd);
                    return;
                }

                index++;
            }

            //② 再收一整批：TCP 保序，所以第 k 个响应必对应第 k 个请求
            for(int k = 0; k < depth; k++)
            {
                uint8_t status = 2;
                size_t vlen = 0;

                if(!recv_response(fd,status,vlen,scratch))
                {
                    record_error(result,"recv/parse failed: " + std::string(strerror(errno)));
                    close(fd);
                    return;
                }

                const uint64_t done_us = now_us();

                if(status == static_cast<uint8_t>(kv::Status::Error))
                {
                    record_error(result,"server returned Error");
                    continue;
                }

                if(measuring)
                {
                    uint64_t lat = (done_us > send_ts[k]) ? (done_us - send_ts[k]) : 0;

                    if(lat > 0xffffffffu)
                    {
                        lat = 0xffffffffu;
                    }

                    result.lat_us.push_back(static_cast<uint32_t>(lat));
                    result.ops++;
                }
            }
        }
    }

    close(fd);
}

uint64_t percentile_us(std::vector<uint32_t> &sorted,double q)
{
    if(sorted.empty())
    {
        return 0;
    }

    size_t idx = static_cast<size_t>(q * static_cast<double>(sorted.size()));

    if(idx >= sorted.size())
    {
        idx = sorted.size() - 1;
    }

    return sorted[idx];
}

void print_usage(const char *argv0)
{
    std::cout
        << "用法：" << argv0 << " [选项]\n"
        << "  --host <ip>         默认 127.0.0.1\n"
        << "  --port <n>          默认 9006\n"
        << "  --conns <n>         并发连接数（每线程一条），默认 1\n"
        << "  --pipeline <n>      流水线深度（一批发 n 条再收 n 条），默认 1（闭环）\n"
        << "  --op <set|get|mix>  负载类型，默认 set\n"
        << "  --value-size <n>    SET 的 value 字节数，默认 100\n"
        << "  --keyspace <n>      键空间大小，默认 10000\n"
        << "  --write-ratio <n>   mix 模式里写的百分比，默认 20\n"
        << "  --duration <秒>     测量时长，默认 3\n"
        << "  --warmup <秒>       预热时长（不计入统计），默认 1\n"
        << "  --nodelay <0|1>     客户端是否设 TCP_NODELAY，默认 1\n"
        << "  --verbose           打印每个线程的明细\n";
}

bool parse_options(int argc,char **argv,Options &opt)
{
    for(int i = 1; i < argc; i++)
    {
        std::string k = argv[i];

        auto next = [&](std::string &out) -> bool
        {
            if(i + 1 >= argc)
            {
                std::cerr << "缺少参数值：" << k << std::endl;
                return false;
            }

            out = argv[++i];
            return true;
        };

        std::string v;

        if(k == "--help" || k == "-h")
        {
            print_usage(argv[0]);
            exit(0);
        }
        else if(k == "--host" && next(v))   { opt.host = v; }
        else if(k == "--port" && next(v))   { opt.port = atoi(v.c_str()); }
        else if(k == "--conns" && next(v))  { opt.conns = std::max(1,atoi(v.c_str())); }
        else if(k == "--pipeline" && next(v)) { opt.pipeline = std::max(1,atoi(v.c_str())); }
        else if(k == "--op" && next(v))     { opt.op = v; }
        else if(k == "--value-size" && next(v)) { opt.value_size = std::max(0,atoi(v.c_str())); }
        else if(k == "--keyspace" && next(v)) { opt.keyspace = std::max(1,atoi(v.c_str())); }
        else if(k == "--write-ratio" && next(v)) { opt.write_ratio = atoi(v.c_str()); }
        else if(k == "--duration" && next(v)) { opt.duration = atof(v.c_str()); }
        else if(k == "--warmup" && next(v)) { opt.warmup = atof(v.c_str()); }
        else if(k == "--nodelay" && next(v)) { opt.nodelay = atoi(v.c_str()); }
        else if(k == "--verbose")           { opt.verbose = true; }
        else
        {
            std::cerr << "未知参数：" << k << "（用 --help 看用法）" << std::endl;
            return false;
        }
    }

    if(opt.op != "set" && opt.op != "get" && opt.op != "mix")
    {
        std::cerr << "--op 只能是 set / get / mix" << std::endl;
        return false;
    }

    return true;
}

} //namespace

int main(int argc,char **argv)
{
    Options opt;

    if(!parse_options(argc,argv,opt))
    {
        return 2;
    }

    //① 一次性编码好请求模板：热路径只做 send/recv，客户端开销压到最低
    const size_t n_keys = static_cast<size_t>(opt.keyspace);
    std::vector<std::string> set_frames(n_keys);
    std::vector<std::string> get_frames(n_keys);

    const std::string value(static_cast<size_t>(opt.value_size),'v');
    char key_buf[32];

    for(size_t i = 0; i < n_keys; i++)
    {
        int key_len = snprintf(key_buf,sizeof(key_buf),"k%08zu",i);
        std::string key(key_buf,static_cast<size_t>(key_len));

        kv::Request set_req;
        set_req.op = kv::Op::Set;
        set_req.key = key;
        set_req.value = value;
        set_frames[i] = make_frame(set_req);

        kv::Request get_req;
        get_req.op = kv::Op::Get;
        get_req.key = key;
        get_frames[i] = make_frame(get_req);
    }

    //② 起线程
    std::vector<ThreadResult> results(static_cast<size_t>(opt.conns));
    std::vector<std::thread> threads;
    threads.reserve(static_cast<size_t>(opt.conns));

    struct rusage ru_before;
    getrusage(RUSAGE_SELF,&ru_before);

    const Clock::time_point t0 = Clock::now();

    for(int i = 0; i < opt.conns; i++)
    {
        threads.emplace_back(worker,std::cref(opt),i,std::cref(set_frames),
                             std::cref(get_frames),std::ref(results[static_cast<size_t>(i)]));
    }

    for(auto &t : threads)
    {
        t.join();
    }

    const double elapsed = std::chrono::duration<double>(Clock::now() - t0).count();

    struct rusage ru_after;
    getrusage(RUSAGE_SELF,&ru_after);

    //③ 合并所有线程的样本（这就是 #9 要修的口径）
    uint64_t total_ops = 0;
    uint64_t total_errors = 0;
    std::string first_error;
    std::vector<uint32_t> lat;
    uint64_t min_thread_ops = UINT64_MAX;
    uint64_t max_thread_ops = 0;

    for(auto &r : results)
    {
        total_ops += r.ops;
        total_errors += r.errors;
        min_thread_ops = std::min(min_thread_ops,r.ops);
        max_thread_ops = std::max(max_thread_ops,r.ops);
        lat.insert(lat.end(),r.lat_us.begin(),r.lat_us.end());

        if(first_error.empty() && !r.first_error.empty())
        {
            first_error = r.first_error;
        }
    }

    if(total_errors > 0)
    {
        std::cerr << "错误：" << total_errors << " 次（首个：" << first_error << "）" << std::endl;
        return 1;
    }

    std::sort(lat.begin(),lat.end());

    const double qps = elapsed > 0 ? static_cast<double>(total_ops) / elapsed : 0;
    const double avg_us = lat.empty() ? 0 :
        static_cast<double>(std::accumulate(lat.begin(),lat.end(),uint64_t(0))) /
        static_cast<double>(lat.size());
    const double client_cpu = (ru_after.ru_utime.tv_sec + ru_after.ru_stime.tv_sec) -
        (ru_before.ru_utime.tv_sec + ru_before.ru_stime.tv_sec) +
        ((ru_after.ru_utime.tv_usec + ru_after.ru_stime.tv_usec) -
         (ru_before.ru_utime.tv_usec + ru_before.ru_stime.tv_usec)) / 1e6;

    std::cout << "op=" << opt.op << " conns=" << opt.conns << " pipeline=" << opt.pipeline
              << " value_size=" << opt.value_size << " keyspace=" << opt.keyspace
              << " nodelay=" << opt.nodelay << std::endl;
    std::cout << "  QPS " << qps
              << " | 平均 " << avg_us << "us"
              << " | P50 " << percentile_us(lat,0.50) << "us"
              << " | P90 " << percentile_us(lat,0.90) << "us"
              << " | P99 " << percentile_us(lat,0.99) << "us"
              << " | P999 " << percentile_us(lat,0.999) << "us"
              << " | 最大 " << (lat.empty() ? 0 : lat.back()) << "us" << std::endl;
    std::cout << "  样本 " << lat.size() << " | 用时 " << elapsed << "s"
              << " | 客户端 CPU " << client_cpu << "s"
              << " | 每线程 ops 最少 " << (results.empty() ? 0 : min_thread_ops)
              << " 最多 " << max_thread_ops << std::endl;

    if(opt.verbose)
    {
        for(size_t i = 0; i < results.size(); i++)
        {
            std::cout << "    thread " << i << ": ops=" << results[i].ops
                      << " samples=" << results[i].lat_us.size() << std::endl;
        }
    }

    std::cout << "RESULT op=" << opt.op << " conns=" << opt.conns
              << " pipeline=" << opt.pipeline << " nodelay=" << opt.nodelay
              << " qps=" << qps
              << " avg_us=" << avg_us
              << " p50_us=" << percentile_us(lat,0.50)
              << " p90_us=" << percentile_us(lat,0.90)
              << " p99_us=" << percentile_us(lat,0.99)
              << " p999_us=" << percentile_us(lat,0.999)
              << " ops=" << total_ops
              << " client_cpu_s=" << client_cpu << std::endl;

    return 0;
}
