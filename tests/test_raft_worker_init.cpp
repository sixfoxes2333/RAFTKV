// M7 启动恢复：真实 StateStore/LogStore 和目录锁；本测试不启动网络或后台线程。
#include "raft/node/worker.h"

#include <cerrno>
#include <cstdlib>
#include <dirent.h>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <string>
#include <sys/stat.h>
#include <sys/wait.h>
#include <type_traits>
#include <unistd.h>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

static_assert(!std::is_copy_constructible<raft::RaftWorker>::value);
static_assert(!std::is_copy_assignable<raft::RaftWorker>::value);
static_assert(!std::is_move_constructible<raft::RaftWorker>::value);
static_assert(!std::is_move_assignable<raft::RaftWorker>::value);

namespace {

// 每个用例拥有独立目录，析构只删除自己创建的测试文件。
struct TempDir
{
    std::string path;
    TempDir()
    {
        char pattern[] = "/tmp/raft-worker-init.XXXXXX";
        const char* created = ::mkdtemp(pattern);
        CHECK(created != nullptr);
        path = created;
    }
    ~TempDir()
    {
        std::error_code ignored;
        std::filesystem::remove_all(path,ignored);
    }
    TempDir(const TempDir&) = delete;
    TempDir& operator=(const TempDir&) = delete;
};

// 复用配置解析；本测试不绑定这些端口。
raft::RaftConfig config_for(const std::string& directory)
{
    raft::RaftConfig config;
    std::string error;
    CHECK(raft::parse_raft_config("1",
          "1@127.0.0.1:19301,2@127.0.0.1:19302,3@127.0.0.1:19303",
          directory,config,error));
    return config;
}

// 读取普通测试文件，以字节比较检查失败启动有没有覆盖已有内容。
std::string read_bytes(const std::string& path)
{
    std::ifstream in(path,std::ios::binary);
    CHECK(in.is_open());
    const std::string bytes((std::istreambuf_iterator<char>(in)),
                             std::istreambuf_iterator<char>());
    CHECK(!in.bad());
    return bytes;
}

// 仅用于私有目录里的损坏/残尾夹具，不模拟 fsync 或掉电。
void write_bytes(const std::string& path,const std::string& bytes)
{
    std::ofstream out(path,std::ios::binary | std::ios::trunc);
    CHECK(out.is_open());
    out.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
    out.close();
    CHECK(out.good());
}

// 用正式存储接口生成恢复输入；条目任期为 1、2、4。
void prepare(const std::string& directory,const raft::HardState& state,bool entries)
{
    std::string error;
    raft::StateStore states(directory);
    CHECK(states.save(state,error));
    raft::LogStore log(directory);
    CHECK(log.create(error));
    if(entries)
    {
        const std::vector<raft::LogEntry> batch = {
            {1,1,"first"},{2,2,std::string("k\0v",3)},{3,4,""}
        };
        CHECK(log.append_batch(batch,error));
    }
}

// 失败必须返回可见错误；函数返回时 Worker 已销毁并释放目录锁。
std::string reject(const std::string& directory)
{
    raft::RaftWorker worker(config_for(directory));
    std::string error;
    CHECK(!worker.init(error));
    CHECK(!error.empty());
    return error;
}

// 空目录生成零状态和空日志；同一实例拒绝再次初始化。
void test_fresh_and_one_shot()
{
    TempDir dir;
    {
        raft::RaftWorker worker(config_for(dir.path));
        std::string error = "stale";
        CHECK(worker.init(error));
        CHECK(error.empty());
        CHECK(read_bytes(dir.path + "/raft.state").size() == 28);
        CHECK(read_bytes(dir.path + "/raft.log").empty());
        CHECK(!worker.init(error));
        CHECK(error.find("init already attempted") != std::string::npos);
    }
    raft::HardState state;
    raft::StateStore states(dir.path);
    std::string error;
    CHECK(states.load(state,error));
    CHECK(state.current_term == 0 && state.voted_for == 0);
    raft::RaftWorker restarted(config_for(dir.path));
    CHECK(restarted.init(error));
    std::cout << "PASS worker init: fresh files, zero state, one-shot init and reopen\n";
}

// 已有任期、投票和二进制日志能恢复，正常启动保持原磁盘字节。
void test_existing_state_and_log()
{
    TempDir dir;
    prepare(dir.path,raft::HardState{5,2},true);
    const auto state_before = read_bytes(dir.path + "/raft.state");
    const auto log_before = read_bytes(dir.path + "/raft.log");
    {
        raft::RaftWorker worker(config_for(dir.path));
        std::string error = "stale";
        CHECK(worker.init(error));
        CHECK(error.empty());
        CHECK(read_bytes(dir.path + "/raft.state") == state_before);
        CHECK(read_bytes(dir.path + "/raft.log") == log_before);
    }
    raft::HardState state;
    raft::StateStore states(dir.path);
    std::string error;
    CHECK(states.load(state,error));
    CHECK(state.current_term == 5 && state.voted_for == 2);
    raft::LogStore log(dir.path);
    CHECK(log.open(error));
    CHECK(log.last_index() == 3);
    raft::LogEntry entry;
    CHECK(log.get(2,entry,error));
    CHECK(entry.index == 2 && entry.term == 2);
    CHECK(entry.command == std::string("k\0v",3));
    std::cout << "PASS worker init: existing term/vote and binary log preserved\n";
}

// 模拟首次启动只完成了零状态保存：新 Worker 可以补建空日志。
void test_partial_bootstrap()
{
    TempDir dir;
    raft::StateStore states(dir.path);
    std::string error;
    CHECK(states.save(raft::HardState{},error));
    const auto before = read_bytes(dir.path + "/raft.state");
    raft::RaftWorker worker(config_for(dir.path));
    CHECK(worker.init(error));
    CHECK(read_bytes(dir.path + "/raft.state") == before);
    CHECK(read_bytes(dir.path + "/raft.log").empty());
    std::cout << "PASS worker init: zero-state-only bootstrap completed\n";
}

// 丢失硬状态或已有任期/投票却丢失日志时拒绝启动，不补写另一份文件。
void test_missing_files()
{
    {
        TempDir dir;
        {
            raft::LogStore log(dir.path);
            std::string error;
            CHECK(log.create(error));
            CHECK(log.append(raft::LogEntry{1,1,"saved"},error));
        }
        const auto before = read_bytes(dir.path + "/raft.log");
        reject(dir.path);
        CHECK(!std::filesystem::exists(dir.path + "/raft.state"));
        CHECK(read_bytes(dir.path + "/raft.log") == before);
    }
    for(const auto state : {raft::HardState{5,2},raft::HardState{0,2}})
    {
        TempDir dir;
        raft::StateStore states(dir.path);
        std::string error;
        CHECK(states.save(state,error));
        const auto before = read_bytes(dir.path + "/raft.state");
        reject(dir.path);
        CHECK(!std::filesystem::exists(dir.path + "/raft.log"));
        CHECK(read_bytes(dir.path + "/raft.state") == before);
    }
    std::cout << "PASS worker init: missing state/log refused without replacement\n";
}

// 恢复输入仍交给 Core 校验：非成员投票和日志尾任期过高都应拒绝。
void test_core_recovery_checks()
{
    for(const auto state : {raft::HardState{5,99},raft::HardState{3,2}})
    {
        TempDir dir;
        prepare(dir.path,state,true);
        const auto before_state = read_bytes(dir.path + "/raft.state");
        const auto before_log = read_bytes(dir.path + "/raft.log");
        reject(dir.path);
        CHECK(read_bytes(dir.path + "/raft.state") == before_state);
        CHECK(read_bytes(dir.path + "/raft.log") == before_log);
    }
    std::cout << "PASS worker init: invalid recovered vote and tail term reach Core checks\n";
}

// 完整记录损坏必须拒绝；真正不完整的日志尾部可以由 LogStore 修复。
void test_corruption_and_torn_tail()
{
    for(const std::string file : {"raft.state","raft.log"})
    {
        TempDir dir;
        prepare(dir.path,raft::HardState{5,2},true);
        auto bytes = read_bytes(dir.path + "/" + file);
        CHECK(!bytes.empty());
        bytes.back() ^= 1;
        write_bytes(dir.path + "/" + file,bytes);
        reject(dir.path);
        CHECK(read_bytes(dir.path + "/" + file) == bytes);
    }
    {
        TempDir dir;
        prepare(dir.path,raft::HardState{5,2},true);
        const auto before = read_bytes(dir.path + "/raft.log");
        write_bytes(dir.path + "/raft.log",before + std::string("RL",2));
        raft::RaftWorker worker(config_for(dir.path));
        std::string error;
        CHECK(worker.init(error));
        CHECK(read_bytes(dir.path + "/raft.log") == before);
    }
    std::cout << "PASS worker init: complete corruption refused, incomplete tail repaired\n";
}

// 文件路径若是目录、FIFO 或符号链接，启动应报错而不是阻塞打开或覆盖。
void test_nonregular_files()
{
    for(const std::string name : {"raft.state","raft.log"})
    {
        for(int kind = 0; kind < 3; ++kind)
        {
            TempDir dir;
            const std::string path = dir.path + "/" + name;
            const std::string target = dir.path + "/target";
            write_bytes(target,"untouched");
            if(kind == 0) CHECK(::mkdir(path.c_str(),0700) == 0);
            if(kind == 1) CHECK(::mkfifo(path.c_str(),0600) == 0);
            if(kind == 2) CHECK(::symlink(target.c_str(),path.c_str()) == 0);
            reject(dir.path);
            CHECK(read_bytes(target) == "untouched");
            struct stat st{};
            CHECK(::lstat(path.c_str(),&st) == 0);
            CHECK(kind == 0 ? S_ISDIR(st.st_mode) :
                 kind == 1 ? S_ISFIFO(st.st_mode) : S_ISLNK(st.st_mode));
        }
    }
    std::cout << "PASS worker init: directories, FIFOs and symlinks refused\n";
}

// 另起一个进程，用全新 Worker 尝试取得已被占用的目录锁。
void check_child_locked(const std::string& directory)
{
    const pid_t child = ::fork();
    CHECK(child >= 0);
    if(child == 0)
    {
        ::execl("/proc/self/exe","test_raft_worker_init","--locked",
                directory.c_str(),static_cast<char*>(nullptr));
        ::_exit(127);
    }
    int status = 0;
    pid_t waited;
    do { waited = ::waitpid(child,&status,0); } while(waited < 0 && errno == EINTR);
    CHECK(waited == child);
    CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
}

// 同进程和独立进程都不能打开被占用目录；销毁持有者后可重新初始化。
void test_directory_lock()
{
    TempDir dir;
    {
        raft::RaftWorker owner(config_for(dir.path));
        std::string error;
        CHECK(owner.init(error));
        CHECK(reject(dir.path).find("lock data directory") != std::string::npos);
        check_child_locked(dir.path);
    }
    raft::RaftWorker next(config_for(dir.path));
    std::string error;
    CHECK(next.init(error));
    std::cout << "PASS worker init: directory lock excludes another object/process and releases\n";
}

// 统计本进程 fd，检查反复创建/销毁 Worker 后是否遗留目录或日志描述符。
std::size_t fd_count()
{
    DIR* directory = ::opendir("/proc/self/fd");
    CHECK(directory != nullptr);
    std::size_t count = 0;
    while(const dirent* entry = ::readdir(directory))
    {
        if(entry->d_name[0] != '.') ++count;
    }
    CHECK(::closedir(directory) == 0);
    return count;
}

// 失败对象只尝试一次；缺失/非目录路径报错；正常与失败析构均释放 fd。
void test_lifecycle_and_failed_init()
{
    TempDir dir;
    const std::string missing = dir.path + "/missing";
    {
        raft::RaftWorker worker(config_for(missing));
        std::string error;
        CHECK(!worker.init(error));
        CHECK(!error.empty());
        CHECK(::mkdir(missing.c_str(),0700) == 0);
        CHECK(!worker.init(error));
        CHECK(error.find("init already attempted") != std::string::npos);
        CHECK(!std::filesystem::exists(missing + "/raft.state"));
    }
    const std::string not_dir = dir.path + "/file";
    write_bytes(not_dir,"keep");
    reject(not_dir);
    CHECK(read_bytes(not_dir) == "keep");
    const auto before = fd_count();
    for(int i = 0; i < 12; ++i)
    {
        raft::RaftWorker worker(config_for(missing));
        std::string error;
        CHECK(worker.init(error));
        reject(missing);
    }
    CHECK(fd_count() == before);
    std::cout << "PASS worker init: failed one-shot init, invalid directories and fd cleanup\n";
}

} // namespace

int main(int argc,char** argv)
{
    if(argc == 3 && std::string(argv[1]) == "--locked")
    {
        raft::RaftWorker worker(config_for(argv[2]));
        std::string error;
        if(worker.init(error)) return 2;
        if(error.find("lock data directory") != std::string::npos) return 0;
        std::cerr << "unexpected child init error: " << error << '\n';
        return 3;
    }
    test_fresh_and_one_shot();
    test_existing_state_and_log();
    test_partial_bootstrap();
    test_missing_files();
    test_core_recovery_checks();
    test_corruption_and_torn_tail();
    test_nonregular_files();
    test_directory_lock();
    test_lifecycle_and_failed_init();
    std::cout << "raft worker init PASS (real stores; no network or worker thread)\n";
    return 0;
}
