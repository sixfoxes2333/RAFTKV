// M8：派生 KV 的 Rebuild 打开方式；默认 Recover 语义必须保持不变。
#include "storage/storage.h"
#include "app/storage_worker.h"
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fcntl.h>
#include <filesystem>
#include <iostream>
#include <string>
#include <sys/file.h>
#include <sys/stat.h>
#include <unistd.h>

#define CHECK(x) do { if(!(x)) { std::cerr << __FILE__ << ':' << __LINE__ \
    << ": FAIL " #x << '\n'; std::exit(1); } } while(false)

namespace {
std::string temp_path(const char* name)
{
    char pattern[] = "/tmp/kv-rebuild.XXXXXX";
    CHECK(::mkdtemp(pattern) != nullptr);
    return std::string(pattern) + "/" + name;
}

//独立写入一个可识别的旧库文件，不经过被测构造。
//用正式写入路径造一个合法旧库，避免把损坏文件当成恢复成功。
void write_stale(const std::string& path,const std::string& payload)
{
    kv::Storage store(path);
    CHECK(store.good());
    CHECK(store.put("stale",payload));
}

std::string read_all(const std::string& path)
{
    std::string out;
    FILE* file = std::fopen(path.c_str(),"rb");
    CHECK(file != nullptr);
    char buffer[4096];
    std::size_t n;
    while((n = std::fread(buffer,1,sizeof(buffer),file)) > 0) out.append(buffer,n);
    CHECK(std::fclose(file) == 0);
    return out;
}

//默认构造仍按旧路径恢复，旧数据必须继续可读。
void recover_defaults()
{
    const auto path = temp_path("plain.data");
    {
        kv::Storage store(path);
        CHECK(store.good());
        CHECK(store.put("keep","value"));
    }
    {
        kv::Storage store(path);
        CHECK(store.good() && store.contains("keep"));
        std::string value;
        CHECK(store.get("keep",value) && value == "value");
    }
    std::cout << "PASS rebuild: default Recover keeps old data\n";
}

//Rebuild 必须忽略旧内容，得到一个只含文件头的空库。
void rebuild_is_empty()
{
    const auto path = temp_path("cache.data");
    write_stale(path,"stale-payload");
    CHECK(read_all(path).size() > 8);
    {
        kv::Storage store(path,kv::Storage::OpenMode::Rebuild);
        CHECK(store.good());
        CHECK(store.key_count() == 0);
        CHECK(!store.contains("stale"));
        CHECK(store.put("fresh","v"));
    }
    const auto bytes = read_all(path);
    CHECK(bytes.size() > 8);
    CHECK(bytes.compare(0,4,"KVDB") == 0);
    CHECK(bytes.find("stale-payload") == std::string::npos);
    CHECK(bytes.find("fresh") != std::string::npos);
    std::cout << "PASS rebuild: stale payload replaced by a fresh header\n";
}

//重建出的库随后可按 Recover 正常读取，说明文件头流程仍然有效。
void rebuild_then_recover()
{
    const auto path = temp_path("round.data");
    write_stale(path,"old-bytes");
    {
        kv::Storage store(path,kv::Storage::OpenMode::Rebuild);
        CHECK(store.good() && store.put("a","1") && store.put("b","2"));
    }
    {
        kv::Storage store(path);
        CHECK(store.good() && store.key_count() == 2);
        std::string value;
        CHECK(store.get("a",value) && value == "1");
        CHECK(store.get("b",value) && value == "2");
    }
    std::cout << "PASS rebuild: rebuilt file recovers normally\n";
}

//有人持锁时，Rebuild 必须在截断之前失败，原文件字节不动。
void locked_file_is_never_truncated()
{
    const auto path = temp_path("locked.data");
    write_stale(path,"must-survive-lock-conflict");
    const auto before = read_all(path);
    const int fd = ::open(path.c_str(),O_RDWR);
    CHECK(fd >= 0);
    CHECK(::flock(fd,LOCK_EX | LOCK_NB) == 0);
    {
        kv::Storage store(path,kv::Storage::OpenMode::Rebuild);
        CHECK(!store.good());
    }
    CHECK(read_all(path) == before);
    CHECK(::flock(fd,LOCK_UN) == 0);
    CHECK(::close(fd) == 0);
    {
        kv::Storage store(path);
        CHECK(store.good() && store.contains("stale"));
    }
    std::cout << "PASS rebuild: lock conflict rejected before any truncate\n";
}

//符号链接和目录不能被当成可重建的普通文件。
void unsafe_targets_rejected()
{
    const auto real = temp_path("real.data");
    write_stale(real,"target");
    const auto before = read_all(real);
    const auto link = temp_path("link.data");
    CHECK(::symlink(real.c_str(),link.c_str()) == 0);
    {
        kv::Storage store(link,kv::Storage::OpenMode::Rebuild);
        CHECK(!store.good());
    }
    CHECK(read_all(real) == before);

    const auto directory = temp_path("dir.data");
    CHECK(::mkdir(directory.c_str(),0700) == 0);
    {
        kv::Storage store(directory,kv::Storage::OpenMode::Rebuild);
        CHECK(!store.good());
    }
    struct stat info{};
    CHECK(::stat(directory.c_str(),&info) == 0 && S_ISDIR(info.st_mode));
    std::cout << "PASS rebuild: symlink and directory refused\n";
}

//硬链接意味着同一份数据还有别的名字，截断会连带破坏另一个入口。
void hardlink_rejected()
{
    const auto real = temp_path("shared.data");
    write_stale(real,"shared-data");
    const auto before = read_all(real);
    const auto other = temp_path("alias.data");
    CHECK(::link(real.c_str(),other.c_str()) == 0);
    struct stat info{};
    CHECK(::stat(real.c_str(),&info) == 0 && info.st_nlink == 2);
    {
        kv::Storage store(other,kv::Storage::OpenMode::Rebuild);
        CHECK(!store.good());
    }
    CHECK(read_all(real) == before && read_all(other) == before);
    std::cout << "PASS rebuild: hard-linked file refused, both names intact\n";
}

//StorageWorker 把 OpenMode 透传到自己的 Storage，并用 Off 模式启动。
void worker_uses_open_mode()
{
    const auto path = temp_path("worker.data");
    write_stale(path,"worker-stale");
    {
        kv::StorageWorker worker(path,1,kv::StorageWorker::SyncMode::Off,
                                 kv::Storage::OpenMode::Rebuild);
        CHECK(worker.ready());
        CHECK(!read_all(path).empty());
        CHECK(read_all(path).find("worker-stale") == std::string::npos);
    }
    {
        kv::StorageWorker worker(path,4,kv::StorageWorker::SyncMode::Always);
        CHECK(worker.ready());
    }
    std::cout << "PASS rebuild: StorageWorker forwards OpenMode, default stays Recover\n";
}
}

int main()
{
    recover_defaults();
    rebuild_is_empty();
    rebuild_then_recover();
    locked_file_is_never_truncated();
    unsafe_targets_rejected();
    hardlink_rejected();
    worker_uses_open_mode();
    std::cout << "storage rebuild PASS (7 groups; derived-KV open path only)\n";
}
