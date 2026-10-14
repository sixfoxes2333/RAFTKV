// test_logstore.cpp —— Raft 日志存储的单元测试（先红后绿）
//
// 【这个测试防的是什么】
//   Raft 的追加、冲突截断和重启恢复都建立在这个模块上：
//     · append_batch 返回 true 之前，整批必须已经 fsync 落盘（否则崩溃后丢已确认日志）
//     · index 必须连续、term 非零且不下降，否则重启恢复会拒绝自己刚写的日志
//     · 重启后必须能从磁盘重建内存索引（last_index / get / term_at）
//     · 残尾（写到一半）可以截断修复；【完整】的损坏记录必须拒绝启动，不许静默丢数据
//     · truncate_suffix 删除确认前不许覆盖，删除后追加不许有空洞
//
// 【为什么有单次调用的命令行模式】
//   logstore_fault_test.py 要用 LD_PRELOAD 注入 pwrite/fsync/ftruncate 失败，
//   所以这个二进制要能被当作"一次创建 / 一次追加 / 一次截断 / 一次查询"单独调用。
//
// 【为什么不用掉电验证】
//   pwrite 成功只代表进了页缓存；同进程或正常重启都看不出"没 fsync"的差别。
//   这里验证的是格式、失败语义与恢复行为；"fsync 是否真的发生、顺序如何"
//   由 logstore_fault_test.py 用调用计数和 strace 覆盖。
#include "raft/logstore.h"

#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

#include <fcntl.h>
#include <unistd.h>

namespace {

int failures = 0;

void check(bool value, const char* message) {
    if (!value) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

raft::LogEntry make_entry(std::uint64_t index, std::uint64_t term, const std::string& command) {
    raft::LogEntry entry;
    entry.index = index;
    entry.term = term;
    entry.command = command;
    return entry;
}

std::vector<char> read_bytes(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

std::string make_temp_dir() {
    char pattern[] = "/tmp/logstore-test.XXXXXX";
    char* made = ::mkdtemp(pattern);
    return made == nullptr ? std::string() : std::string(made);
}

// 外部把文件截短（模拟写到一半就没电）：故意绕开 LogStore，直接动文件。
void cut_file(const std::string& path, off_t size) {
    (void)::truncate(path.c_str(), size);
}

// 外部在文件末尾追加垃圾字节（模拟只写了半个头部就断了）。
void append_bytes(const std::string& path, const std::vector<char>& bytes) {
    const int fd = ::open(path.c_str(), O_WRONLY | O_APPEND);
    if (fd < 0) {
        return;
    }
    (void)::write(fd, bytes.data(), bytes.size());
    ::close(fd);
}

// 外部翻转文件里一个字节（模拟磁盘位翻转 / 运行期被别的进程改坏）。
void flip_byte(const std::string& path, off_t offset) {
    const int fd = ::open(path.c_str(), O_RDWR);
    if (fd < 0) {
        return;
    }
    char byte = 0;
    if (::pread(fd, &byte, 1, offset) == 1) {
        byte = static_cast<char>(byte ^ 0x01);
        (void)::pwrite(fd, &byte, 1, offset);
    }
    ::close(fd);
}

// 单次调用模式：给 logstore_fault_test.py 用。
// 每个模式只做一件事：成功打印一行结果并返回 0，失败打印原因并返回 1。
int helper(int argc, char** argv) {
    const std::string mode = argv[1];
    std::string error;

    if (mode == "create" && argc == 3) {
        raft::LogStore store(argv[2]);
        if (!store.create(error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << "created\n";
        return 0;
    }
    if (mode == "append" && argc == 4) {
        raft::LogStore store(argv[2]);
        if (!store.open(error)) {
            std::cerr << error << '\n';
            return 1;
        }
        const std::uint64_t count = std::stoull(argv[3]);
        std::vector<raft::LogEntry> entries;
        std::uint64_t index = store.last_index() + 1;
        for (std::uint64_t i = 0; i < count; ++i, ++index) {
            entries.push_back(make_entry(index, 1, "e" + std::to_string(index)));
        }
        if (!store.append_batch(entries, error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << "appended " << store.last_index() << '\n';
        return 0;
    }
    if (mode == "truncate" && argc == 4) {
        raft::LogStore store(argv[2]);
        if (!store.open(error)) {
            std::cerr << error << '\n';
            return 1;
        }
        if (!store.truncate_suffix(std::stoull(argv[3]), error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << "truncated " << store.last_index() << '\n';
        return 0;
    }
    if (mode == "last" && argc == 3) {
        raft::LogStore store(argv[2]);
        if (!store.open(error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << store.last_index() << '\n';
        return 0;
    }
    if (mode == "verify" && argc == 4) {
        raft::LogStore store(argv[2]);
        if (!store.open(error)) {
            std::cerr << error << '\n';
            return 1;
        }
        const std::uint64_t count = std::stoull(argv[3]);
        if (store.last_index() != count) {
            std::cerr << "last_index " << store.last_index() << " != " << count << '\n';
            return 1;
        }
        for (std::uint64_t index = 1; index <= count; ++index) {
            raft::LogEntry entry;
            if (!store.get(index, entry, error)) {
                std::cerr << error << '\n';
                return 1;
            }
            if (entry.index != index || entry.term != 1 ||
                entry.command != "e" + std::to_string(index)) {
                std::cerr << "entry " << index << " mismatch\n";
                return 1;
            }
        }
        std::cout << "verified " << count << '\n';
        return 0;
    }
    if (mode == "get" && argc == 4) {
        raft::LogStore store(argv[2]);
        if (!store.open(error)) {
            std::cerr << error << '\n';
            return 1;
        }
        raft::LogEntry entry;
        if (!store.get(std::stoull(argv[3]), entry, error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << entry.index << ' ' << entry.term << ' ' << entry.command.size() << '\n';
        return 0;
    }

    std::cerr << "用法: test_logstore [create|append|truncate|last|verify|get ...]\n";
    return 2;
}

}  // namespace

int main(int argc, char** argv) {
    // 故障注入脚本用的单次调用模式
    if (argc >= 2) {
        return helper(argc, argv);
    }

    const std::string root = make_temp_dir();
    if (root.empty()) {
        std::cerr << "mkdtemp failed\n";
        return 2;
    }

    std::string error;

    // ---- 1. create / open 的基本契约 ----
    {
        const std::string directory = root + "/basic";
        std::filesystem::create_directory(directory);
        const std::string path = directory + "/raft.log";

        raft::LogStore store(directory);
        check(store.create(error), "create 新建空日志");
        check(error.empty(), "成功后必须清空 error");
        check(store.good(), "create 后 good()");
        check(store.last_index() == 0, "空日志 last_index()==0");
        check(std::filesystem::file_size(path) == 0, "新建的 raft.log 是空文件");

        check(!store.create(error), "同一个对象再次 create 必须失败");
        check(!error.empty(), "重复 create 必须给出原因");

        raft::LogStore second(directory);
        check(!second.create(error), "已有日志时另开对象 create 必须失败（O_EXCL，不许覆盖）");
    }
    {
        raft::LogStore store(root + "/basic");
        check(store.open(error), "重开空日志必须成功");
        check(error.empty(), "重开成功后 error 为空");
        check(store.last_index() == 0, "空日志重开 last_index 仍为 0");
    }
    {
        raft::LogStore store(root + "/absent");
        check(!store.open(error), "文件缺失时 open 必须失败（不许偷偷新建）");
        check(!error.empty(), "open 失败必须给出原因");
        check(!store.good(), "open 失败后 good() 为假");
    }
    {
        raft::LogStore store(root + "/no-such-dir");
        check(!store.create(error), "目录不存在时 create 必须失败（不许偷偷建目录）");
    }
    {
        // raft.log 是符号链接：必须拒绝，否则会写到链接指向的地方
        const std::string directory = root + "/symlink";
        std::filesystem::create_directory(directory);
        (void)::symlink("/tmp", (directory + "/raft.log").c_str());
        raft::LogStore store(directory);
        check(!store.open(error), "raft.log 是符号链接时必须拒绝（open）");
        check(!store.create(error), "raft.log 是符号链接时必须拒绝（create）");
    }
    {
        // raft.log 是目录：必须按"非普通文件"拒绝
        const std::string directory = root + "/dirfile";
        std::filesystem::create_directory(directory);
        std::filesystem::create_directory(directory + "/raft.log");
        raft::LogStore store(directory);
        check(!store.open(error), "raft.log 是目录时必须拒绝");
    }

    // ---- 2. 追加 / 查询 ----
    const std::string main_dir = root + "/main";
    std::filesystem::create_directory(main_dir);
    const std::string main_path = main_dir + "/raft.log";
    {
        raft::LogStore store(main_dir);
        check(store.create(error), "建主日志");

        std::vector<raft::LogEntry> batch;
        batch.push_back(make_entry(1, 3, "alpha"));
        batch.push_back(make_entry(2, 3, std::string("b\0c", 3)));
        batch.push_back(make_entry(3, 4, "gamma"));
        batch.push_back(make_entry(4, 4, ""));
        check(store.append_batch(batch, error), "批量追加 4 条（含二进制命令与空命令）");
        check(error.empty(), "批量成功后 error 为空");
        check(store.last_index() == 4, "批量追加后 last_index()==4");

        for (std::size_t i = 0; i < batch.size(); ++i) {
            raft::LogEntry out;
            out.command = "sentinel";
            check(store.get(i + 1, out, error), "get 必须能读回每条命令");
            check(out.index == batch[i].index && out.term == batch[i].term &&
                      out.command == batch[i].command,
                  "get 读回的 index/term/command 与写入一致");
        }

        // 参数越界：必须失败、不许 poison、不许改动输出参数
        raft::LogEntry probe;
        probe.index = 77;
        probe.term = 88;
        probe.command = "keep";
        check(!store.get(0, probe, error), "get(0) 必须失败");
        check(!store.get(5, probe, error), "get(last+1) 必须失败");
        check(probe.index == 77 && probe.term == 88 && probe.command == "keep",
              "get 失败时不许改动输出参数");
        check(!error.empty(), "get 失败必须给出原因");
        check(store.good(), "get 参数错误不许 poison");

        std::uint64_t term = 999;
        check(store.term_at(1, term, error) && term == 3, "term_at(1)==3");
        check(store.term_at(4, term, error) && term == 4, "term_at(4)==4");
        term = 999;
        check(!store.term_at(5, term, error), "term_at(last+1) 必须失败");
        check(term == 999, "term_at 失败时不许改动输出参数");
        check(store.good(), "term_at 参数错误不许 poison");

        // 校验失败：不改对象；改好以后再写必须成功
        check(!store.append(make_entry(6, 4, "gap"), error), "index 不连续必须拒绝");
        check(store.good(), "index 校验失败不许 poison");
        check(!store.append(make_entry(5, 0, "zero"), error), "term=0 必须拒绝");
        check(store.good(), "term=0 不许 poison");
        check(!store.append(make_entry(5, 3, "back"), error), "term 下降必须拒绝");
        check(store.good(), "term 下降不许 poison");

        // 批次内有一条非法：整批都不许写
        std::vector<raft::LogEntry> mixed;
        mixed.push_back(make_entry(5, 4, "ok"));
        mixed.push_back(make_entry(8, 4, "gap"));
        const auto size_before = std::filesystem::file_size(main_path);
        check(!store.append_batch(mixed, error), "批次内非法必须整批拒绝");
        check(std::filesystem::file_size(main_path) == size_before,
              "整批拒绝时不许写入任何字节");

        check(store.append(make_entry(5, 4, "ok"), error), "改正后追加必须成功");
        check(store.last_index() == 5, "改正后 last_index()==5");

        std::vector<raft::LogEntry> empty;
        check(store.append_batch(empty, error), "空批次必须成功");
        check(error.empty(), "空批次成功后 error 为空");
        check(store.last_index() == 5, "空批次不改变 last_index");
    }

    // ---- 3. 磁盘格式：黄金字节（大端 + 双 CRC）----
    // CRC 黄金值用独立实现（Python zlib.crc32）算好写死在这里，
    // 避免"测试和实现共用同一个 crc32 函数"导致的自我印证。
    {
        const auto bytes = read_bytes(main_path);
        check(bytes.size() >= 41, "文件至少包含第一条记录（41 字节）");
        if (bytes.size() >= 41) {
            const unsigned char expect[41] = {
                // 头部 28 字节
                0x52, 0x4C, 0x4F, 0x47,                          // "RLOG"
                0x00, 0x00, 0x00, 0x01,                          // version 1
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01,  // index 1
                0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03,  // term 3
                0x00, 0x00, 0x00, 0x05,                          // command_size 5
                0xBD, 0x90, 0x8F, 0x35,                          // 头部 CRC
                'a', 'l', 'p', 'h', 'a',                         // 命令
                0xD0, 0xE0, 0x39, 0x6A,                          // 命令 CRC
            };
            bool same = true;
            for (std::size_t i = 0; i < 41; ++i) {
                if (static_cast<unsigned char>(bytes[i]) != expect[i]) {
                    same = false;
                }
            }
            check(same, "第一条记录的 41 字节与黄金字节完全一致");
        }
    }

    // ---- 4. 重开：从磁盘重建索引 ----
    {
        raft::LogStore store(main_dir);
        check(store.open(error), "重开主日志");
        check(error.empty(), "重开成功后 error 为空");
        check(store.last_index() == 5, "重开后 last_index()==5");
        raft::LogEntry out;
        check(store.get(2, out, error) && out.command == std::string("b\0c", 3),
              "重开后二进制命令读回正确");
        check(store.get(4, out, error) && out.command.empty(), "重开后空命令读回正确");
        std::uint64_t term = 0;
        check(store.term_at(5, term, error) && term == 4, "重开后 term_at(5)==4");
    }

    // ---- 5. 命令长度上限 ----
    {
        const std::string directory = root + "/limit";
        std::filesystem::create_directory(directory);
        raft::LogStore store(directory);
        check(store.create(error), "建上限测试日志");

        raft::LogEntry big = make_entry(1, 1, "");
        big.command.assign(raft::LogStore::MAX_COMMAND_SIZE + 1, 'x');
        check(!store.append(big, error), "超过 MAX_COMMAND_SIZE 必须拒绝");
        check(store.good(), "超限不许 poison");

        big.command.assign(raft::LogStore::MAX_COMMAND_SIZE, 'y');
        check(store.append(big, error), "恰好 MAX_COMMAND_SIZE 必须接受");
        raft::LogEntry out;
        check(store.get(1, out, error), "超长命令必须能读回");
        check(out.command.size() == raft::LogStore::MAX_COMMAND_SIZE &&
                  out.command.front() == 'y' && out.command.back() == 'y',
              "超长命令内容完整");
    }

    // ---- 6. truncate_suffix ----
    {
        const std::string directory = root + "/trunc";
        std::filesystem::create_directory(directory);
        raft::LogStore store(directory);
        check(store.create(error), "建截断测试日志");

        std::vector<raft::LogEntry> batch;
        for (std::uint64_t i = 1; i <= 5; ++i) {
            batch.push_back(make_entry(i, 4, "t-" + std::to_string(i)));
        }
        check(store.append_batch(batch, error), "写 5 条");

        check(store.truncate_suffix(6, error), "truncate(last+1) 是成功的无操作");
        check(error.empty(), "无操作成功后 error 为空");
        check(store.last_index() == 5, "无操作不改变 last_index");
        check(!store.truncate_suffix(0, error), "truncate(0) 必须拒绝");
        check(!store.truncate_suffix(7, error), "truncate(last+2) 必须拒绝");
        check(store.good(), "截断参数错误不许 poison");

        check(store.truncate_suffix(3, error), "删除 index 3 及以后");
        check(store.last_index() == 2, "删除后 last_index()==2");

        std::vector<raft::LogEntry> next;
        next.push_back(make_entry(3, 9, "n-3"));
        next.push_back(make_entry(4, 9, "n-4"));
        check(store.append_batch(next, error), "在原位置继续追加（不许有空洞）");
        check(store.last_index() == 4, "续写后 last_index()==4");
    }
    {
        raft::LogStore store(root + "/trunc");
        check(store.open(error), "重开截断日志");
        check(store.last_index() == 4, "重开后 last_index()==4");
        raft::LogEntry out;
        check(store.get(2, out, error) && out.command == "t-2", "第 2 条仍是 t-2");
        check(store.get(3, out, error) && out.command == "n-3", "第 3 条已被 n-3 覆盖");
        std::uint64_t term = 0;
        check(store.term_at(3, term, error) && term == 9, "第 3 条 term 已更新为 9");
        check(std::filesystem::file_size(root + "/trunc/raft.log") == 39 + 39 + 39 + 39,
              "文件里恰好只有 4 条记录");
    }
    {
        raft::LogStore store(root + "/trunc");
        check(store.open(error), "再次重开");
        check(store.truncate_suffix(1, error), "truncate(1) 清空日志");
        check(store.last_index() == 0, "清空后 last_index()==0");
        check(store.append(make_entry(1, 7, "again"), error), "清空后可以重新从 1 开始写");
    }
    {
        raft::LogStore store(root + "/trunc");
        check(store.open(error), "清空重写后重开");
        check(store.last_index() == 1, "清空重写后 last_index()==1");
        raft::LogEntry out;
        check(store.get(1, out, error) && out.command == "again", "新第 1 条内容正确");
        check(std::filesystem::file_size(root + "/trunc/raft.log") == 41,
              "清空重写后文件只剩一条记录");
    }

    // ---- 7. 残尾：截断修复 ----
    {
        const std::string directory = root + "/torn";
        std::filesystem::create_directory(directory);
        const std::string path = directory + "/raft.log";
        {
            raft::LogStore store(directory);
            check(store.create(error), "建残尾测试日志");
            std::vector<raft::LogEntry> batch;
            for (std::uint64_t i = 1; i <= 3; ++i) {
                batch.push_back(make_entry(i, 1, "r-" + std::to_string(i)));
            }
            check(store.append_batch(batch, error), "写 3 条");
        }
        const auto full = std::filesystem::file_size(path);
        cut_file(path, static_cast<off_t>(full - 10));  // 最后一条记录被砍掉 10 字节
        {
            raft::LogStore store(directory);
            check(store.open(error), "残尾必须能修复");
            check(store.last_index() == 2, "残尾截掉后 last_index()==2");
            check(std::filesystem::file_size(path) == full - 39,
                  "文件被截到最后一条完整记录末尾");
        }
        append_bytes(path, {'R', 'L', 'O', 'G'});  // 只有 4 字节，远不到 32 字节头部
        {
            raft::LogStore store(directory);
            check(store.open(error), "不足一个头部的垃圾尾巴必须能修复");
            check(store.last_index() == 2, "垃圾尾巴不影响已有记录");
            check(std::filesystem::file_size(path) == full - 39, "垃圾尾巴已被清掉");
        }
    }

    // ---- 8. 完整损坏：必须拒绝启动，而且不许改动文件 ----
    {
        const std::string directory = root + "/corrupt-head";
        std::filesystem::create_directory(directory);
        const std::string path = directory + "/raft.log";
        {
            raft::LogStore store(directory);
            check(store.create(error), "建损坏测试日志");
            std::vector<raft::LogEntry> batch;
            for (std::uint64_t i = 1; i <= 3; ++i) {
                batch.push_back(make_entry(i, 1, "c-" + std::to_string(i)));
            }
            check(store.append_batch(batch, error), "写 3 条");
        }
        const auto full = std::filesystem::file_size(path);
        flip_byte(path, 39 + 28);  // 第 2 条记录的头部 CRC 字段
        {
            raft::LogStore store(directory);
            check(!store.open(error), "完整记录的头部损坏必须拒绝启动");
            check(!error.empty(), "拒绝时必须给出原因");
            check(!store.good(), "拒绝后对象不可用");
        }
        check(std::filesystem::file_size(path) == full, "拒绝损坏时不许截断文件");
    }
    {
        const std::string directory = root + "/corrupt-tail";
        std::filesystem::create_directory(directory);
        const std::string path = directory + "/raft.log";
        {
            raft::LogStore store(directory);
            check(store.create(error), "建尾部损坏测试日志");
            std::vector<raft::LogEntry> batch;
            for (std::uint64_t i = 1; i <= 3; ++i) {
                batch.push_back(make_entry(i, 1, "d-" + std::to_string(i)));
            }
            check(store.append_batch(batch, error), "写 3 条");
        }
        const auto full = std::filesystem::file_size(path);
        flip_byte(path, 78 + 32);  // 第 3 条（最后一条完整记录）的命令首字节
        {
            raft::LogStore store(directory);
            check(!store.open(error), "最后一条完整记录损坏也不能当残尾（必须拒绝）");
        }
        check(std::filesystem::file_size(path) == full, "拒绝损坏时不许截断文件");
    }

    // ---- 9. 运行期读损坏：保守 poison ----
    {
        const std::string directory = root + "/runtime";
        std::filesystem::create_directory(directory);
        const std::string path = directory + "/raft.log";
        raft::LogStore store(directory);
        check(store.create(error), "建运行期损坏测试日志");
        check(store.append(make_entry(1, 3, "alpha"), error), "写第一条");
        check(store.append(make_entry(2, 3, "beta"), error), "写第二条");

        raft::LogEntry out;
        check(store.get(1, out, error), "损坏前能读第一条");
        flip_byte(path, 41 + 32);  // 第二条记录的命令首字节
        check(!store.get(2, out, error), "命令被改坏后 get 必须失败");
        check(!store.good(), "运行期读损坏必须 poison");
        check(!store.append(make_entry(3, 3, "more"), error), "poison 后必须拒绝追加");
    }

    // ---- 10. uint64 边界任期 ----
    {
        const std::string directory = root + "/maxterm";
        std::filesystem::create_directory(directory);
        raft::LogStore store(directory);
        check(store.create(error), "建边界任期日志");
        check(store.append(make_entry(1, std::numeric_limits<std::uint64_t>::max(), "max"),
                           error),
              "term 为 uint64 最大值可以写入");
        raft::LogEntry out;
        check(store.get(1, out, error) &&
                  out.term == std::numeric_limits<std::uint64_t>::max(),
              "uint64 最大值能读回");
    }

    std::filesystem::remove_all(root);

    if (failures != 0) {
        std::cerr << failures << " failures\n";
        return 1;
    }
    std::cout << "logstore tests PASS\n";
    return 0;
}
