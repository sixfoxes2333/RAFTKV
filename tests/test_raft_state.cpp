// test_raft_state.cpp —— Raft term/vote 持久化的单元测试（先红后绿）
//
// 【这个测试防的是什么】
//   Raft 要求：在回复"我投给谁 / 我任期多少"之前，term 与 vote 必须真的落盘。
//   否则重启后可能【同一任期投两票】→ 选举安全被破坏（可能选出日志不是最新的 leader
//   → 已提交的写丢失）。所以这个模块的失败语义和"保存成功"同等重要：
//
//   · 保存后重开必须读回同一个值
//   · 文件缺失 / 损坏 / 截断 / 多余字节 都必须明确失败，且不许改动输出参数
//   · 残留的 raft.state.tmp 不能影响正式状态
//   · 保存失败不能静默成功（.tmp 残留无害，但不能冒充已提交状态）
//
// 【为什么有 save/load 两个命令行模式】
//   raft_state_fault_test.py 需要用 LD_PRELOAD 注入 write/fsync 失败，
//   所以这个二进制要能被当作"一次保存/一次读取"来单独调用。
//
// 【为什么不用掉电验证】
//   真正的持久化差别只在掉电时暴露（ext4 约 5 秒提交一次日志，平时看不出差别）。
//   这里能验证的是：文件格式、失败语义、以及改名替换的可见性；
//   系统调用顺序（write → fsync(文件) → rename → fsync(目录)）由
//   raft_state_fault_test.py 之外的 strace 检查覆盖。
#include "raft/state.h"

#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <iterator>
#include <limits>
#include <string>
#include <vector>

namespace {

int failures = 0;

void check(bool value, const char* message) {
    if (!value) {
        ++failures;
        std::cerr << "FAIL: " << message << '\n';
    }
}

std::vector<char> read_bytes(const std::string& path) {
    std::ifstream file(path, std::ios::binary);
    return {std::istreambuf_iterator<char>(file), std::istreambuf_iterator<char>()};
}

void write_bytes(const std::string& path, const std::vector<char>& bytes) {
    std::ofstream file(path, std::ios::binary | std::ios::trunc);
    file.write(bytes.data(), static_cast<std::streamsize>(bytes.size()));
}

}  // namespace

int main(int argc, char** argv) {
    // 给故障注入测试用的单次调用模式
    if (argc == 5 && std::string(argv[1]) == "save") {
        raft::StateStore store(argv[2]);
        std::string error;
        if (!store.save({std::stoull(argv[3]), std::stoull(argv[4])}, error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << "saved\n";
        return 0;
    }
    if (argc == 3 && std::string(argv[1]) == "load") {
        raft::StateStore store(argv[2]);
        raft::HardState state;
        std::string error;
        if (!store.load(state, error)) {
            std::cerr << error << '\n';
            return 1;
        }
        std::cout << state.current_term << ' ' << state.voted_for << '\n';
        return 0;
    }
    if (argc != 1) {
        std::cerr << "用法: test_raft_state [save <dir> <term> <vote> | load <dir>]\n";
        return 2;
    }

    char pattern[] = "/tmp/raft-state-test.XXXXXX";
    char* made = ::mkdtemp(pattern);
    if (made == nullptr) {
        std::cerr << "mkdtemp failed\n";
        return 2;
    }

    const std::string directory = made;
    const std::string path = directory + "/raft.state";
    const std::string temp_path = directory + "/raft.state.tmp";
    raft::StateStore store(directory);
    std::string error;
    raft::HardState out{99, 88};  // 故意放非零值：验证失败时输出参数不被改动

    // ---- 基本读写 ----
    check(!store.load(out, error), "文件缺失必须失败（不许偷偷当 term=0）");
    check(out.current_term == 99 && out.voted_for == 88, "失败时不许改动输出参数");
    check(!error.empty(), "失败必须给出原因");

    check(store.save({}, error), "保存初始状态");
    check(error.empty(), "成功后必须清空 error");
    check(store.load(out, error) && out.current_term == 0 && out.voted_for == 0,
          "初始状态能读回");

    check(store.save({3, 2}, error), "保存 term=3 vote=2");
    raft::StateStore reopened(directory);
    check(reopened.load(out, error) && out.current_term == 3 && out.voted_for == 2,
          "重新构造对象后读到同一对值");
    check(reopened.save({4, 0}, error), "任期前进并清空投票");
    check(store.load(out, error) && out.current_term == 4 && out.voted_for == 0,
          "最新值生效");

    // ---- 磁盘格式（大端 + 黄金值）----
    check(store.save({0x0102030405060708ULL, 0x1112131415161718ULL}, error),
          "保存 64 位边界值");
    const auto golden = read_bytes(path);
    check(golden.size() == 28, "文件长度固定 28 字节");
    if (golden.size() != 28) {
        std::filesystem::remove_all(directory);
        return 1;
    }
    check(std::string(golden.begin(), golden.begin() + 4) == "RFST", "magic 是 RFST");
    check(static_cast<unsigned char>(golden[7]) == 1, "version 用大端");
    for (unsigned i = 0; i < 8; ++i) {
        check(static_cast<unsigned char>(golden[8 + i]) == i + 1, "term 用大端逐字节写出");
        check(static_cast<unsigned char>(golden[16 + i]) == 0x11 + i, "vote 用大端逐字节写出");
    }
    check(store.load(out, error) && out.current_term == 0x0102030405060708ULL &&
              out.voted_for == 0x1112131415161718ULL,
          "64 位边界值能往返");
    check(store.save({std::numeric_limits<std::uint64_t>::max(),
                      std::numeric_limits<std::uint64_t>::max()}, error),
          "保存 uint64 最大值");
    check(store.load(out, error) &&
              out.current_term == std::numeric_limits<std::uint64_t>::max() &&
              out.voted_for == std::numeric_limits<std::uint64_t>::max(),
          "uint64 最大值能读回");

    // ---- 损坏与截断：必须报错，不许"尽力恢复" ----
    auto reject = [&](const std::vector<char>& bytes, const char* message) {
        write_bytes(path, bytes);
        out = {99, 88};
        check(!store.load(out, error), message);
        check(out.current_term == 99 && out.voted_for == 88, "损坏时不许改动输出参数");
        check(!error.empty(), "损坏时必须给出原因");
    };
    reject({}, "空文件必须拒绝");
    reject(std::vector<char>(golden.begin(), golden.end() - 1), "截断的文件必须拒绝");
    auto bytes = golden;
    bytes.push_back('x');
    reject(bytes, "多余字节必须拒绝");
    bytes = golden;
    bytes[0] ^= 1;
    reject(bytes, "magic 被改必须拒绝");
    bytes = golden;
    bytes[7] = 2;
    reject(bytes, "未知版本必须拒绝");
    bytes = golden;
    bytes[8] ^= 1;
    reject(bytes, "内容被翻位必须拒绝（CRC）");
    bytes = golden;
    bytes[27] ^= 1;
    reject(bytes, "CRC 字段被翻位必须拒绝");

    // ---- 残留 tmp：不许影响正式状态，也不许被当成数据来源 ----
    write_bytes(path, golden);
    write_bytes(temp_path, {'x', 'y'});
    check(store.load(out, error) && out.current_term == 0x0102030405060708ULL,
          "残留 tmp 不能影响正式状态");
    check(store.save({7, 3}, error), "有残留 tmp 时仍能保存");
    check(store.load(out, error) && out.current_term == 7 && out.voted_for == 3,
          "保存后读到新值");
    check(!std::filesystem::exists(temp_path), "保存成功后 tmp 已被 rename 掉");

    // ---- 目录参数非法 ----
    raft::StateStore empty("");
    check(!empty.save({}, error) && !error.empty(), "空目录路径必须拒绝（save）");
    check(!empty.load(out, error), "空目录路径必须拒绝（load）");
    raft::StateStore absent(directory + "/absent");
    check(!absent.save({}, error), "目录不存在必须拒绝（不许偷偷创建节点目录）");

    // ---- 目标是目录：rename 必须失败并且明确报错 ----
    std::filesystem::remove(path);
    std::filesystem::create_directory(path);
    check(!store.load(out, error), "正式路径是目录时必须拒绝");
    check(!store.save({8, 1}, error), "rename 失败必须报错");
    check(std::filesystem::is_directory(path), "rename 失败不许破坏目标");

    std::filesystem::remove_all(directory);

    if (failures != 0) {
        std::cerr << failures << " failures\n";
        return 1;
    }
    std::cout << "raft state tests PASS\n";
    return 0;
}
