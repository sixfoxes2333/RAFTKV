// test_recovery.cpp —— Bitcask 状态文件恢复 + 残损尾截断（M2e）
//
// 【核心恢复策略】
//   1) 正常记录：按顺序扫描；SET 更新 keydir，DEL 从 keydir 删除
//   2) Incomplete（文件末尾记录没写完整）：安全截断到这条记录起点，继续启动
//   3) Corrupt（完整记录 CRC 错 / type 非法 / 长度越界）：拒绝启动，不静默丢数据
//
// 为什么要区分 Incomplete 与 Corrupt：
//   · 末尾少几个字节通常是崩溃/短写造成的 torn tail，前面的记录仍可信，可以截掉尾巴
//   · 完整记录 CRC 错可能是磁盘损坏；若直接截断，可能在没人知道的情况下丢掉真实数据
//
// 【当前预期】
//   M2e 实现前：本测试应该 FAIL（构造函数只校验文件头，没有 recover，重开后 keydir 为空）
//   M2e 实现后：全部 PASS
#include "storage/storage.h"
#include "storage/record.h"
#include <cstdio>
#include <fstream>
#include <iostream>
#include <string>

static int g_failed = 0;

#define CHECK(cond)                                                             \
    do {                                                                        \
        if (cond)                                                               \
            std::cout << "  ok   : " #cond << std::endl;                        \
        else {                                                                  \
            std::cout << "  FAIL : " #cond "   (line " << __LINE__ << ")"       \
                      << std::endl;                                              \
            g_failed++;                                                         \
        }                                                                       \
    } while (0)

static std::streamoff file_size(const char *path)
{
    std::ifstream f(path,std::ios::binary | std::ios::ate);
    if(!f)
    {
        return -1;
    }
    return static_cast<std::streamoff>(f.tellg());
}

static bool append_bytes(const char *path,const std::string &bytes)
{
    std::ofstream f(path,std::ios::binary | std::ios::app);
    if(!f)
    {
        return false;
    }
    f.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
    f.flush();
    return f.good();
}

static bool flip_byte(const char *path,std::streamoff offset)
{
    std::fstream f(path,std::ios::binary | std::ios::in | std::ios::out);
    if(!f)
    {
        return false;
    }

    f.seekg(offset);
    char byte = 0;
    f.read(&byte,1);
    if(!f)
    {
        return false;
    }

    byte = static_cast<char>(byte ^ 0xFF);
    f.seekp(offset);
    f.write(&byte,1);
    f.flush();
    return f.good();
}

static bool write_bytes(const char *path,std::streamoff offset,const std::string &bytes)
{
    std::fstream f(path,std::ios::binary | std::ios::in | std::ios::out);
    if(!f)
    {
        return false;
    }

    f.seekp(offset);
    f.write(bytes.data(),static_cast<std::streamsize>(bytes.size()));
    f.flush();
    return f.good();
}

static void check_value(kv::Storage &s,const std::string &key,const std::string &expected)
{
    std::string out = "sentinel";
    CHECK(s.get(key,out));
    CHECK(out == expected);
}

static void check_missing(kv::Storage &s,const std::string &key)
{
    std::string out = "sentinel";
    CHECK(!s.get(key,out));
    CHECK(out == "sentinel");   //未命中不能污染输出参数
}

int main()
{
    //用 encode_record 推导尺寸：格式从 v1(13B 头) 升到 v2(17B 头) 时，这些断言不用改
    const std::streamoff FILE_HDR = 8;   //magic(4) + version(4)
    const std::streamoff REC_A = static_cast<std::streamoff>(kv::encode_record(kv::RecordType::Set,"a","1").size());
    const std::streamoff REC_B = static_cast<std::streamoff>(kv::encode_record(kv::RecordType::Set,"b","2").size());
    const std::streamoff REC_C = static_cast<std::streamoff>(kv::encode_record(kv::RecordType::Set,"c","3").size());
    const std::streamoff REC_DEL_B = static_cast<std::streamoff>(kv::encode_record(kv::RecordType::Del,"b","").size());
    std::streamoff short_body_len = 0;   //[4] 里那段残损 body 的长度（块内计算，块外使用）
    // -------------------------------------------------------------------------
    std::cout << "[1] normal recovery : overwrite + tombstone + append after reopen" << std::endl;
    const char *PATH_NORMAL = "/tmp/kv_recover_normal.log";
    std::remove(PATH_NORMAL);

    //磁盘顺序：SET a=1 / SET b=2 / SET a=9 / DEL b
    {
        kv::Storage s(PATH_NORMAL);
        CHECK(s.good());
        CHECK(s.put("a","1"));
        CHECK(s.put("b","2"));
        CHECK(s.put("a","9"));
        CHECK(s.del("b"));
        CHECK(s.key_count() == 1);
    }
    //8B 头 + SET(a,1) + SET(b,2) + SET(a,9) + DEL(b)
    CHECK(file_size(PATH_NORMAL) == FILE_HDR + REC_A + REC_B + REC_A + REC_DEL_B);

    //第一次重启：recover 必须让最新 SET 胜出，并让墓碑继续生效
    {
        kv::Storage s(PATH_NORMAL);
        CHECK(s.good());
        CHECK(s.key_count() == 1);
        check_value(s,"a","9");
        check_missing(s,"b");

        //恢复后仍可继续追加，偏移必须接在现有文件末尾
        CHECK(s.put("c","3"));
        CHECK(s.key_count() == 2);
        check_value(s,"c","3");
    }
    CHECK(file_size(PATH_NORMAL) == FILE_HDR + REC_A + REC_B + REC_A + REC_DEL_B + REC_C);   //再加 SET(c,3)

    //第二次重启：验证 recover 后追加的数据也能再次恢复
    {
        kv::Storage s(PATH_NORMAL);
        CHECK(s.good());
        CHECK(s.key_count() == 2);
        check_value(s,"a","9");
        check_value(s,"c","3");
        check_missing(s,"b");
    }

    // -------------------------------------------------------------------------
    std::cout << "[2] header-only file" << std::endl;
    const char *PATH_EMPTY = "/tmp/kv_recover_empty.log";
    std::remove(PATH_EMPTY);
    {
        kv::Storage s(PATH_EMPTY);   //只创建 8B 文件头
        CHECK(s.good());
    }
    CHECK(file_size(PATH_EMPTY) == FILE_HDR);
    {
        kv::Storage s(PATH_EMPTY);
        CHECK(s.good());
        CHECK(s.key_count() == 0);
    }

    // -------------------------------------------------------------------------
    std::cout << "[3] incomplete tail : partial record header" << std::endl;
    const char *PATH_SHORT_HEADER = "/tmp/kv_recover_short_header.log";
    std::remove(PATH_SHORT_HEADER);
    {
        kv::Storage s(PATH_SHORT_HEADER);
        CHECK(s.good());
        CHECK(s.put("a","1"));
        CHECK(s.put("b","2"));
    }
    CHECK(file_size(PATH_SHORT_HEADER) == FILE_HDR + REC_A + REC_B);

    {
        std::string rec = kv::encode_record(kv::RecordType::Set,"tail","value");
        CHECK(append_bytes(PATH_SHORT_HEADER,rec.substr(0,7)));   //连 13B 头都不完整
    }
    CHECK(file_size(PATH_SHORT_HEADER) == FILE_HDR + REC_A + REC_B + 7);   //末尾多 7B（连记录头都不完整）

    {
        kv::Storage s(PATH_SHORT_HEADER);
        CHECK(s.good());                         //残损尾可修复，不应拒绝启动
        CHECK(file_size(PATH_SHORT_HEADER) == FILE_HDR + REC_A + REC_B); //必须截回最后一个好记录的末尾
        CHECK(s.key_count() == 2);
        check_value(s,"a","1");
        check_value(s,"b","2");
        check_missing(s,"tail");

        //截断后继续追加，证明文件仍可用
        CHECK(s.put("c","3"));
    }
    CHECK(file_size(PATH_SHORT_HEADER) == FILE_HDR + REC_A + REC_B + REC_C);
    {
        kv::Storage s(PATH_SHORT_HEADER);
        CHECK(s.good());
        CHECK(s.key_count() == 3);
        check_value(s,"c","3");
    }

    // -------------------------------------------------------------------------
    std::cout << "[4] incomplete tail : full header + partial body" << std::endl;
    const char *PATH_SHORT_BODY = "/tmp/kv_recover_short_body.log";
    std::remove(PATH_SHORT_BODY);
    {
        kv::Storage s(PATH_SHORT_BODY);
        CHECK(s.good());
        CHECK(s.put("a","1"));
    }
    CHECK(file_size(PATH_SHORT_BODY) == FILE_HDR + REC_A);

    {
        std::string rec = kv::encode_record(kv::RecordType::Set,"tail","value");
        CHECK(rec.size() > 2);
        //记录头完整、body 少 2 字节（真实残尾的典型形态）
        short_body_len = static_cast<std::streamoff>(rec.size()) - 2;
        CHECK(append_bytes(PATH_SHORT_BODY,rec.substr(0,static_cast<size_t>(short_body_len))));
    }
    CHECK(file_size(PATH_SHORT_BODY) == FILE_HDR + REC_A + short_body_len);

    {
        kv::Storage s(PATH_SHORT_BODY);
        CHECK(s.good());
        CHECK(file_size(PATH_SHORT_BODY) == FILE_HDR + REC_A);
        CHECK(s.key_count() == 1);
        check_value(s,"a","1");
        check_missing(s,"tail");
    }

    // -------------------------------------------------------------------------
    std::cout << "[5] complete last record with bad CRC : fail closed" << std::endl;
    const char *PATH_BAD_LAST = "/tmp/kv_recover_bad_last.log";
    std::remove(PATH_BAD_LAST);
    {
        kv::Storage s(PATH_BAD_LAST);
        CHECK(s.good());
        CHECK(s.put("a","1"));
        CHECK(s.put("b","2"));
    }
    CHECK(file_size(PATH_BAD_LAST) == FILE_HDR + REC_A + REC_B);
    CHECK(flip_byte(PATH_BAD_LAST,file_size(PATH_BAD_LAST) - 1));   //翻转最后一条 b 记录的最后一个字节
    {
        kv::Storage s(PATH_BAD_LAST);
        CHECK(!s.good());                       //完整记录 CRC 错：拒绝启动，不静默截断
    }
    CHECK(file_size(PATH_BAD_LAST) == FILE_HDR + REC_A + REC_B);      //损坏现场保留，便于人工检查/恢复

    // -------------------------------------------------------------------------
    std::cout << "[6] middle record with bad CRC : never truncate valid suffix" << std::endl;
    const char *PATH_BAD_MIDDLE = "/tmp/kv_recover_bad_middle.log";
    std::remove(PATH_BAD_MIDDLE);
    {
        kv::Storage s(PATH_BAD_MIDDLE);
        CHECK(s.good());
        CHECK(s.put("a","1"));                //offset 8，15B
        CHECK(s.put("b","2"));                //offset 23，15B
        CHECK(s.put("c","3"));                //offset 38，15B
    }
    CHECK(file_size(PATH_BAD_MIDDLE) == FILE_HDR + REC_A + REC_B + REC_C);
    CHECK(flip_byte(PATH_BAD_MIDDLE,FILE_HDR + REC_A + REC_B - 1));     //破坏中间 b 记录的最后一个字节
    {
        kv::Storage s(PATH_BAD_MIDDLE);
        CHECK(!s.good());                       //不能把 b 以后合法的 c 一起静默截掉
    }
    CHECK(file_size(PATH_BAD_MIDDLE) == FILE_HDR + REC_A + REC_B + REC_C);

    // -------------------------------------------------------------------------
    std::cout << "[7] length field corrupted mid-file : must refuse (not truncate)" << std::endl;
    const char *PATH_BAD_LEN = "/tmp/kv_recover_bad_len.log";
    std::remove(PATH_BAD_LEN);
    {
        kv::Storage s(PATH_BAD_LEN);
        CHECK(s.good());
        CHECK(s.put("a","1"));
        CHECK(s.put("b","2"));
        CHECK(s.put("c","3"));
    }
    const std::streamoff bad_len_size = FILE_HDR + REC_A + REC_B + REC_C;
    CHECK(file_size(PATH_BAD_LEN) == bad_len_size);

    //把中间 b 记录的 val_len 字段改成 257（v2 里 val_len 位于记录头内偏移 13）
    //头校验和随即失配 → 必须判 Corrupt；绝对不能因为"长度超出文件剩余"就当成残尾截断
    {
        std::string vlen_be;
        vlen_be.push_back(0x00);
        vlen_be.push_back(0x00);
        vlen_be.push_back(0x01);
        vlen_be.push_back(0x01);   //257
        CHECK(write_bytes(PATH_BAD_LEN,FILE_HDR + REC_A + 13,vlen_be));
    }
    {
        kv::Storage s(PATH_BAD_LEN);
        CHECK(!s.good());                              //拒绝启动
    }
    CHECK(file_size(PATH_BAD_LEN) == bad_len_size);     //合法记录 c 必须保留

    // -------------------------------------------------------------------------
    std::cout << "[8] header checksum corrupted : must refuse" << std::endl;
    const char *PATH_BAD_HCRC = "/tmp/kv_recover_bad_hcrc.log";
    std::remove(PATH_BAD_HCRC);
    {
        kv::Storage s(PATH_BAD_HCRC);
        CHECK(s.good());
        CHECK(s.put("a","1"));
        CHECK(s.put("b","2"));
    }
    const std::streamoff bad_hcrc_size = FILE_HDR + REC_A + REC_B;
    CHECK(flip_byte(PATH_BAD_HCRC,FILE_HDR + 1));        //改坏第 2 条记录 header_crc 的第 2 字节
    {
        kv::Storage s(PATH_BAD_HCRC);
        CHECK(!s.good());
    }
    CHECK(file_size(PATH_BAD_HCRC) == bad_hcrc_size);

    // -------------------------------------------------------------------------
    std::cout << "[9] old format version (v1) : must refuse" << std::endl;
    const char *PATH_V1 = "/tmp/kv_recover_v1.log";
    std::remove(PATH_V1);
    {
        //手写一个 v1 文件头：magic "KVDB" + version = 1
        std::string hdr;
        hdr += "KVDB";
        hdr.push_back(0x00);
        hdr.push_back(0x00);
        hdr.push_back(0x00);
        hdr.push_back(0x01);
        CHECK(append_bytes(PATH_V1,hdr));
    }
    CHECK(file_size(PATH_V1) == 8);
    {
        kv::Storage s(PATH_V1);
        CHECK(!s.good());   //FORMAT_VERSION 已是 2，旧版本文件必须拒绝
    }
    CHECK(file_size(PATH_V1) == 8);   //拒绝时不能改动文件
    // -------------------------------------------------------------------------
    if(g_failed == 0)
    {
        std::cout << "\nall recovery tests passed" << std::endl;
        return 0;
    }

    std::cout << "\n" << g_failed << " recovery check(s) FAILED" << std::endl;
    return 1;
}
