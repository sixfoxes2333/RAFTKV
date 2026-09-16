#include "storage/storage.h"
#include<fstream>
#include<iostream>
#include<iterator>
#include<string>
#include<cstdio>

//简易检查宏 程序持续跑完 打印失败在第几行  统一返回失败码
static int g_failed = 0;
#define CHECK(cond)                                                             \
    do {                                                                        \
        if (cond)                                                               \
            std::cout << "  ok   : " #cond << std::endl;                        \
        else {                                                                  \
            std::cout << "  FAIL : " #cond "   (line " << __LINE__ << ")"       \
                 << std::endl;                                                  \
            g_failed++;                                                         \
        }                                                                       \
    } while (0)



//十六进制显示
//将二进制字节流转换为 十六进制文本形式
static std::string hex(const std::string &s)
{
    static const char* digits = "0123456789abcdef";
    std::string out;
    for(unsigned char c : s )
    {
        out += digits[c >> 4];
        out += digits[c & 0x0F];
        out += ' ';
    }

    if(!out.empty())
    {
        out.pop_back();
    }
    return out;
}


int main()
{
    //测试 CRC32 标准校验值
    std::cout << "[1] CRC32 标准值" << std::endl;
    CHECK(kv::crc32("123456789",9) == 0xCBF43926u);

    //SET 记录编码
    std::cout << "[2] SET 记录编码" << std::endl;
    std::string r1 = kv::encode_record(kv::RecordType::Set,"a","1");
    CHECK(r1.size() == 15);
    CHECK(hex(r1) == "20 84 55 73 01 00 00 00 01 00 00 00 01 61 31");

    //DEL 记录编码
    std::cout << "[3] DEL 记录编码" << std::endl;
    std::string r2 = kv::encode_record(kv::RecordType::Del,"a","");
    CHECK(r2.size() == 14);
    CHECK(hex(r2) == "16 96 3a 20 02 00 00 00 01 00 00 00 00 61");

    //写文件
    std::cout << "[4] 写文件 / 读回 / 解码" << std::endl;
    const char* path = "/tmp/test_storage.log";
    {
        std::ofstream f(path,std::ios::binary);
        f.write(r1.data(),r1.size());
        f.write(r2.data(),r2.size());
    }

    std::ifstream f(path,std::ios::binary);
    std::string all((std::istreambuf_iterator<char>(f)),std::istreambuf_iterator<char>());
    CHECK(all.size() == r1.size() + r2.size());

    kv::Record rec;
    size_t consumed = 0;

    // 第一条
    CHECK(kv::decode_record(all.data(), all.size(), rec, consumed));
    CHECK(rec.type == kv::RecordType::Set);
    CHECK(rec.key == "a");
    CHECK(rec.value == "1");
    CHECK(consumed == 15);

    // 第二条：从 consumed 处继续
    CHECK(kv::decode_record(all.data() + consumed, all.size() - consumed, rec, consumed));
    CHECK(rec.type == kv::RecordType::Del);
    CHECK(rec.key == "a");
    CHECK(rec.value == "");
    CHECK(consumed == 14);

    //坏输入必须被拒绝
    std::cout << "[5] 坏输入拒绝" << std::endl;

    // 5a 只有 10 字节：连 13 字节的最小头都不够 → 第一道检查拦下
    CHECK(kv::decode_record(r1.data(), 10, rec, consumed) == false);

    // 5b 正好 13 字节：头够了，但 key+value 的字节还没到 → 第三道检查拦下
    CHECK(kv::decode_record(r1.data(), 13, rec, consumed) == false);

    // 5c 篡改 key_len 的第一个字节（偏移 5）：
    //    长度变成 0xFF000001，超过 MAX_KEY → 上限检查拦下
    std::string bad_len = r1;
    bad_len[5] = static_cast<char>(0xFF);
    CHECK(kv::decode_record(bad_len.data(), bad_len.size(), rec, consumed) == false);

    // 5d 篡改 value 的一个字节（偏移 14）：
    //    长度字段都合法，只有内容变了 → 必须由 CRC 拦下
    std::string bad_val = r1;
    bad_val[14] ^= 0x01;
    CHECK(kv::decode_record(bad_val.data(), bad_val.size(), rec, consumed) == false);


    std::cout << "[6] 文件头 " << std::endl;
    {
        //新建文件
        std::remove("/tmp/kv_test_a.log");
        {
            kv::Storage s("/tmp/kv_test_a.log");
            CHECK(s.good());
        }
        std::ifstream fa("/tmp/kv_test_a.log",std::ios::binary);
        char hdr[8] = {0};
        fa.read(hdr,8);

        CHECK(fa.gcount() == 8);
        CHECK(std::string(hdr,4) == "KVDB");
        CHECK(hdr[4] == 0 && hdr[5] == 0 && hdr[6] == 0 && hdr[7] == 1);

        //已有合法文件
        {
            kv::Storage s("/tmp/kv_test_a.log");
            CHECK(s.good());
        }

        //非 kv 文件
        {
            std::ofstream fb("/tmp/kv_test_b.log",std::ios::binary);
            fb << "this is not a kv data file";
        }

        {
            kv::Storage s("/tmp/kv_test_b.log");
            CHECK(!s.good());
        }

        //小于 8 字节的文件
        std::ofstream fc("/tmp/kv_test_c.log",std::ios::binary);
        fc << "abc";
        fc.close();
        kv::Storage s("/tmp/kv_test_c.log");
        CHECK(!s.good());


    }

    std::cout << "[7] put" << std::endl;
    const char *PATH_PUT = "/tmp/kv_test_put.log";
    std::remove(PATH_PUT);
    {
        kv::Storage s(PATH_PUT);
        CHECK(s.good());
        CHECK(s.put("a", "1"));
        CHECK(s.put("b", "2"));
        CHECK(s.put("a", "9"));        // 覆盖写：再追加一条，索引改指新的
        CHECK(s.key_count() == 2);     // 索引里仍然只有 a、b 两个 key
        CHECK(!s.put("", "x"));        // 空 key 拒绝
        s.dump_index(std::cout);       // 打印索引
    }

    {
        std::ifstream f(PATH_PUT,std::ios::binary | std::ios::ate);
        CHECK(f.tellg() == 53);
    }

    std::cout << "[8] get" << std::endl;
    const char *PATH_GET = "/tmp/kv_test_get.log";
    std::remove(PATH_GET);
    {
        kv::Storage s(PATH_GET);
        CHECK(s.good());

        //8.1 命中
        CHECK(s.put("k1", "v1"));
        std::string out = "sentinel";   //哨兵：验证未命中/失败时不许改 value
        CHECK(s.get("k1", out));
        CHECK(out == "v1");

        //8.2 未命中：返回 false 且不污染 value
        out = "sentinel";
        CHECK(!s.get("nope", out));
        CHECK(out == "sentinel");

        //8.3 覆盖后再取：拿到最新值
        CHECK(s.put("k1", "v2"));
        CHECK(s.get("k1", out));
        CHECK(out == "v2");

        //8.4 空 value（边界）
        CHECK(s.put("empty", ""));
        out = "sentinel";
        CHECK(s.get("empty", out));
        CHECK(out.empty());

        //8.5 大 value（1MB）：长度与内容都要一致
        std::string big(1024 * 1024, 'x');
        big[0] = 'A';
        big[big.size() - 1] = 'Z';
        CHECK(s.put("big", big));
        out.clear();
        CHECK(s.get("big", out));
        CHECK(out.size() == big.size());
        CHECK(out == big);

        //8.6 多 key 互不干扰
        CHECK(s.put("k2", "v2b"));
        CHECK(s.put("k3", "v3b"));
        CHECK(s.key_count() == 5);   //k1 / empty / big / k2 / k3
        CHECK(s.get("k2", out));
        CHECK(out == "v2b");
        CHECK(s.get("k3", out));
        CHECK(out == "v3b");

        //8.7 CRC 被篡改：绕过 Storage 直接把文件最后一个字节翻转
        //     （最后一条记录就是 k3；索引仍在内存里指着它，所以 get 会去读这条坏记录）
        //     索引是私有的 只能直接改文件；这里复用同一个 Storage 对象 不能重开
        //     （重开会让 index_ 变空，get 会因为"查不到"而返回 false —— 那就测不到 CRC 这条路径了）
        {
            std::fstream f(PATH_GET,std::ios::binary | std::ios::in | std::ios::out);
            f.seekg(0,std::ios::end);
            std::streamoff size = f.tellg();
            CHECK(size > 0);

            f.seekg(size - 1);
            char last = 0;
            f.read(&last,1);
            char flipped = static_cast<char>(last ^ 0xFF);
            f.seekp(size - 1);
            f.write(&flipped,1);
        }

        out = "sentinel";
        CHECK(!s.get("k3", out));      //坏记录必须被拒绝
        CHECK(out == "sentinel");      //且不污染 value

        //没被破坏的记录必须照旧可读（证明坏记录没有连带影响）
        CHECK(s.get("k2", out));
        CHECK(out == "v2b");
    }





    //汇总
    if (g_failed == 0) 
    {
        std::cout << "\nall tests passed" << std::endl;
        return 0;
    }
    std::cout << "\n" << g_failed << " check(s) FAILED" << std::endl;
    return 1;

}