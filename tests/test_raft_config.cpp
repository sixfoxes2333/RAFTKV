// M6 第4步：Raft 配置解析
//
// 纯字符串解析：不读环境变量（除最后一组）、不建 socket、不碰文件系统。
// 规则用 check() 收集失败一口气报出来；只有"能否继续跑下去"的前提才用 CHECK。

#include "raft/transport/config.h"

#include <cstdlib>
#include <iostream>
#include <limits>
#include <string>
#include <vector>

#define CHECK(expr) do { if (!(expr)) { \
    std::cerr << __FILE__ << ':' << __LINE__ << ": FAIL " #expr << '\n'; \
    std::exit(1); \
} } while (false)

namespace {

using raft::PeerAddress;
using raft::RaftConfig;

int failures = 0;

void check(bool ok,const std::string& what)
{
    if(!ok)
    {
        std::cerr << "FAIL: " << what << '\n';
        ++failures;
    }
}

// 哨兵：失败时必须原样保留
RaftConfig sentinel()
{
    RaftConfig config;
    config.self_id = 999;
    config.self.id = 999;
    config.self.host = "keep.me";
    config.self.port = 999;
    config.peers.push_back(PeerAddress{998,"keep.too",998});
    config.data_dir = "keep-dir";
    return config;
}

bool unchanged(const RaftConfig& config)
{
    return config.self_id == 999 &&
           config.self.id == 999 &&
           config.self.host == "keep.me" &&
           config.self.port == 999 &&
           config.peers.size() == 1 &&
           config.peers[0].id == 998 &&
           config.data_dir == "keep-dir";
}

// 期待失败，并确认 out 没被改动
void expect_refused(const std::string& id,const std::string& peers,const std::string& dir,
                    const std::string& what)
{
    RaftConfig config = sentinel();
    std::string error;

    const bool ok = raft::parse_raft_config(id,peers,dir,config,error);

    check(!ok,what + " must be refused");
    check(!error.empty(),what + " must explain why");
    check(unchanged(config),what + " must leave out untouched");
}

// ---------- 正常 --------------

void test_valid_configs()
{
    std::string error;

    // 三节点：故意乱序写，解析后应按 ID 升序
    {
        RaftConfig config;
        const std::string peers = "3@127.0.0.1:9103,1@127.0.0.1:9101,2@127.0.0.1:9102";

        CHECK(raft::parse_raft_config("2",peers,"/tmp/raft-n2",config,error));
        check(error.empty(),"valid: success clears error");
        check(config.self_id == 2,"valid: self id");
        check(config.self.id == 2,"valid: self entry id");
        check(config.self.host == "127.0.0.1","valid: self host");
        check(config.self.port == 9102,"valid: self port comes from the self entry");
        check(config.data_dir == "/tmp/raft-n2","valid: data dir");

        check(config.peers.size() == 2,"valid: two other peers");
        check(config.peers[0].id == 1 && config.peers[1].id == 3,"valid: peers sorted by id");
        check(config.peers[0].port == 9101 && config.peers[1].port == 9103,"valid: peer ports");

        const auto members = config.members();
        check(members.size() == 3,"valid: members includes self");
        check(members[0] == 1 && members[1] == 2 && members[2] == 3,"valid: members sorted");
    }

    // 单节点：PEERS 里只有自己，也是合法集群
    {
        RaftConfig config;
        CHECK(raft::parse_raft_config("1","1@0.0.0.0:9101","/tmp/raft-n1",config,error));
        check(config.peers.empty(),"single: no other peers");
        const auto members = config.members();
        check(members.size() == 1 && members[0] == 1,"single: members is just self");
    }

    // 大 ID 也能用（成员 ID 不要求连续）
    {
        RaftConfig config;
        CHECK(raft::parse_raft_config("18446744073709551615",
                                      "1@127.0.0.1:1,18446744073709551615@127.0.0.1:2",
                                      "/tmp/raft-big",config,error));
        check(config.self_id == std::numeric_limits<std::uint64_t>::max(),"big: uint64 max id accepted");
    }
}

// ---------- ID 与端口 --------------

void test_bad_id_and_port()
{
    const std::string peers = "1@127.0.0.1:9101,2@127.0.0.1:9102";

    for(const std::string id : {"0","","-1","+1","1a"," 1","18446744073709551616"})
    {
        expect_refused(id,peers,"/tmp/d","id '"+id+"'");
    }

    for(const std::string port : {"0","65536","abc","","9102abc"})
    {
        expect_refused("2","1@127.0.0.1:9101,2@127.0.0.1:"+port,"/tmp/d","port '"+port+"'");
    }
}

// ---------- PEERS 语法 --------------

void test_bad_peer_syntax()
{
    const std::string dir = "/tmp/d";

    expect_refused("2","1@127.0.0.1:9101,2#127.0.0.1:9102",dir,"peer without '@'");
    expect_refused("2","1@127.0.0.1:9101,2@127.0.0.19102",dir,"peer without ':'");
    expect_refused("2","1@127.0.0.1:9101,2:9102@127.0.0.1",dir,"'@' after ':'");
    expect_refused("2","1@127.0.0.1:9101,,2@127.0.0.1:9102",dir,"empty segment in the middle");
    expect_refused("2","1@127.0.0.1:9101,2@127.0.0.1:9102,",dir,"trailing comma");
    expect_refused("2","1@:9101,2@127.0.0.1:9102",dir,"empty host");
    expect_refused("2","1@::1:9101,2@127.0.0.1:9102",dir,"host containing ':' (IPv6 not supported)");
    expect_refused("2","",dir,"empty peers");
    expect_refused("2","@127.0.0.1:9101",dir,"missing peer id");
}

// ---------- 成员关系 --------------

void test_member_relations()
{
    const std::string dir = "/tmp/d";

    expect_refused("2","1@127.0.0.1:9101,1@127.0.0.1:9102,2@127.0.0.1:9102",dir,"duplicate peer id");
    expect_refused("2","1@127.0.0.1:9101,2@127.0.0.1:9102,2@127.0.0.1:9103",dir,"self listed twice");
    expect_refused("9","1@127.0.0.1:9101,2@127.0.0.1:9102",dir,"self missing from peers");
    expect_refused("2","1@127.0.0.1:9101,3@127.0.0.1:9101,2@127.0.0.1:9102",dir,"duplicate peer address");
    expect_refused("2","1@127.0.0.1:9102,2@127.0.0.1:9102",dir,"peer address collides with self");
    expect_refused("2","1@127.0.0.1:9101,2@127.0.0.1:9102","","empty data dir");
}

// ---------- 环境变量 --------------

void test_env_loading()
{
    // 先存好原来的值，测完还原，免得影响同一进程里的其它代码
    const char* names[3] = {"KV_RAFT_ID","KV_RAFT_PEERS","KV_RAFT_DATA"};
    std::string saved[3];
    bool had[3] = {false,false,false};

    for(int i = 0 ; i < 3 ; i++)
    {
        if(const char* raw = std::getenv(names[i]))
        {
            had[i] = true;
            saved[i] = raw;
        }
    }

    const auto restore = [&]()
    {
        for(int i = 0 ; i < 3 ; i++)
        {
            if(had[i]) ::setenv(names[i],saved[i].c_str(),1);
            else ::unsetenv(names[i]);
        }
    };

    std::string error;

    // 正常读取
    ::setenv("KV_RAFT_ID","3",1);
    ::setenv("KV_RAFT_PEERS","1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103",1);
    ::setenv("KV_RAFT_DATA","/tmp/raft-n3",1);
    {
        RaftConfig config;
        CHECK(raft::load_raft_config_from_env(config,error));
        check(config.self_id == 3,"env: self id from environment");
        check(config.self.port == 9103,"env: own port from peers entry");
        check(config.members().size() == 3,"env: three members");
    }

    // 缺哪个就报哪个名字
    for(const char* name : names)
    {
        ::unsetenv(name);
        RaftConfig config;
        config.self_id = 4242;
        error.clear();
        check(!raft::load_raft_config_from_env(config,error),"env: missing "+std::string(name)+" must fail");
        check(error.find(name) != std::string::npos,
              "env: error must name " + std::string(name));
        check(config.self_id == 4242,"env: failure must leave out untouched");
        ::setenv("KV_RAFT_ID","3",1);
        ::setenv("KV_RAFT_PEERS","1@127.0.0.1:9101,2@127.0.0.1:9102,3@127.0.0.1:9103",1);
        ::setenv("KV_RAFT_DATA","/tmp/raft-n3",1);
    }

    // 值非法时报对应项，而不是报缺失
    ::setenv("KV_RAFT_ID","0",1);
    {
        RaftConfig config;
        error.clear();
        check(!raft::load_raft_config_from_env(config,error),"env: id 0 must fail");
        check(error.find("KV_RAFT_ID") != std::string::npos,"env: id error must name KV_RAFT_ID");
    }

    restore();
}

} // namespace

int main()
{
    test_valid_configs();
    test_bad_id_and_port();
    test_bad_peer_syntax();
    test_member_relations();
    test_env_loading();

    if(failures != 0)
    {
        std::cerr << failures << " failures\n";
        return 1;
    }

    std::cout << "raft config PASS (pure parsing; no socket, no filesystem)\n";
    return 0;
}
