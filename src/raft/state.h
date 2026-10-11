#ifndef RAFT_STATE_H
#define RAFT_STATE_H
#include <cstdint>
#include <string>

namespace raft
{


    struct HardState
    {
        std::uint64_t current_term = 0; //当前任期
        std::uint64_t voted_for = 0;    //在当前任期将投票给了哪个节点
    };

    class StateStore
    {
    public:
        //保留目录路径
        explicit StateStore(const std::string& directory);

        //加载状态 成功则更新 out
        bool load(HardState& out ,std:: string& error)const;

        
        //保存 term/vote 记录
        bool save(const HardState& state,std::string& error)const;

    private:

        std::string directory_;

    };



};//namespace raft



#endif