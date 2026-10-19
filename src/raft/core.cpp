#include "raft/core.h"
#include <algorithm>
#include <exception>
#include <limits>
#include <utility>

namespace raft
{

bool RaftCore::init(std::uint64_t self_id,const std::vector<std::uint64_t>& members,const HardState& recovered,std::uint64_t last_log_index,std::uint64_t last_log_term,std::string& error)
{
    error.clear();

    //避免重复初始化 或 停止后初始化
    if(initalized_ || stopped_)
    {
        error = "core init :already initialized or stopped";
        return false;
    }

    if(self_id == 0 || members.empty())
    {
        error = "core init: nonzero self id and nonempty members required";
        return false;
    }

    //检查 ID 非零且不重复
    for(std::size_t i = 0 ; i < members.size() ; i++)
    {
        if(members[i] == 0)
        {
            error = "core init: member id must be nonzero";
            return false;
        }

        for(std::size_t j = 0 ; j < i ; j++)
        {
            if(members[i] == members[j])
            {
                error = "core init: duplicate member id";
                return false;
            }
        }

    }

    //本机节点也要属于配置中的集群
    if(std::find(members.begin(),members.end(),self_id) == members.end())
    {
        error = "core init: self id not in members";
        return false;
    }

    if((recovered.current_term == 0 && recovered.voted_for != 0) || (recovered.voted_for != 0 && std::find(members.begin(),members.end(),recovered.voted_for) == members.end()))
    {
        error = "core init: invalid recovered vote";
        return false;
    }

    if((last_log_index == 0) != (last_log_term == 0) || last_log_term > recovered.current_term)
    {
        error = "core init: inconsistent recovered log position";
        return false;
    }

    try
    {
        std::vector<std::uint64_t> pending_members(members);
        std::vector<std::uint64_t> pending_votes;

        pending_votes.reserve(members.size());

        std::vector<std::uint64_t>pending_next(members.size(),0);
        std::vector<std::uint64_t>pending_match(members.size(),0);
        
        members_.swap(pending_members);
        votes_.swap(pending_votes);

        next_index_.swap(pending_next);
        match_index_.swap(pending_match);

    }
    catch(const std::exception& ex)
    {
        error = std::string("core init: allocation") + ex.what();
        return false;
    }

    self_id_ = self_id;

    majority_ = members_.size() / 2 +1;

    hard_state_ = recovered;

    role_ = Role::Follower;

    last_log_index_ = last_log_index;
    last_log_term_ = last_log_term;

    initalized_ = true;
    return true;

}

//判断此实例能否继续处理协议事件
bool RaftCore::good()const
{
    return initalized_ && !stopped_;
}

//查询当前协议角色
Role RaftCore::role()const
{
    return role_;
}

//返回当前内存中的 term/vote 的只读引用
const HardState& RaftCore::hard_state()const
{
    return hard_state_;
}

//返回门槛
std::size_t RaftCore::majority()const
{
    return majority_;
}

//停止当前实例
void RaftCore::stop()
{
    stopped_ = true;
}

//开始拉票 任期+1 投自己一票 进入 candidate
bool RaftCore::start_election(ElectionOutPut& out,std::string& error)
{
    error.clear();

    if(!good())
    {
        error = "start election: core not ready";
        return false;
    }

    if(role_ == Role::Leader)
    {
        error = "start election: already leader";
        return false;
    }

    if(hard_state_.current_term == std::numeric_limits<std::uint64_t>::max())
    {
        error = "start election: term overflow";
        return false;
    }

    ElectionOutPut next;

    const std::uint64_t term = hard_state_.current_term + 1;
    next.hard_state = {term,self_id_};
    next.request = {term,self_id_,last_log_index_,last_log_term_};

    //收其他成员的 ID
    try
    {
        next.targets.reserve(members_.size()-1);

        for(const auto id : members_)
        {
            if(id != self_id_)
            {
                next.targets.push_back(id);
            }
        }

        votes_.reserve(members_.size());
    }
    catch(const std::exception& ex)
    {
        error = std::string("start election: allocation: ") + ex.what();
        return false;
    }

    votes_.clear();
    votes_.push_back(self_id_);
    hard_state_ = next.hard_state;
    
    if(majority_ == 1)
    {
        become_leader();
    }
    else
    {
        role_ = Role::Candidate;
    }

    out.hard_state = next.hard_state;
    out.request = next.request;
    out.targets.swap(next.targets);
    return true;

}

bool RaftCore::handle_request_vote(std::uint64_t from,const RequestVoteRequest& request,VoteOutPut& out,std::string& error)
{
    error.clear();

    if(!good())
    {
        error = "request vote: core not ready";
        return false;
    }

    if(from == 0 || std::find(members_.begin(),members_.end(),from) == members_.end())
    {
        error = "request vote: sender is not a member";
        return false;
    }

    if(request.candidate_id != from)
    {
        error = "request vote: candidate id differs from sender";
        return false;
    }

    if(request.term == 0)
    {
        error = "request vote: term must be nonzero";
        return false;
    }

    VoteOutPut result;

    //请求任期小于当前任期
    if(request.term < hard_state_.current_term)
    {
        result.response = {hard_state_.current_term,false};
        out = result;
        return true;
    }

    //请求任期更大
    if(request.term > hard_state_.current_term)
    {
        hard_state_ = {request.term,0};
        role_ = Role::Follower;
        votes_.clear();
        result.hard_state = hard_state_;
    }

    //如果任期相同
    
    bool grant = false;

    if(hard_state_.voted_for == 0 || hard_state_.voted_for == request.candidate_id) //voteed_for == 0 还没投 
    {

        //日志最新的人才能当 leader
        //   voted_for == request.candidate_id 本任期已投给本人
        const bool log_up_to_data = request.last_log_term > last_log_term_ || (request.last_log_term == last_log_term_ && request.last_log_index >= last_log_index_);
        grant = log_up_to_data;
    }

    if(grant)
    {
        if(hard_state_.voted_for != request.candidate_id)
        {
            hard_state_.voted_for = request.candidate_id;
            result.hard_state = hard_state_;
        }

        result.reset_election_timer = true;
    }

    result.response = {hard_state_.current_term,grant};
    out = std::move(result);
    return true;

}


bool RaftCore::handle_vote_response(std::uint64_t from,const RequestVoteResponse& response,VoteCountOutPut& out,std::string& error)
{
    error.clear();

    if(!good())
    {
        error = "vote response: core not ready";
        return false;
    }

    if(from == 0 || from == self_id_ || std::find(members_.begin(),members_.end(),from) == members_.end())
    {
        error = "vote response: sender is not another member";
        return false;
    }

    VoteCountOutPut result;


    if(response.term < hard_state_.current_term)
    {
        out = result;
        return true;
    }

    //如果回复任期更大，本节点退位并作废本轮计票
    if(response.term > hard_state_.current_term)
    {
        hard_state_ = {response.term,0};
        role_ = Role::Follower;
        votes_.clear();
        result.hard_state = hard_state_;
        out = std::move(result);
        return true;
    }

    //不属于 candidate，计票无意义
    if(role_ != Role::Candidate)
    {
        out = result;
        return true;
    }

    //拒票
    if(!response.vote_granted)
    {
        out = result;
        return true;
    }

    //重复回复只计一票
    if(std::find(votes_.begin(),votes_.end(),from) != votes_.end())
    {
        out = result;
        return true;
    }

    votes_.push_back(from);

    if(votes_.size() >= majority_)
    {
        become_leader();
        result.become_leader = true;
    }

    out = std::move(result);
    return true;
    
}


bool RaftCore::attach_log(const LogView* log,std::string& error)
{
    error.clear();

    if(initalized_ || stopped_)
    {
        error = "attach log: core already initialized or stopped";
        return false;
    }

    if(log == nullptr)
    {
        error = "attach log: null view";
        return false;
    }

    log_ = log;
    return true;

}


void RaftCore::become_leader()
{
    role_ = Role::Leader;

    //刚刚当选时假设 follower 都与本机相当
    const std::uint64_t next = last_log_index_ + 1;

    for(std::size_t i = 0 ; i < members_.size() ; i++)
    {
        next_index_[i] = next;

        //自己的日志算已经复制
        match_index_[i] = (members_[i] == self_id_) ? last_log_index_ : 0;
    }
}

std::uint64_t RaftCore::next_index(std::uint64_t peer)const
{
    for(std::size_t i = 0 ; i < members_.size() ; i++)
    {
        if(members_[i] == peer)
        {
            return next_index_[i];
        }
    }

    return 0;
}

std::uint64_t RaftCore::match_index(std::uint64_t peer)const
{
    for(std::size_t i = 0 ; i < members_.size() ; i++)
    {
        if(members_[i] == peer)
        {
            return match_index_[i];
        }
    }

    return 0;
}




}//namespace raft