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

    if(std::find(members_.begin(),members_.end(),from) == members_.end())
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

    if(from == self_id_ || std::find(members_.begin(),members_.end(),from) == members_.end())
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


bool RaftCore::on_log_persisted(std::string& error)
{
    error.clear();

    if(!good())
    {
        error = "sync log tail: core not ready";
        return false;
    }

    if(log_ == nullptr)
    {
        error = "sync log tail: no log view";
        return false;
    }

    const std::uint64_t last = log_->last_index();

    std::uint64_t term = 0;

    if(last != 0 && !log_->term_at(last,term))
    {
        error = "sync log tail: cannot read last term";
        return false;
    }

    if(last < commit_index_)
    {
        error = "sync log tail: commit entry missing";
        return false;
    }

    if(role_ == Role::Leader)
    {
        if(last < last_log_index_)
        {
            error = "sync log tail: leader log shortened";
            return false;
        }

        if(last == last_log_index_ && term != last_log_term_)
        {
            error = "sync log tail: leader log replaced";
            return false;
        }
    }

    last_log_index_ = last;
    last_log_term_ = term;

    if(role_ == Role::Leader)
    {
        for(std::size_t i = 0 ; i < members_.size() ; i++)
        {
            if(members_[i] == self_id_)
            {
                match_index_[i] = last;
            }
        }

    }

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

bool RaftCore::build_proposal(const std::string& command,LogEntry& out,std::string& error)const
{
    error.clear();

    if(!good() || role_ != Role::Leader)
    {
        error = "build proposal : core is not a leader";
        return false;
    }

    //保存成功后，on_log_persisted 通过本视图同步本机尾部
    if(log_ == nullptr)
    {
        error = "build proposal : no log view";
        return false;
    }

    if(last_log_index_ >= std::numeric_limits<std::uint64_t>::max()-1)
    {
        error = "build proposal : log index exhausted";
        return false;
    }

    if(command.size() > LogStore::MAX_COMMAND_SIZE)
    {
        error = "build proposal : command too large";
        return false;
    }

    try
    {
        LogEntry entry;
        entry.index = last_log_index_ + 1;
        entry.term = hard_state_.current_term;
        entry.command = command;

        out = std::move(entry);
        return true;
    }
    catch(const std::exception &ex)
    {
        error = std::string("build proposal: ") + ex.what();
        return false;
    }
    
}

bool RaftCore::advance_commit(CommitOut& out,std::string& error)
{
    error.clear();

    if(!good() || role_ != Role::Leader)
    {
        error = "advance commit : core is not a leader";
        return false;
    }

    if(log_ == nullptr)
    {
        error = "advance commit : no log view";
        return false;
    }

    CommitOut result;
    //此处 candidate 为暂定日志编号
    std::uint64_t candidate = commit_index_;

    for(const std::uint64_t index : match_index_)
    {
        if(index <= candidate)
        {
            continue;
        }

        std::size_t count = 0;
        for(const std::uint64_t matched : match_index_)
        {
            if(matched >= index)
            {
                count++;
            }
        }

        if(count >= majority_)
        {
            candidate = index;
        }
    }

    if(candidate == commit_index_)
    {
        out = result;
        return true;
    }

    if(candidate > last_log_index_)
    {
        error = "advance commit : confirmed index beyond local log";
        return false;
    }

    std::uint64_t term = 0;
    //查询日志创建时的任期
    if(!log_->term_at(candidate,term))
    {
        error = "advance commit : cannot read candidate term";
        return false;
    }

    if(term != hard_state_.current_term)
    {
        out = result;
        return true;
    }

    result.committed_from = commit_index_ + 1;
    result.committed_to = candidate;
    commit_index_ = candidate;
    out = result;
    return true;

}


bool RaftCore::advance_follower_commit(const AppendEntriesRequest& accepted_request,CommitOut& out,std::string& error)
{
    error.clear();

    if(!good() || role_ != Role::Follower)
    {
        error = "follower commit : core is not a follower";
        return false;
    }

    if(log_ == nullptr)
    {
        error = "follower commit : no log view";
        return false;
    }

    if(accepted_request.term != hard_state_.current_term)
    {
        error = "follower commit : request term mismatch";
        return false;
    }

    const auto max_index = std::numeric_limits<std::uint64_t>::max();

    if(accepted_request.entries.size() > max_index - accepted_request.prev_log_index)
    {
        error = "follower commit : match index overflow";
        return false;
    }

    const std::uint64_t matched_last = accepted_request.prev_log_index + accepted_request.entries.size();

    //Core 记住的已保存尾部还没到此 不算作本次保存完成
    if(matched_last > last_log_index_)
    {
        error = "follower commit : matched index beyond saved log";
        return false;
    }

    CommitOut result;

    const std::uint64_t next_commit = std::min(accepted_request.leader_commit,matched_last);

    if(next_commit <= commit_index_)
    {
        out = result;
        return true;
    }

    result.committed_from = commit_index_ + 1;
    result.committed_to = next_commit;
    commit_index_ = next_commit;
    out = result;
    return true;

}

//给 peer 打包请求，next_index 是下一条准备发给它的位置
bool RaftCore::build_append_entries(std::uint64_t peer,AppendEntriesRequest& out,std::string& error)const
{
    error.clear();

    if(!good() || role_ != Role::Leader)
    {
        error = "build append : core is not a leader";
        return false;
    }

    //找到 peer 在成员表的下标
    std::size_t slot = members_.size();

    for(std::size_t i = 0 ; i < members_.size() ; i++)
    {
        if(members_[i] == peer)
        {
            slot = i;
            break;
        }
    }

    if(slot == members_.size() || peer == self_id_)
    {
        error = "build append : target is not another member";
        return false;
    }

    if(log_ == nullptr)
    {
        error = "build append : no log view";
        return false;
    }

    const std::uint64_t last = log_->last_index();

    //编号到最大
    if(last == std::numeric_limits<std::uint64_t>::max())
    {
        error = "build append : log index exhausted";
        return false;
    }

    if(commit_index_ > last)
    {
        error = "build append : commit index beyond log";
        return false;
    }

    const std::uint64_t next = next_index_[slot];

    //next 最大为 last+1 last+1 表示无新日志
    if(next == 0 || next > last + 1)
    {
        error = "build append : invalid next index";
        return false;
    }

    try
    {
        AppendEntriesRequest request;

        request.term = hard_state_.current_term;
        request.leader_id = self_id_;
        request.leader_commit = commit_index_;

        request.prev_log_index = next-1;

        if(request.prev_log_index != 0)
        {
            if(!log_->term_at(request.prev_log_index,request.prev_log_term))
            {
                error = "build append : cannot read previous term";
                return false;
            }
        }

        //无日志 即心跳
        if(next <= last)
        {
            request.entries.reserve(MAX_APPEND_ENTRIES);

            std::size_t bytes = 0;

            for(std::uint64_t index = next ; index <= last ; index++)
            {
                LogEntry entry;

                if(!log_->entry_at(index,entry))
                {
                    error = "build append : cannot read entry";
                    return false;
                }

                const std::size_t size = entry.command.size();

                //超过 1 MiB 就给下一批
                if(!request.entries.empty() && size > MAX_APPEND_COMMAND_BYTES - bytes)
                {
                    break;
                }

                request.entries.push_back(std::move(entry));
                bytes += size;

                if(request.entries.size() >= MAX_APPEND_ENTRIES || bytes >= MAX_APPEND_COMMAND_BYTES)
                {
                    break;
                }

            }
        }

        out = std::move(request);
        return true;
    }
    catch(const std::exception& ex)
    {
        error = std::string("build append : ") + ex.what();
        return false;
    }

}

//收到 leader 的日志请求，判断是否接 是否删/加 哪些日志
bool RaftCore::handle_append_entries(std::uint64_t from,const AppendEntriesRequest& request,AppendEntriesOut& out,std::string& error)
{
    error.clear();

    if(!good())
    {
        error = "append entris : core not ready";
        return false;
    }

    if(log_ == nullptr)
    {
        error = "append entris : no log view";
        return false;
    }

    if(from == self_id_ || std::find(members_.begin(),members_.end(),from) == members_.end())
    {
        error = "append entris : sender is not another member";
        return false;
    }

    if(request.leader_id != from)
    {
        error = "append entris : leader id mismatch";
        return false;
    }

    if(request.term == 0)
    {
        error = "append entries : zero term";
        return false;
    }

    //同任期还有另一个leader 说明集群状态不对，则不更改任何
    if(request.term == hard_state_.current_term && role_ == Role::Leader)
    {
        error = "append entris : another leader in same term";
        return false;
    }

    AppendEntriesOut result;

    result.term = hard_state_.current_term;

    //发送方任期比自己小 直接拒绝
    if(request.term < hard_state_.current_term)
    {
        out = std::move(result);
        return true;
    }

    //发送方任期更大 退位 清理投票记录
    if(request.term > hard_state_.current_term)
    {
        hard_state_.current_term = request.term;
        hard_state_.voted_for = 0;

        role_ = Role::Follower;
        votes_.clear();

        result.term = hard_state_.current_term;
        result.hard_state = hard_state_;
    }
    else if(role_ == Role::Candidate)
    {
        role_ = Role::Follower;
        votes_.clear();
    }

    result.reset_election_timer = true;

    const std::uint64_t last = log_->last_index();

    if(request.prev_log_index > last)
    {
        out = std::move(result);
        return true;
    }

    //核对前一条的任期 第 0 条按 0 算
    if(request.prev_log_index != 0)
    {
        std::uint64_t local_term = 0;

        if(!log_->term_at(request.prev_log_index,local_term))
        {
            error = "append entries : cannot read previous term";
            return false;
        }

        if(local_term != request.prev_log_term)
        {
            out = std::move(result);
            return true;
        }
    }

    std::uint64_t conflict = 0;

    for(std::size_t i = 0 ; i < request.entries.size() ; i++)
    {
        const std::uint64_t index = request.prev_log_index+1+i;

        if(index > last)
        {
            conflict = index;
            break;
        }

        std::uint64_t local_term = 0;
        if(!log_->term_at(index,local_term))
        {
            error = "append entries : cannot read local term";
            return false;
        }

        if(local_term != request.entries[i].term)
        {
            if(index <= commit_index_)
            {
                error = "append entries : conflict below commit index";
                return false;
            }

            conflict = index;
            break;
        }
    }


    if(conflict != 0)
    {
        result.truncate_from = conflict;

        for(std::size_t i = 0 ; i < request.entries.size() ; i++)
        {
            LogEntry entry = request.entries[i];

            entry.index = request.prev_log_index+1+i;

            if(entry.index >= conflict)
            {
                result.entries.push_back(std::move(entry));
            }
        }
    }

    result.accept = true;
    out = std::move(result);
    return true;

}


bool RaftCore::handle_append_entries_response(std::uint64_t from,const AppendEntriesRequest& sent,const AppendEntriesResponse& response,AppendResponseOut& out,std::string& error)
{
    error.clear();

    if(!good())
    {
        error = "append response : core not ready";
        return false;
    }
    //找到回复节点
    const auto it = std::find(members_.begin(),members_.end(),from);

    if(from == self_id_ || it == members_.end())
    {
        error = "append response : sender is not another member";
        return false;
    }

    //准备本次输出
    AppendResponseOut result;

    //先处理更高任期
    if(response.term > hard_state_.current_term)
    {
        hard_state_ = {response.term,0};

        role_ = Role::Follower;
        votes_.clear();

        result.hard_state = hard_state_;
        out = std::move(result);
        return true;
    }

    if(role_ != Role::Leader || response.term != hard_state_.current_term || sent.term != hard_state_.current_term)
    {
        out = std::move(result);
        return true;
    }

    if(sent.leader_id != self_id_ || sent.prev_log_index > last_log_index_ || sent.entries.size() > last_log_index_ - sent.prev_log_index)
    {
        error = "append response: invalid sent request";
        return false;
    }

    const std::uint64_t sent_last = sent.prev_log_index + sent.entries.size();

    if(sent_last == std::numeric_limits<std::uint64_t>::max())
    {
        error = "append response : next index would overflow";
        return false;
    }

    //换算回复节点的下标
    const std::size_t slot = static_cast<std::size_t>(it-members_.begin());

    if(response.success)
    {
        if(sent_last > match_index_[slot])
        {
            match_index_[slot] = sent_last;
            next_index_[slot] = sent_last + 1;
        }
    }
    else if(sent.prev_log_index + 1 == next_index_[slot] && sent.prev_log_index > match_index_[slot])
    {
        next_index_[slot]--;
        result.retry = true;
    }

    out = std::move(result);
    return true;

}




}//namespace raft