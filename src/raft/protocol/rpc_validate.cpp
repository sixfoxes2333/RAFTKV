#include "raft/protocol/rpc_validate.h"
#include <limits>

namespace raft
{

bool validate_vote_request(const RequestVoteRequest& request,std::string& error)
{
    error.clear();

    if(request.term == 0)
    {
        error = "rpc validate : zero term";
        return false;
    }

    if(request.candidate_id == 0)
    {
        error = "rpc validate : zero candidate id";
        return false;
    }

    //空日志 : index 和 term 必须大于 0
    //非空日志 index > 0 term >= 1
    if(request.last_log_index == 0)
    {
        if(request.last_log_term != 0)
        {
            error = "rpc validate : empty log with non-zero term";
            return false;
        }
    }
    else if(request.last_log_term == 0)
    {
        error = "rpc validate : non-empty log with zero term";
        return false;
    }

    return true;

}

bool validate_append_entries(const AppendEntriesRequest& request,std::string& error)
{
    error.clear();

    if(request.term == 0)
    {
        error = "rpc validate : zero term";
        return false;
    }

    if(request.leader_id == 0)
    {
        error = "rpc validate : zero leader id";
        return false;
    }

    if(request.prev_log_index == 0)
    {
        if(request.prev_log_term != 0)
        {
            error = "rpc validate : index 0 with non-zero prev term";
            return false;
        }
    }
    else if(request.prev_log_term == 0)
    {
        error = "rpc validate : non-zero prev index with zero term";
        return false;
    }

    if(request.prev_log_index > std::numeric_limits<std::uint64_t>::max() - request.entries.size())
    {
        error = "rpc validate : log position overflow";
        return false;
    }

    std::uint64_t previous_term = request.prev_log_term;

    for(const auto& entry : request.entries)
    {
        if(entry.term == 0 || entry.term > request.term)
        {
            error = "rpc validate : entry term out of range";
            return false;
        }

        if(entry.term < previous_term)
        {
            error = "rpc validate : entry term decreases";
            return false;
        }

        previous_term = entry.term;
    }

    return true;

}

}//namespace raft