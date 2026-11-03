#ifndef RAFT_RAFT_VALIDATE_H
#define RAFT_RAFT_VALIDATE_H

#include "raft/core/core.h"
#include <string>

namespace raft
{

    //检查网络收到的请求
    bool validate_vote_request(const RequestVoteRequest& request,std::string& error);

    bool validate_append_entries(const AppendEntriesRequest& request,std::string& error);

}//namespace raft


#endif