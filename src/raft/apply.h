#ifndef RAFT_APPLY_H
#define RAFT_APPLY_H

#include "raft/core.h"
#include "raft/logstore.h"
#include "raft/state.h"
#include <string>

namespace raft
{

//执行 handle_append_entries
bool apply_append_entries(RaftCore& core,StateStore& state,LogStore& store,const AppendEntriesOut& out,AppendEntriesResponse& response,std::string& error);

}//namespace raft

#endif