#include "raft/config.h"
#include <algorithm>
#include <cstdlib>
#include <limits>
#include <utility>

namespace raft
{
    namespace
    {
        bool parse_u64(const std::string& text,std::uint64_t& out)
        {
            if(text.empty())
            {
                return false;
            }

            std::uint64_t value = 0;

            for(const char c : text)
            {
                if(c < '0' || c > '9')
                {
                    return false;
                }

                const auto digit = static_cast<std::uint64_t>(c-'0');

                if(value > (std::numeric_limits<std::uint64_t>::max()-digit)/10)
                {
                    return false;
                }

                value = value * 10 + digit;
            }

            out = value;
            return true;
        }

        bool parse_port(const std::string& text,std::uint16_t& out)
        {
            std::uint64_t value = 0;

            if(!parse_u64(text,value))
            {
                return false;
            }

            if(value == 0 || value > 65535)
            {
                return false;
            }

            out = static_cast<std::uint16_t>(value);
            return true;
        }

        bool parse_peer(const std::string& text,PeerAddress& out,std::string& error)
        {
            const auto at = text.find('@');

            if(at == std::string::npos)
            {
                error = "raft config : peer has no '@' : " + text;
                return false;
            }

            const auto colon = text.rfind(':');

            if(colon == std::string::npos || colon < at)
            {
                error = "raft config : peer has no ':' after '@' :" + text;
                return false;
            }

            PeerAddress peer;

            if(!parse_u64(text.substr(0,at),peer.id) || peer.id == 0)
            {
                error = "raft config : bad peer id : " + text;
                return false;
            }

            peer.host = text.substr(at + 1,colon-at-1);

            if(peer.host.empty() || peer.host.find(':') != std::string::npos)
            {
                error = "raft config : bad peer host : " + text;
                return false;
            }

            if(!parse_port(text.substr(colon+1),peer.port))
            {
                error = "raft config : bad peer port : " + text;
                return false;
            }

            out = peer;
            return true;

        }

    }

    std::vector<std::uint64_t>RaftConfig::members()const
    {
        std::vector<std::uint64_t>ids;

        ids.reserve(peers.size()+1);

        for(const auto& peer:peers)
        {
            ids.push_back(peer.id);
        }

        ids.push_back(self_id);
        std::sort(ids.begin(),ids.end());
        return ids;
    }

    bool parse_raft_config(const std::string& id_text,const std::string& peers_text,const std::string& data_dir,RaftConfig& out,std::string& error)
    {
        error.clear();

        RaftConfig parsed;

        if(!parse_u64(id_text,parsed.self_id) || parsed.self_id == 0)
        {
            error = "raft config : bad KV_RAFT_ID : " + id_text;
            return false;
        }

        if(data_dir.empty())
        {
            error = "raft config : empty KV_RAFT_DATA";
            return false;
        }

        if(peers_text.empty())
        {
            error = "raft config : empty KV_RAFT_PEERS";
            return false;
        }

        bool self_seen = false;

        std::size_t begin = 0;

        while(begin <= peers_text.size())
        {
            const auto comma = peers_text.find(',',begin);
            const auto end = (comma == std::string::npos) ? peers_text.size() : comma;
            const auto token = peers_text.substr(begin,end-begin);

            //连续逗号，结尾逗号都会产生空段
            if(token.empty())
            {
                error = "raft config : empty peer entry";
                return false;
            }

            PeerAddress peer;

            if(!parse_peer(token,peer,error))
            {
                return false;
            }

            if(peer.id == parsed.self_id)
            {
                if(self_seen)
                {
                    error = "raft config : self id appears twice";
                    return false;
                }

                self_seen = true;
                parsed.self = peer;
            }
            else
            {
                for(const auto& other : parsed.peers)
                {
                    if(other.id == peer.id)
                    {
                        error = "raft config : duplicate peer id : " + std::to_string(peer.id);
                        return false;
                    }
                }

                //两个成员不可共用一个监听地址
                for(const auto& other : parsed.peers)
                {
                    if(other.host == peer.host && other.port == peer.port)
                    {
                        error = "raft config : duplicare peer address";
                        return false;
                    }
                }

                parsed.peers.push_back(peer);
            }

            if(comma == std::string::npos)
            {
                break;
            }

            begin = comma + 1;
        }

        if(!self_seen)
        {
            error = "raft config : self id " + std::to_string(parsed.self_id) + "is not in KV_RAFT_PEERS";
            return false;
        }

        //自己和别人的监听地址不可相撞
        for(const auto& peer:parsed.peers)
        {
            if(peer.host == parsed.self.host && peer.port == parsed.self.port)
            {
                error = "raft config : peer address collides with self";
                return false;
            }
        }

        std::sort(parsed.peers.begin(),parsed.peers.end(),[](const PeerAddress& a,const PeerAddress& b){return a.id < b.id;});

        parsed.data_dir = data_dir;
        out = std::move(parsed);
        return true;

    }

    bool load_raft_config_from_env(RaftConfig& out,std::string& error)
    {
        error.clear();

        const auto get = [](const char* name,std::string& value,std::string& error)->bool
        {
            const char* raw = std::getenv(name);

            if(raw == nullptr || *raw == '\0')
            {
                error = std::string("raft config : missing ") + name;
                return false;
            }

            value = raw;
            return true;
        };

        std::string id_text,peers_text,data_dir;

        if(!get("KV_RAFT_ID",id_text,error))
        {
            return false;
        }

        if(!get("KV_RAFT_PEERS",peers_text,error))
        {
            return false;
        }

        if(!get("KV_RAFT_DATA",data_dir,error))
        {
            return false;
        }

        return parse_raft_config(id_text,peers_text,data_dir,out,error);
    }

}//namespace raft