#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <chrono>
#include <sys/stat.h>
#include <sys/types.h>
#include <vector>
#include <filesystem>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <thread>
#include <mutex>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "coordinator.grpc.pb.h"

namespace fs = std::filesystem;

using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::ServerList;
using csce438::Confirmation;
using csce438::ID;
//using csce438::ServerList;
//using csce438::SynchService;

struct zNode{
    int serverID;
    std::string hostname;
    std::string port;
    std::string type;
    std::time_t last_heartbeat;
    bool missed_heartbeat;
    bool isActive();
    bool is_master;

};

struct syncNode{
  int serverID;
  std::string hostname;
  std::string port;
  
};


std::vector<syncNode*> syncServers;

//potentially thread safe
std::mutex v_mutex;
std::vector<zNode*> cluster1;
std::vector<zNode*> cluster2;
std::vector<zNode*> cluster3;
std::map<int,std::vector<zNode*>> clusters;


//func declarations
int findServer(std::vector<zNode*> v, int id);
int findSyncServer(std::vector<syncNode*> v, int id);
std::time_t getTimeNow();
void checkHeartbeat();


bool zNode::isActive(){
    bool status = false;
    if(!missed_heartbeat){
        status = true;
    }else if(difftime(getTimeNow(),last_heartbeat) < 10){
        status = true;
    }
    return status;
}

int findServer(std::vector<zNode*> v, int id){
  int index = 0 ;
  for(zNode* node : v){
    if(node->serverID == id){
      return index;

    }
    index++;
  }
   
  return -1;
}

int findMaster(std::vector<zNode*> v){
  int index = 0 ;
  for(zNode* node : v){
    if(node->is_master == true){
      return index;

    }
    index++;
  }
  return -1;
}



int findSyncServer(std::vector<syncNode*> v, int id){
  int index = 0 ;
  for(syncNode* node : v){
    if(node->serverID == id){
      return index;

    }
    index++;
  }
   
  return -1;
}

class CoordServiceImpl final : public CoordService::Service {

  
  Status Heartbeat(ServerContext* context, const ServerInfo* serverinfo, Confirmation* confirmation) override {
    //std::cout<<"Got Heartbeat! "<<serverinfo->type()<<"("<<serverinfo->serverid()<<")"<<std::endl;
    int index =  findServer(clusters[serverinfo->clusterid()],serverinfo->serverid());
    if(index != -1){
      zNode* node = clusters[serverinfo->clusterid()][index];
      node->last_heartbeat = getTimeNow();
      if(node->is_master){
          confirmation->set_status(true);
          confirmation->set_is_master(true);
      }
      else{
          int master_index = findMaster(clusters[serverinfo->clusterid()]);
          if(master_index == -1)
          {
            node->is_master = true;
             confirmation->set_status(true);
             confirmation->set_is_master(true);
          } 
      }
      //std::cout<< "Heartbeat received from "<<serverinfo->serverid()<< " in cluster "<<serverinfo->clusterid()<<std::endl;
    }else{
      std::cout<< "Server not found but received a heartbeat request from unkown server"<<std::endl;
    }
    // Your code here
    
    return Status::OK;
  }
  
  //function returns the server information for requested client id
  //this function assumes there are always 3 clusters and has math
  //hardcoded to represent this.
  Status GetServer(ServerContext* context, const ID* id, ServerInfo* serverinfo) override {
    std::cout<<"Got GetServer for clientID: "<<id->id()<<std::endl;
    int clusterId = ((id->id()-1)%3)+1;
    if(clusters.find(clusterId) != clusters.end()){
        

        int master_index = findMaster(clusters[clusterId]);
      if(master_index != -1){  
        zNode* node = clusters[clusterId][master_index];
       if(node->isActive()){ 
        serverinfo->set_clusterid(clusterId);
        serverinfo->set_serverid(node->serverID);
        serverinfo->set_hostname(node->hostname);
        serverinfo->set_port(node->port);
        serverinfo->set_type(node->type);
         } 
       }
       else{
        std::cout<<"server in the cluster is not active"<<std::endl;
       }
    } else{
      std::cout<<"cluster not found for the client"<< std::endl;
    }
    // Your code here
    // If server is active, return serverinfo
     
    return Status::OK;
  }

  Status CreatePath(ServerContext* context, const ServerInfo* request, Confirmation* confirmation) override {
      std::unique_lock<std::mutex> lock(v_mutex);
      zNode* node= new zNode;
      node->serverID = request->serverid();
      node->hostname = request->hostname();
      node->port = request->port();
      node->type = request->type();
      node->last_heartbeat = getTimeNow();
      node->is_master = false;
      auto it = clusters.find(request->clusterid());
      if(it != clusters.end()){
        int index =  findServer(clusters[request->clusterid()],request->serverid());
        if(index == -1){
          int master_index = findMaster(it->second);
          if(master_index == -1){
            node->is_master = true;
            confirmation->set_is_master(true);
          }
          it->second.push_back(node);
          
        }
        else{
          zNode* curr_node = clusters[request->clusterid()][index];
          curr_node->serverID = request->serverid();
          curr_node->hostname = request->hostname();
          curr_node->port = request->port();
          curr_node->type = request->type();
          curr_node->last_heartbeat = getTimeNow();
          curr_node->is_master = false;
          int master_index = findMaster(it->second);
          if(master_index == -1){
            node->is_master = true;
            confirmation->set_is_master(true);
          }
          std::cout<<"node has been updated"<<std::endl;
        }
      } else{
         std::vector<zNode*> cluster;
         node->is_master = true;
         confirmation->set_is_master(true);
         cluster.push_back(node);
         clusters[request->clusterid()] = cluster;
      }
      confirmation->set_status(true);
      
      //master_iterator = std::find(clusters[request.clusterid()].)
      std::cout<<"path created for cluster id "<<request->clusterid() << " , server id "<< request->serverid()<<std::endl;
      lock.unlock();
      return Status::OK;
  }

  Status RegisterSyncServer(ServerContext* context, const ServerInfo* request, Confirmation* confirmation) override {
     syncNode* node = new syncNode;
     node->serverID = request->serverid();
     node->hostname = request->hostname();
     node->port = request->port();
     int index = findSyncServer(syncServers,request->serverid());
     if(index == -1){
        syncServers.push_back(node);
        std::cout<< "Sync service : "<< request->serverid() << " is registered!! "<<std::endl;
     }else{
         std::cout<< "Sync service : "<< request->serverid() << " is already available!! "<<std::endl;
     }
     confirmation->set_status(true);

    return Status::OK;
  }

  Status GetFollowerServer(ServerContext* context, const ID* id, ServerInfo* serverInfo) override{
       
      std::cout<<"Got Sync server for clientID: "<<id->id()<<std::endl;
      int syncId = ((id->id()-1)%3)+1;
      int index = findSyncServer(syncServers, syncId);
      if(index != -1){
          syncNode* syncNode = syncServers[index]; 
          serverInfo->set_serverid(syncNode->serverID);
          serverInfo->set_hostname(syncNode->hostname);
          serverInfo->set_port(syncNode->port);
      }else{
        std::cout<<" Sync server Not available for clientID: "<<id->id()<<std::endl;
      }

      return Status::OK;
  }
  

  Status GetAllFollowerServers(ServerContext* context, const ID* id, ServerList* serverList) override{
       
      std::cout<<"Got Sync server for clientID: "<<id->id()<<std::endl;
      int syncId = ((id->id()-1)%3)+1;
  
      for(syncNode* node : syncServers){
        if(node->serverID != syncId){
          serverList->add_serverid(node->serverID);
          serverList->add_hostname(node->hostname);
          serverList->add_port(node->port);
        }
        
      }
      

      return Status::OK;
  }

  Status GetCounterpart(ServerContext* context, const ServerInfo* request, ServerInfo* serverInfo) override{
      auto it = clusters.find(request->clusterid());
      if(it != clusters.end()){
        for( zNode* node : clusters[request->clusterid()]){
          if(node->serverID != request->serverid() && !node->is_master){
            serverInfo->set_serverid(node->serverID);
            serverInfo->set_hostname(node->hostname);
            serverInfo->set_port(node->port);
            serverInfo->set_clusterid(request->clusterid());
            
          }else{
            serverInfo->set_hostname("NA");
          }
        }
      }else{
          std::cout<<" cluster not found for id "<<request->clusterid() << " , server id "<< request->serverid()<<std::endl;
      }
        
    //  confirmation->set_status(true);
      //master_iterator = std::find(clusters[request.clusterid()].)
      
     // lock.unlock();
      return Status::OK;
  }



};

void ClearDirectory(){
  std::vector<std::string> directories;
  directories.push_back("./master1");
  directories.push_back("./master2");
  directories.push_back("./master3");
  directories.push_back("./slave1");
  directories.push_back("./slave2");
  directories.push_back("./slave3");
    try {
      for(std::string directoryPath: directories){
        for (const auto& entry : fs::directory_iterator(directoryPath)) {
            if (fs::is_regular_file(entry)) {
                fs::remove(entry.path());
               // std::cout << "Removed file: " << entry.path() << std::endl;
            }
        }
      //  std::cout << "All files removed from directory: " << directoryPath << std::endl;
      }
    } catch (const fs::filesystem_error& e) {
        std::cerr << "Filesystem error: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "An error occurred while removing files." << std::endl;
    }
}

void RunServer(std::string port_no){
  //start thread to check heartbeats
  std::thread hb(checkHeartbeat);
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  CoordServiceImpl service;
  //grpc::EnableDefaultHealthCheckService(true);
  //grpc::reflection::InitProtoReflectionServerBuilderPlugin();
  ServerBuilder builder;
  // Listen on the given address without any authentication mechanism.
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  // Register "service" as the instance through which we'll communicate with
  // clients. In this case it corresponds to an *synchronous* service.
  builder.RegisterService(&service);
  // Finally assemble the server.
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  ClearDirectory();
  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}

int main(int argc, char** argv) {
  
  std::string port = "3010";
  int opt = 0;
  while ((opt = getopt(argc, argv, "p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;
          break;
      default:
	std::cerr << "Invalid Command Line Argument\n";
    }
  }
  RunServer(port);
  return 0;
}



void checkHeartbeat(){
    while(true){
      //check servers for heartbeat > 10
      //if true turn missed heartbeat = true
      // Your code below
      for(const auto& cluster : clusters){
        bool master_down = false;
        for(zNode* node : cluster.second){
          if(difftime(getTimeNow(),node->last_heartbeat)>10){
	            if(!node->missed_heartbeat){
	                node->missed_heartbeat = true;
                  if(node->is_master == true){
                      node->is_master = false;
                      master_down = true;
                  }
	               // s->last_heartbeat = getTimeNow();
	            }
        	}
        }

        // now replacing in a master
        for(zNode* node : cluster.second){
          if(node->isActive() && master_down){
	            node->is_master = true;
              master_down = false;
        	}
        }
      }
      
      sleep(3);    
        
    }
	
}


std::time_t getTimeNow(){
    return std::chrono::system_clock::to_time_t(std::chrono::system_clock::now());
}

