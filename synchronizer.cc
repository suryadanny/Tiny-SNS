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
#include <algorithm>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>

#include "sns.grpc.pb.h"
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
using grpc::ClientContext;
using csce438::CoordService;
using csce438::ServerInfo;
using csce438::Confirmation;
using csce438::ID;
using csce438::ServerList;
using csce438::SynchService;
using csce438::AllUsers;
using csce438::TLFL;


std::unique_ptr<CoordService::Stub> coord_stub_;
int synchID = 1;
std::vector<std::string> get_lines_from_file(std::string,bool);
void run_synchronizer(std::string,std::string,std::string,int);
void write_user_to_file(std::vector<std::string>,int);
std::vector<std::string> get_all_users_func(int);
std::vector<std::string> get_tl_or_fl(int, std::string, bool);

int findUser(std::vector<std::string> users , std::string username){
   int index  = 0 ;
   for(std::string c : users){
    if(c == username)
      return index;
    index++;
  }
  return -1;
}


void updateFollowers(std::vector<std::string> followers,std::string clientId){
    std::string master_file_path = "./master"+std::to_string(synchID)+"/"+clientId+"followers.txt";
    std::string slave_file_path = "./slave"+std::to_string(synchID)+"/"+clientId+"followers.txt";


    std::ifstream fileCheck(master_file_path);
    if (!fileCheck.is_open()) {
        std::ofstream newFile(master_file_path);
        newFile.close();
    }



    // slave_users_file.open(slave_file_path);
    // master_users_file.open(master_file_path);
    

    std::ofstream master_follower_file(master_file_path, std::ios::app);

      if (master_follower_file.is_open()) {
          // Append the userID to the file
          for(std::string user : followers){
                master_follower_file<< user+"\n";
           }
          
          // Close the file
          master_follower_file.close();
          
      //    std::cout << "UserID appended to the file." << std::endl;
      } else {
        //  std::cerr << "Failed to open the file for appending." << std::endl;
      }

    
    master_follower_file.close();

    std::vector<std::string> master_followers = get_lines_from_file(master_file_path,false);
    

    std::ifstream file1Check(slave_file_path);
    if (!file1Check.is_open()) {
        std::ofstream newFile(slave_file_path);
        newFile.close();
    }

    std::ofstream slave_follower_file(slave_file_path, std::ios::app);

    std::vector<std::string> slave_followers =  get_lines_from_file(slave_file_path,false);
    for(std:: string c: master_followers ){
      if(findUser(slave_followers,c) == -1  ){
          slave_follower_file<<c+"\n";
      }
    }


}

class SynchServiceImpl final : public SynchService::Service {
    Status GetAllUsers(ServerContext* context, const Confirmation* confirmation, AllUsers* allusers) override{
        //std::cout<<"Got GetAllUsers"<<std::endl;
        std::vector<std::string> list = get_all_users_func(synchID);
        //package list
        for(auto s:list){
            allusers->add_users(s);
        }

        //return list
        return Status::OK;
    }

    //Status syncFollower()

    Status GetTLFL(ServerContext* context, const ID* id, TLFL* tlfl) override {
        //std::cout<<"Got GetTLFL"<<std::endl;
        int clientID = id->id();

        std::vector<std::string> tl = get_tl_or_fl(synchID, std::to_string(clientID), true);
        std::vector<std::string> fl = get_tl_or_fl(synchID, std::to_string(clientID), false);

        //now populate TLFL tl and fl for return
        for(auto s:tl){
            tlfl->add_tl(s);
        }
        for(auto s:fl){
            tlfl->add_fl(s);
        }
        tlfl->set_status(true); 

        return Status::OK;
    }

    Status ResynchServer(ServerContext* context, const ServerInfo* serverinfo, Confirmation* c) override {
        std::cout<<serverinfo->type()<<"("<<serverinfo->serverid()<<") just restarted and needs to be resynched with counterpart"<<std::endl;
        std::string backupServerType;

        // YOUR CODE HERE


        return Status::OK;
    }
};

Status RegisterWithCoordinator(ServerInfo& serverDetails, Confirmation& confirmation, std::string coord_login_info){
    //std::string coord_login_info = serverDetails.hostname() + ":" + serverDetails.port();
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(
			       grpc::CreateChannel(
			      coord_login_info, grpc::InsecureChannelCredentials())));
    std::cout<<"registering with "+coord_login_info << std::endl;  
    ClientContext context;
    //std::cout << "server details: "+serverDetails.hostname() + " , " <<serverDetails.port() << " ,"<<serverDetails.clusterid() << " ," << serverDetails.serverid() <<" ,"<<std::endl;
    Status status =  coord_stub_->RegisterSyncServer(&context, serverDetails, &confirmation);
    
    return status;
};

void RunServer(std::string coordIP, std::string coordPort, std::string port_no, int synchID){
  //localhost = 127.0.0.1
  std::string server_address("127.0.0.1:"+port_no);
  SynchServiceImpl service;
  ServerInfo serverinfo;
  serverinfo.set_hostname("0.0.0.0");
  serverinfo.set_port(port_no);
  serverinfo.set_clusterid(synchID);
  serverinfo.set_serverid(synchID);
  serverinfo.set_type("sync");
  std::string coord_login_info = coordIP + ":" + coordPort;
  Confirmation confirmation;
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
 // Status status = RegisterWithCoordinator(serverinfo,confirmation , coord_login_info);

  // if(!confirmation.status()){
  //   std::cout << "Sync Server couldn't  register with coordinator and  exiting!!" << std::endl;
  //   return ;
  // }

  std::thread t1(run_synchronizer,coordIP, coordPort, port_no, synchID);
  /*
  TODO List:
    -Implement service calls
    -Set up initial single heartbeat to coordinator
    -Set up thread to run synchronizer algorithm
  */

  // Wait for the server to shutdown. Note that some other thread must be
  // responsible for shutting down the server for this call to ever return.
  server->Wait();
}


void updateTimeline(std::string user,std::vector<std::string> timeline){

    std::string masterpath = "master" + std::to_string(synchID) + "/" + user + "followingtimeline.txt";
    std::string slavepath = "slave" + std::to_string(synchID) + "/" + user + "followingtimeline.txt";

        // Open the file in append mode
    std::ofstream outputFile;
    std::ofstream slaveOutputFile;
    // Check if the file is open successfully
    outputFile.open(masterpath, std::ios::app);
    while (!outputFile.is_open()) {
        outputFile.open(masterpath, std::ios::app);
        sleep(1);

    }

    // Append each line of the timeline to the file
    for (const std::string& line : timeline) {
        outputFile << line << std::endl;
    }

    // Close the file when done
    outputFile.close();

    slaveOutputFile.open(slavepath, std::ios::app);
    while (!slaveOutputFile.is_open()) {
        slaveOutputFile.open(slavepath, std::ios::app);
        sleep(1);

    }

    // Append each line of the timeline to the file
    for (const std::string& line : timeline) {
        slaveOutputFile << line << std::endl;
    }

    // Close the file when done
    slaveOutputFile.close();



}



int main(int argc, char** argv) {
  
  int opt = 0;
  std::string coordIP;
  std::string coordPort;
  std::string port = "3029";

  while ((opt = getopt(argc, argv, "h:k:p:i:")) != -1){
    switch(opt) {
      case 'h':
          coordIP = optarg;
          break;
      case 'k':
          coordPort = optarg;
          break;
      case 'p':
          port = optarg;
          break;
      case 'i':
          synchID = std::stoi(optarg);
          break;
      default:
	         std::cerr << "Invalid Command Line Argument\n";
    }
  }

  RunServer(coordIP, coordPort, port, synchID);
  return 0;
}

void run_synchronizer(std::string coordIP, std::string coordPort, std::string port, int synchID){
    //setup coordinator stub
    //std::cout<<"synchronizer stub"<<std::endl;
    std::string coord_login_info = coordIP + ":" + coordPort;
    std::unique_ptr<CoordService::Stub> coord_stub_;
   
    ServerInfo serverinfo;
    serverinfo.set_hostname("0.0.0.0");
    serverinfo.set_port(port);
    serverinfo.set_clusterid(synchID);
    serverinfo.set_serverid(synchID);
    serverinfo.set_type("sync");
    Confirmation reg_status;
    coord_stub_ = std::unique_ptr<CoordService::Stub>(CoordService::NewStub(
			       grpc::CreateChannel(
			      coord_login_info, grpc::InsecureChannelCredentials())));
    std::cout<<"registering with "+coord_login_info << std::endl;  
    
    ClientContext context;
    //std::cout << "server details: "+serverDetails.hostname() + " , " <<serverDetails.port() << " ,"<<serverDetails.clusterid() << " ," << serverDetails.serverid() <<" ,"<<std::endl;
    Status status =  coord_stub_->RegisterSyncServer(&context, serverinfo, &reg_status);
  
    if(!reg_status.status()){
        std::cout << "Sync Server couldn't  register with coordinator and  exiting!!" << std::endl;
        return ;
    }
    
    ID id;
    id.set_id(synchID);
    std::string conn_str2 ;
    std::string conn_str1; 
    int stub1_id;
    int stub2_id;
    bool servers_not_received = true;
    
    while(servers_not_received){
         
        ServerList serverList;
         ClientContext contextCor;
        //  std::cout<<"looking to fectch other syncs"<<serverList.hostname_size()<<std::endl;
            serverList.clear_hostname();
            serverList.clear_port();
            serverList.clear_serverid();
         
        // context.clear();
         Status stat  = coord_stub_->GetAllFollowerServers(&contextCor, id, &serverList);
        //   std::cout<<"looking to fectch other syncs ,  list size : "<<std::endl;
         if(serverList.hostname_size() > 1){
             servers_not_received= false;
            conn_str1 = serverList.hostname(0) +":"+ serverList.port(0);
             stub1_id = serverList.serverid(0);
             
              conn_str2  = serverList.hostname(1)+":" + serverList.port(1);
              stub2_id = serverList.serverid(1);
         }   
         sleep(5);
    }
    std::unique_ptr<SynchService::Stub> sync1_stub_;
    std::unique_ptr<SynchService::Stub> sync2_stub_;

    // std::string conn_str1 = serverList.hostname(0) + serverList.port(0);

    // std::string conn_str2 = serverList.hostname(1) + serverList.port(1);

    sync1_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(conn_str1, grpc::InsecureChannelCredentials())));
    sync2_stub_ = std::unique_ptr<SynchService::Stub>(SynchService::NewStub(grpc::CreateChannel(conn_str2, grpc::InsecureChannelCredentials())));
    
    std::cout<<conn_str1<<std::endl;
    std::cout<<conn_str2<<std::endl;
    //send init heartbeat

    //TODO: begin synchronization process
    while(true){
        //change this to 30 eventually
        sleep(20);


        std::vector<std::string> current_users =  get_all_users_func(synchID);
        //std::set<std::string> user_set(current_users.begin(), current_users.end());
        
        std::vector<std::string> other_sync_users;

        grpc::ClientContext context1;
        Confirmation confirm1; 
        AllUsers users1;
        AllUsers users2;
        sync1_stub_->GetAllUsers(&context1,confirm1, &users1);

        for(std::string user : users1.users()){
            current_users.push_back(user);
           // std::cout<<"user : "<<user<<std::endl;
        }


        grpc::ClientContext context2;
        confirm1.clear_status();
        confirm1.clear_is_master();
        sync2_stub_->GetAllUsers(&context2,confirm1, &users2);

        for(std::string user : users2.users()){
            current_users.push_back(user);
           // std::cout<<"user : "<<user<<std::endl;
        }
        
        std::sort(current_users.begin(),current_users.end());
        auto last = std::unique(current_users.begin(),current_users.end());
        current_users.erase(last,current_users.end()); 
        // for(std::string user :current_users){
        //   std::cout<<"user A : "<<user<<std::endl;
        // }
        write_user_to_file(current_users,synchID);

            for(auto i : current_users){
                 
                
                    ID clientid;
                    TLFL tlfl;
                    std::vector<std::string> timeline;
                    
                    grpc::ClientContext timelinecontext1;

                    clientid.set_id(std::stoi(i));
                    std::vector<std::string> addedFollowers;
                    
                    std::vector<std::string> followers =  get_tl_or_fl(synchID, i, false);

                    status = sync1_stub_->GetTLFL(&timelinecontext1,clientid,&tlfl);

                    for(auto fl: tlfl.fl()){
                      if(findUser(followers,fl) == -1 && findUser(addedFollowers,fl) == -1 ){
                        addedFollowers.push_back(fl);
                      }
                    }

                    grpc::ClientContext timelinecontext2;

                    for(auto data : tlfl.tl()){
                        timeline.push_back(data);
                    }

                    tlfl.clear_tl();
                    tlfl.clear_fl();

                    status = sync2_stub_->GetTLFL(&timelinecontext2,clientid,&tlfl);

                    for(auto data : tlfl.tl()){
                        timeline.push_back(data);
                    }

                    for(auto fl: tlfl.fl()){
                      if(findUser(followers,fl) == -1 && findUser(addedFollowers,fl) == -1 ){
                        addedFollowers.push_back(fl);
                      }
                    }

                    updateFollowers(addedFollowers,i);


                    updateTimeline(i,timeline);
                
                    
                 // std::cout << "user : "+i<<std::endl;
            }
    }
    return;
}

void write_user_to_file(std::vector<std::string> users, int synchId){
    std::string master_file_path = "./master"+std::to_string(synchID)+"/all_users.txt";
    std::string slave_file_path = "./slave"+std::to_string(synchID)+"/all_users.txt";

    std::ofstream slave_users_file;
    std::ofstream master_users_file;
    
    master_users_file.open(master_file_path);
    for(std::string user : users){
        master_users_file<< user+"\n";
      //  slave_users_file<< user+"\n";
    }

    
    master_users_file.close();

    slave_users_file.open(slave_file_path);
    for(std::string user : users){
        slave_users_file<< user+"\n";
    }

    slave_users_file.close();
}

std::vector<std::string> get_lines_from_file(std::string filename,bool tl){
  std::vector<std::string> users;
  std::string user;
  std::ifstream file; 

  if(!fs::exists(filename))
         return users;

  file.open(filename);
  while(!file.is_open()){
     file.open(filename);
    //return empty vector if empty file
    //std::cout<<"returned empty vector bc empty file"<<std::endl;
    //file.close();
    sleep(1);
    
  }
  while(getline(file, user)){
   
    if(!user.empty())
      users.push_back(user);
    
    if(tl && user.find("-----") != std::string::npos ){
      
      users.clear();
     // std::cout<<"clear out user  : "<<users.size()<<std::endl;
    }
  } 

  file.close();
  if(tl){
  std::ofstream fileApp;
  fileApp.open(filename,std::ios::app);
  while(!fileApp.is_open()){
     fileApp.open(filename,std::ios::app);
    //return empty vector if empty file
    //std::cout<<"returned empty vector bc empty file"<<std::endl;
    //file.close();
    sleep(1);
    
  }

  fileApp<<"-----\n";
  fileApp.close();

   }
  //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
  /*for(int i = 0; i<users.size(); i++){
    std::cout<<users[i]<<std::endl;
  }*/ 

  return users;
}

bool file_contains_user(std::string filename, std::string user){
    std::vector<std::string> users;
    //check username is valid
    users = get_lines_from_file(filename,false);
    for(int i = 0; i<users.size(); i++){
      //std::cout<<"Checking if "<<user<<" = "<<users[i]<<std::endl;
      if(user == users[i]){
        //std::cout<<"found"<<std::endl;
        return true;
      }
    }
    //std::cout<<"not found"<<std::endl;
    return false;
}

std::vector<std::string> get_all_users_func(int synchID){
    //read all_users file master and client for correct serverID
    std::string master_users_file = "./master"+std::to_string(synchID)+"/all_users.txt";
    std::string slave_users_file = "./slave"+std::to_string(synchID)+"/all_users.txt";
    //take longest list and package into AllUsers message
    std::vector<std::string> master_user_list = get_lines_from_file(master_users_file,false);
    std::vector<std::string> slave_user_list = get_lines_from_file(slave_users_file,false);

    if(master_user_list.size() >= slave_user_list.size())
        return master_user_list;
    else
        return slave_user_list;
}

bool emptyTextFile(const std::string& filePath) {
    std::ofstream outputFile(filePath, std::ofstream::out | std::ofstream::trunc);
    
    if (!outputFile.is_open()) {
        //std::cerr << "Error opening file: " << filePath << std::endl;
        return false;
    }
    
    outputFile.close();
    return true;
}

std::vector<std::string> get_tl_or_fl(int synchID, std::string clientID, bool tl){
    std::string master_fn = "./master"+std::to_string(synchID)+"/"+clientID;
    std::string slave_fn = "./slave"+std::to_string(synchID)+"/" +clientID;
    if(tl){
        master_fn.append("followingtimeline.txt");
        slave_fn.append("followingtimeline.txt");
    }else{
        master_fn.append("followers.txt");
        slave_fn.append("followers.txt");
    }

    std::vector<std::string> m = get_lines_from_file(master_fn,tl);
    if(tl){
        return m;
    }

    std::vector<std::string> s = get_lines_from_file(slave_fn,tl);
    //  if(tl){
    //     emptyTextFile(master_fn);
    //     //emptyTextFile(slave_fn);
    //  }

    if(m.size()>=s.size()){
        return m;
    }else{
        return s;
    }

}
