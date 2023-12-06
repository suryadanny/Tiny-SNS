/*
 *
 * Copyright 2015, Google Inc.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met:
 *
 *     * Redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer.
 *     * Redistributions in binary form must reproduce the above
 * copyright notice, this list of conditions and the following disclaimer
 * in the documentation and/or other materials provided with the
 * distribution.
 *     * Neither the name of Google Inc. nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 */

#include <ctime>

#include <google/protobuf/timestamp.pb.h>
#include <google/protobuf/duration.pb.h>
#include <thread>
#include <fstream>
#include <iostream>
#include <memory>
#include <string>
#include <stdlib.h>
#include <unistd.h>
#include <google/protobuf/util/time_util.h>
#include <grpc++/grpc++.h>
#include<glog/logging.h>
#define log(severity, msg) LOG(severity) << msg; google::FlushLogFiles(google::severity); 
#include "coordinator.grpc.pb.h"
#include "sns.grpc.pb.h"
#include <queue>


using google::protobuf::Timestamp;
using google::protobuf::Duration;
using grpc::Server;
using grpc::ServerBuilder;
using grpc::ServerContext;
using grpc::ServerReader;
using grpc::ClientContext;
using grpc::ServerReaderWriter;
using grpc::ServerWriter;
using grpc::Status;
using csce438::Message;
using csce438::ListReply;
using csce438::Request;
using csce438::Reply;
using csce438::CoordService;
using csce438::Confirmation;
using csce438::ServerInfo;
using csce438::SNSService;

struct Client {
  std::string username;
  bool connected = false;
  int following_file_size = 0;
  std::vector<Client*> client_followers;
  std::vector<Client*> client_following;
  ServerReaderWriter<Message, Message>* stream = 0;
  bool operator==(const Client& c1) const{
    return (username == c1.username);
  }
};

//Vector that stores every client that has been created
std::vector<Client*> client_db;
std::unique_ptr<CoordService::Stub> coord_stub_;
std::string global_server_data_file = "server.txt";
std::atomic_bool master(false);
int Gl_ServerId;
int Gl_ClusterId;
void updatefollowers(Client *c);
std::string slaveAddress;

struct SlaveRequests{

    Request request;
    std::string command;

};

SlaveRequests slaverequests;

std::unique_ptr<SNSService::Stub> slavestub_;  
std::queue<SlaveRequests> requ;

//Helper function used to find a Client object given its username
int find_user(std::string username){
  int index = 0;
  
  for(Client* c : client_db){
    if(c->username == username)
      return index;
    index++;
  }
  return -1;
}


void updateList(){

    
    std::string filePath;
    
    if(master == true)
      filePath = "./master"+std::to_string(Gl_ClusterId)+"/all_users.txt";
    else
      filePath = "./slave"+std::to_string(Gl_ClusterId)+"/all_users.txt";

    // Open the file
    std::ifstream inputFile(filePath);

    // Check if the file was successfully opened
    if (!inputFile.is_open()) {
        std::cerr << "Failed to open the file: " << filePath << std::endl;

    }

    std::string user;
    std::vector<std::string> all_users;
    // Read the file line by line
    while (std::getline(inputFile, user)) {
      all_users.push_back(user);
      int user_index = find_user(user);
      if(user_index < 0){
      
      Client* c = new Client();
      c->username = user;
      client_db.push_back(c);
      }
      

    }

    


    // Close the file
    inputFile.close();
    for(Client* c : client_db){
      updatefollowers(c);
    }
}

void saveFollower(std::string user1, std::string user2) {
    


    std::string filepath;

    if (master == true)
        filepath = "./master" + std::to_string(Gl_ClusterId) + "/" + user2 + "followers.txt";
    else
        filepath = "./slave" + std::to_string(Gl_ClusterId) + "/" + user2 + "followers.txt";

    // Open the file for appending
    std::ofstream file(filepath, std::ios::app);

    if (!file.is_open()) {
        std::cerr << "Failed to open the file: " << filepath << std::endl;
        return ;
    }

    // Append random text to the file
    file << user1 << std::endl;
    
    // Close the file
    file.close();

    std::cout << "Appended new follower to the file: " << filepath << std::endl; 


}

  void saveUser(std::string userID){


      std::string filePath;
      
      if(master == true)
        filePath = "./master"+std::to_string(Gl_ClusterId)+"/all_users.txt";
      else
        filePath = "./slave"+std::to_string(Gl_ClusterId)+"/all_users.txt";
      
      std::cout<<filePath<<"\n";

     std::ifstream fileCheck(filePath);
    if (!fileCheck.is_open()) {
        std::ofstream newFile(filePath);
        newFile.close();
    }

    // Open the file for appending (ios::app)
    std::ofstream file(filePath, std::ios::app);

      if (file.is_open()) {
          // Append the userID to the file
          file << userID << std::endl;

          // Close the file
          file.close();
          
          std::cout << "UserID appended to the file." << std::endl;
      } else {
          std::cerr << "Failed to open the file for appending." << std::endl;
      }


  }

void updatefollowers(Client *c){

    std::string filePath;

    if (master == true)
        filePath = "./master" + std::to_string(Gl_ClusterId) + "/" + c->username + "followers.txt";
    else
        filePath = "./slave" + std::to_string(Gl_ClusterId) + "/" + c->username + "followers.txt";

    int arr[10000] = {};

    if(master == true){

      for(auto follower:c->client_followers){

        int userint = std::stoi(follower->username);
        arr[userint] = 1;

      }

    }

    std::ifstream inputFile(filePath);

    // Check if the file is open successfully
    if (!inputFile.is_open()) {
        std::cerr << "Error opening the file." << std::endl;
            std::ofstream newFile(filePath);
            newFile.close();
        
    }

    std::string line;
    
    // Read and process each line in the file
    while (std::getline(inputFile, line)) {
        // Process the line here (e.g., print it)
        
        int userint = std::stoi(line);

        if(arr[userint] == 0)
        { 

           if(find_user(line) != -1){
             Client *newfollower = client_db[find_user(line)];
             newfollower->username = line;
            c->client_followers.push_back(newfollower);
            newfollower->client_following.push_back(c);
           }
            else{
              Client *newfollower = new Client();
              newfollower->username = line;
              c->client_followers.push_back(newfollower);
              newfollower->client_following.push_back(c);
            }

          
          
          arr[userint] = 1;
        }
    }

    // Close the file when done
    inputFile.close();

  }

bool emptyTextFile(const std::string& filePath) {
    std::ofstream outputFile(filePath, std::ofstream::out | std::ofstream::trunc);
    
    if (!outputFile.is_open()) {
        std::cerr << "Error opening file: " << filePath << std::endl;
        return false;
    }
    
    outputFile.close();
    return true;
}

std::vector<std::string> get_lines_from_file(std::string filename){
  std::vector<std::string> users;
  std::string user;
  std::ifstream file; 

  file.open(filename);
  if(!file.is_open()){
    //return empty vector if empty file

    return users;
  }
  while(getline(file,user)){
    

    if(!user.empty())
      users.push_back(user);
  } 

  file.close();

  //std::cout<<"File: "<<filename<<" has users:"<<std::endl;
  /*for(int i = 0; i<users.size(); i++){
    std::cout<<users[i]<<std::endl;
  }*/ 

  return users;
}



  

void readTimeline(std::string user, ServerReaderWriter<Message, Message>* stream){
    std::string delimiter = "::";
    std::string prefix = "-----";
   // -----sync
   sleep(2);
     std::vector<std::string> records;
    Client* c = client_db[find_user(user)];
    while(c->connected == true){  
     // std::cout<<"running"<<std::endl;
      std::vector<std::string> timeline;

      std::string filepath;

      filepath = "./master" + std::to_string(Gl_ClusterId) + "/" + user + "followingtimeline.txt";

      timeline = get_lines_from_file(filepath);

      if(timeline.empty() == true)
       continue;

    // emptyTextFile(filepath);

       
      std::sort(timeline.begin(),timeline.end());
      // auto last = std::unique(timeline.begin(),timeline.end());
      // timeline.erase(last,timeline.end()); 

      

      for(std::string rec : timeline){
          int in = rec.find(":");
          if(in != std::string::npos && std::find(records.begin(),records.end(),rec.substr(in-1))==records.end()){
              records.push_back(rec.substr(in-1));
             // std::cout<<"records added : "<<rec.substr(in-2)<<std::endl;
          }    
      }

     // std::cout<<"records available :  "<<records.size()<<std::endl;
      for(auto data : records){
            Message msg;
            msg.set_username(data.substr(0,1));
              msg.set_msg(data.substr(2));
          // std::cout<<" call "<< std::endl;
            stream->Write(msg);
            sleep(1);
        
      }
      sleep(10);
    }
}



class SNSServiceImpl final : public SNSService::Service {
  
  Status List(ServerContext* context, const Request* request, ListReply* list_reply) override { 

    std::cout<<"updating user "<<std::endl;
    updateList();
    std::vector<std::string> fl;
    fl.clear();
    log(INFO,"Serving List Request from: " + request->username()  + "\n");
    for(Client* user: client_db){
      list_reply->add_all_users(user->username);
      if(user->username == request->username()){
         for(Client* follower : user->client_followers){
              if(std::find(fl.begin(),fl.end(),follower->username) == fl.end()){
                list_reply->add_followers(follower->username);
                fl.push_back(follower->username);
              } 
         }
      }
    }
    // Client* user = client_db[find_user(request->username())];
    // std::cout<<"list over here"<<std::endl;
    // int index = 0;
    // for(Client* c : client_db){
    //   list_reply->add_all_users(c->username);
    // }
    // std::vector<Client*>::const_iterator it;
    // for(it = user->client_followers.begin(); it!=user->client_followers.end(); it++){
    //   list_reply->add_followers((*it)->username);
    // }
    return Status::OK;
  }

  Status Follow(ServerContext* context, const Request* request, Reply* reply) override {
    updateList();

    slaverequests.command = "Follow";

    std::string username1 = request->username();
    std::string username2 = request->arguments(0);

    slaverequests.request.set_username(username1);
    slaverequests.request.add_arguments(username2);
    if(master){
      std::cout<<"sending follow  request to slave queue"<<std::endl;
      requ.push(slaverequests);
    }


    log(INFO,"Serving Follow Request from: " + username1 + " for: " + username2 + "\n");

    int join_index = find_user(username2);
    if(join_index < 0 || username1 == username2)
      reply->set_msg("Join Failed -- Invalid Username");
    else{
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[join_index];      
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) != user1->client_following.end()){
	reply->set_msg("Join Failed -- Already Following User");
        return Status::OK;
      }
      user1->client_following.push_back(user2);
      user2->client_followers.push_back(user1);
      reply->set_msg("Follow Successful");
      saveFollower(username1,username2);
    }
    return Status::OK; 
  }

  Status UnFollow(ServerContext* context, const Request* request, Reply* reply) override {
    std::string username1 = request->username();
    std::string username2 = request->arguments(0);
    log(INFO,"Serving Unfollow Request from: " + username1 + " for: " + username2);
 
    int leave_index = find_user(username2);
    if(leave_index < 0 || username1 == username2) {
      reply->set_msg("Unknown follower");
    } else{
      Client *user1 = client_db[find_user(username1)];
      Client *user2 = client_db[leave_index];
      if(std::find(user1->client_following.begin(), user1->client_following.end(), user2) == user1->client_following.end()){
	reply->set_msg("You are not a follower");
        return Status::OK;
      }
      
      user1->client_following.erase(find(user1->client_following.begin(), user1->client_following.end(), user2)); 
      user2->client_followers.erase(find(user2->client_followers.begin(), user2->client_followers.end(), user1));
      reply->set_msg("UnFollow Successful");
    }
    return Status::OK;
  }

  // RPC Login
  Status Login(ServerContext* context, const Request* request, Reply* reply) override {

    slaverequests.request.set_username(request->username());
    slaverequests.command = "Login";
    if(master){
       requ.push(slaverequests);
       std::cout<<"sending login  request to slave queue"<<std::endl;
    }
    Client* c = new Client();
    std::string username = request->username();
    std::string filePath;
    log(INFO, "Serving Login Request: " + username + "\n");
    if(master == true)
        filePath = "./master"+std::to_string(Gl_ClusterId)+"/all_users.txt";
    else
        filePath = "./slave"+std::to_string(Gl_ClusterId)+"/all_users.txt";
  //  std::ofstream postToFile;
    // postToFile.open(filePath,std::ios::app);
    int user_index = find_user(username);
    if(user_index < 0){
      c->username = username;
      client_db.push_back(c);
     // postToFile<<username+"\n";
      saveUser(username);
      //postToFile.close();
      
      reply->set_msg("Login Successful!");
    }
    else{
      Client *user = client_db[user_index];
      if(user->connected) {
	log(WARNING, "User already logged on");
        reply->set_msg("you have lready joined");
      }
      else{
        std::string msg = "Welcome Back " + user->username;
	      reply->set_msg(msg);
         // user->connected = true;
      }
    }
    return Status::OK;
  }

  Status Timeline(ServerContext* context, 
		ServerReaderWriter<Message, Message>* stream) override {
    log(INFO,"Serving Timeline Request");
    std::string initpathmaster = "./master" + std::to_string(Gl_ClusterId) + "/";
    std::string initpathslave = "./slave" + std::to_string(Gl_ClusterId) + "/";
    Message message;
    Client *c;

    bool firstTime = false;
     std::string user;
  //  stream->Read(&message);

   std::thread rt;
    while(stream->Read(&message)) {


      std::string username = message.username();
      if(message.msg() == "Set Stream"){
            
          user = username;
          int user_index = find_user(username);
          c = client_db[user_index];
          c->connected=true;
      //   message.set_msg("");
        ServerReaderWriter<Message, Message>* readstream;
        readstream = stream;
        std::cout<<"sending stream for timeline"<<std::endl;
        rt = std::thread(readTimeline,std::ref(message.username()),readstream);
       // rt.detach();

      }


      

      updatefollowers(c);

      firstTime = true;

 
      //Write the current message to "username.txt"
      std::string filename = initpathmaster+username+"timeline.txt";
      std::string filenameslave = initpathslave+username+"timeline.txt";

      std::cout<<"Filepath = "<<filename<<"\n";

      std::ofstream user_file(filename,std::ios::app|std::ios::out|std::ios::in);
      std::ofstream user_file_slave(filenameslave,std::ios::app|std::ios::out|std::ios::in);

      google::protobuf::Timestamp temptime = message.timestamp();
      std::string time = google::protobuf::util::TimeUtil::ToString(temptime);
      std::string fileinput = message.username()+":"+message.msg()+"\n";
      //"Set Stream" is the default message from the client to initialize the stream
      if(message.msg() != "Set Stream"){
        user_file << fileinput;
        user_file_slave<<fileinput;
      //If message = "Set Stream", print the first 20 chats from the people you follow
      }
      else{
        if(c->stream==0)
      	  c->stream = stream;
          std::string line;
          std::vector<std::string> newest_twenty;
          std::ifstream in(initpathmaster+username+"followingtimeline.txt");
          int count = 0;
        //Read the last up-to-20 lines (newest 20 messages) from userfollowing.txt
        while(getline(in, line)){
//          count++;
//          if(c->following_file_size > 20){
//	    if(count < c->following_file_size-20){
//	      continue;
//            }
//          }
          newest_twenty.push_back(line);
        }
        Message new_msg; 
 	//Send the newest messages to the client to be displayed
        if(newest_twenty.size() >= 40){ 	
          for(int i = newest_twenty.size()-40; i<newest_twenty.size(); i+=2){
            new_msg.set_msg(newest_twenty[i]);
            stream->Write(new_msg);
          }
        }else{
              for(int i = 0; i<newest_twenty.size(); i+=2){
                new_msg.set_msg(newest_twenty[i]);
                stream->Write(new_msg);
              }
        }
        //std::cout << "newest_twenty.size() " << newest_twenty.size() << std::endl; 
        continue;
      }
      //Send the message to each follower's stream
      std::vector<Client*>::const_iterator it;
      for(it = c->client_followers.begin(); it!=c->client_followers.end(); it++){
        Client *temp_client = *it;
      	if(temp_client->stream!=0 && temp_client->connected)
	            temp_client->stream->Write(message);
        //For each of the current user's followers, put the message in their following.txt file
        std::string temp_username = temp_client->username;
        std::string temp_file = initpathmaster + temp_username + "followingtimeline.txt";
	      std::ofstream following_file(temp_file,std::ios::app|std::ios::out|std::ios::in);
	      following_file << fileinput;
        std::string slave_temp_file = initpathslave + temp_username + "followingtimeline.txt";
	      std::ofstream  slave_following_file(slave_temp_file,std::ios::app|std::ios::out|std::ios::in);
	      slave_following_file << fileinput;
        temp_client->following_file_size++;
	      std::ofstream user_file(initpathmaster + temp_username + "timeline.txt",std::ios::app|std::ios::out|std::ios::in);
        user_file << time+" - "+fileinput;
        std::ofstream slave_user_file(initpathslave + temp_username + "timeline.txt",std::ios::app|std::ios::out|std::ios::in);
        slave_user_file << time+" - "+fileinput;
      }
    }
    //  slaverequests.command = "Logout";
    // slaverequests.request.set_username(user);
    // requ.push(slaverequests);

    
    c->connected = false;
    rt.join();
    //If the client disconnected from Chat Mode, set connected to false
    
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
    Status status =  coord_stub_->CreatePath(&context, serverDetails, &confirmation);
    //master = confirmation.is_master();
    return status;
}

void GenerateHeartbeat(ServerInfo& serverinfo, std::atomic_bool& master){
    
    
    while(true){
         ClientContext context;
         Confirmation confirm;
         //std::cout<<"triggerring heartbeat"<<std::endl;
         coord_stub_->Heartbeat(&context, serverinfo, &confirm);
         
         if(!master && confirm.is_master()){
          //std::cout<<"updated to master : "<<master<<std::endl;
           std::cout<< " assigned master replacing files "<<std::endl;
         }

         master = confirm.is_master();
         //context.clear();
         sleep(5);
    }   
}

void syncSlave() {

  if(master == false)
    return;

    SlaveRequests sync;

    ServerInfo curr_server;
      
      curr_server.set_clusterid(Gl_ClusterId);
      curr_server.set_serverid(Gl_ServerId);
    while(true){

      ClientContext context;

      ServerInfo slaveServerInfo;
      
      //std::cout<<" slave of server id  :"<<Gl_ServerId<<std::endl;
      Status status = coord_stub_->GetCounterpart(&context, curr_server, &slaveServerInfo);

      if(slaveServerInfo.hostname() != "NA"){
        log(INFO,"Got Slave Info!\n");
        slaveAddress = slaveServerInfo.hostname() + ":" + slaveServerInfo.port();
        
        slavestub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
          grpc::CreateChannel(
        slaveAddress, grpc::InsecureChannelCredentials())));

        break;
      }
      sleep(3);
    }


    log(INFO,"Slave addr = " + slaveAddress + "\n");      

    
    while(true){
          
      sleep(5);
      
      ClientContext slavecontext;

      if(requ.empty() == false){
      
        sync = requ.front();
        requ.pop();        

        std::string cmd = sync.command;

        log(INFO,"Syncronising Command: " + cmd +  " with Slave.\n");
        
        if(cmd == "Login"){

        Request slaveRequest;
        slaveRequest.set_username(sync.request.username());
        
        Reply slaveReply;
        grpc::Status status = slavestub_->Login(&slavecontext,slaveRequest,&slaveReply);
        }
        else if(cmd == "Follow"){
        
         Request slaveRequest;
         slaveRequest.set_username(sync.request.username());
         slaveRequest.add_arguments(sync.request.arguments(0));

         Reply slaveReply;
         Status status = slavestub_->Follow(&slavecontext,slaveRequest,&slaveReply);

        }

      }

    }

}


void getSlave(){

    // std::shared_ptr<grpc::Channel> channel = grpc::CreateChannel(coordinatorAddress, grpc::InsecureChannelCredentials());
    
    // // Create a stub for the coordinator service.
    // std::unique_ptr<csce438::CoordService::Stub> stub = csce438::CoordService::NewStub(channel);    
    // ID id;
    
          ServerInfo curr_server;
      
      curr_server.set_clusterid(Gl_ClusterId);
      curr_server.set_serverid(Gl_ServerId);
    while(true){

      ClientContext context;

      ServerInfo slaveServerInfo;
      
      //std::cout<<" slave of server id  :"<<Gl_ServerId<<std::endl;
      Status status = coord_stub_->GetCounterpart(&context, curr_server, &slaveServerInfo);

      if(slaveServerInfo.hostname() != "NA"){
        log(INFO,"Got Slave Info!\n");
        slaveAddress = slaveServerInfo.hostname() + ":" + slaveServerInfo.port();
        
        slavestub_ = std::unique_ptr<SNSService::Stub>(SNSService::NewStub(
          grpc::CreateChannel(
        slaveAddress, grpc::InsecureChannelCredentials())));

        break;
      }
      sleep(3);
    }


    log(INFO,"Slave addr = " + slaveAddress + "\n");      



    

}

void RunServer(std::string port_no, std::string coord_host, std::string coord_port, int clusterid, int serverid) {
  std::string server_address = "0.0.0.0:"+port_no;
  SNSServiceImpl service;
  ServerInfo serverinfo;
  serverinfo.set_hostname("0.0.0.0");
  serverinfo.set_port(port_no);
  serverinfo.set_clusterid(clusterid);
  serverinfo.set_serverid(serverid);
  serverinfo.set_type("server");
  Gl_ServerId = serverid;
  Gl_ClusterId = clusterid;
  std::string coord_login_info = coord_host + ":" + coord_port;
  Confirmation confirmation;
  
  
  ServerBuilder builder;
  builder.AddListeningPort(server_address, grpc::InsecureServerCredentials());
  builder.RegisterService(&service);
  std::unique_ptr<Server> server(builder.BuildAndStart());
  std::cout << "Server listening on " << server_address << std::endl;
  log(INFO, "Server listening on "+server_address);
  
  // std::ifstream user_data;
  // user_data.open(global_server_data_file);
  // std::string username;
  // if(user_data.is_open()){
  //   while(getline(user_data,username)){
  //     Client* c = new Client;
  //     c->username = username;
  //     client_db.push_back(c);
  //   }
  //   user_data.close();
  // }

  Status status  = RegisterWithCoordinator(serverinfo, confirmation, coord_login_info);
  std::cout<< "coordinator infor : "<< coord_login_info << std::endl;
  //std::cout << "server details: "+serverinfo.hostname() + " , " <<serverinfo.port() + " ,"<<serverinfo.clusterid() << " ," << serverinfo.serverid() << " ,"<<std::endl;
  if(!confirmation.status()){
    std::cout << "Server could  register with coordinator and  exiting!!" << std::endl;
    return ;
  }
  master = confirmation.is_master();

  std::cout<<"server is master : "<<master<<std::endl;
  std::thread heartbeat(GenerateHeartbeat,std::ref(serverinfo),std::ref(master));

  //std::thread functionThread2 (getSlave);  

  std::thread functionThread3 (syncSlave);



  server->Wait();
}



int main(int argc, char** argv) {

  
  
  std::string port = "3010";
  std::string coord_host = "0.0.0.0";
  std::string coord_port = "0.0.0.0";
  int serverid = 0;
  int clusterid = 0 ;
  int opt = 0;
  while ((opt = getopt(argc, argv, "c:s:h:k:p:")) != -1){
    switch(opt) {
      case 'p':
          port = optarg;break;
      case 'h':
          coord_host = optarg;break;
      case 's':
          serverid = atoi(optarg);break;
      case 'c':
          clusterid = atoi(optarg);break;
      case 'k':
           coord_port = optarg;break;
      default:
	  std::cerr << "Invalid Command Line Argument\n";
    }
  }
  
  global_server_data_file = "server-"+std::to_string(serverid)+"-data.txt";
  std::string log_file_name = std::string("server-") + port;
  google::InitGoogleLogging(log_file_name.c_str());
  log(INFO, "Logging Initialized. Server starting...");
  RunServer(port,coord_host,coord_port,clusterid,serverid);

  return 0;
}
