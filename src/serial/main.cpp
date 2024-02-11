/* 
 * tcpserver.c - A multithreaded TCP echo server 
 * usage: tcpserver <port>
 * 
 * Testing : 
 * nc localhost <port> < input.txt
 */

#include <sys/socket.h>
#include <error.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <map>
#include <sstream>

using namespace std;

string db_read(string keyname,map<string, string> &db){
      auto it = db.find(keyname);
      return (it != db.end()) ? it->second : "";
}
int db_delete(string keyname,map<string, string> &db){
    auto it = db.find(keyname);
    if(it!=db.end()){
      db.erase(it);
      return 1;
    }
    return 0;
}
int db_write(string keyname, string value,map<string, string> &db){
    db.insert(pair<string, string>(keyname, value));
    return 1;
}
int db_count(map<string, string> &db){
    return db.size();
}

void process_data(int socket, string data, map<string, string> &db){
  istringstream iss(data);
  string op;
  int res;
  char successmsg[] = "FIN\n";
  char failuremsg[] = "NULL\n";

  while(iss >> op){
    //cout<<op<<"\n";
    if(op=="WRITE"){
      string keyname, value;
      iss >> keyname;
      iss >> value;
      if(db_write(keyname,value.substr(1),db)){
        send(socket, successmsg,strlen(successmsg),0);
      }

      #ifdef DEBUG
        cout << "Write Detected\n";
        cout << "Detected key: "<<keyname<<"\n";
        cout<< "Detected Value: "<<(value.substr(1))<<"\n";
      #endif

    }else if(op=="READ"){
      string keyname;
      string resp;
      iss >> keyname;
      resp = db_read(keyname,db)+"\n";
      if(resp == "\n"){
        send(socket, failuremsg, strlen(failuremsg), 0);
      }else{
        send(socket, &resp[0], resp.size(),0);
      }

      #ifdef DEBUG
        cout<< " READ detected\n";
        cout << "Read value: "<<resp<<"\n";
      #endif

    }else if(op=="END"){
      send(socket, "\n", 1, 0);
      close(socket);

    }else if(op == "DELETE"){
      string keyname;
      iss >> keyname;
      int del = db_delete(keyname,db);
      if(del){
        send(socket, successmsg, strlen(successmsg), 0);
      }else{
        send(socket, failuremsg, strlen(failuremsg), 0);
      }
      #ifdef DEBUG
        cout<< " Delete detected\n";
      #endif
    }else if(op=="COUNT"){
      int count = db_count(db);
      string res = to_string(count)+'\n';
      send(socket,res.c_str(),res.size(),0);
    }else{
      ;
    }
    #ifdef DEBUG
      else if(op == "DISPLAY"){
        cout<<"-----------DEBUG-START------------\n";
        map<string, string>::iterator it = db.begin();
        while(it!=db.end()){
          cout<< it->first << " : " << it->second << "\n";
          ++it;
        }
        cout<<"-------------DEBUG-END------------\n";
        cout<<"\n";
      }
    #endif
  }
}

void start_listen(struct sockaddr_in &address, int server_fd){
  map<string, string> db;
  int addrlen = sizeof(address);
  while (true) {
    printf("---Waiting for connection---\n");
    int new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);
    if (new_socket < 0) {
      perror("Cannot create new socket\n");
      exit(EXIT_FAILURE);
    }

    char buffer[1024] = {0};

    ssize_t valread;
    while ((valread = read(new_socket, buffer, sizeof(buffer))) > 0) {
      process_data(new_socket, buffer, db);
      memset(buffer, 0, sizeof(buffer));
    }

    char newline[] = "\n";
    send(new_socket,newline,sizeof(newline),0); 

    close(new_socket);
    }
}

void initialize_server(int port, struct sockaddr_in &address, int server_fd, int server_bind, int server_listen){
  int addrlen = sizeof(address);
  address.sin_family = AF_INET;
  address.sin_addr.s_addr = htonl(INADDR_ANY);
  address.sin_port = htons(port);

  if((server_fd = socket(AF_INET, SOCK_STREAM, 0))<0){
  perror("Cannot create socket\n");
  exit(EXIT_FAILURE);
  }

  if((server_bind = bind(server_fd, (struct sockaddr *)&address, sizeof(address)))<0){
    perror("Could not bind\n");
    exit(EXIT_FAILURE);
  };

  if((server_listen = listen(server_fd, 3))<0){
    perror("Cannot listen\n");
    exit(EXIT_FAILURE);
  }
  start_listen(address,server_fd);
}

int main(int argc, char ** argv) {
  int portno; /* port to listen on */
  
  /* 
   * check command line arguments 
   */
  if (argc != 2) {
    fprintf(stderr, "usage: %s <port>\n", argv[0]);
    exit(1);
  }

  // DONE: Server port number taken as command line argument
  portno = atoi(argv[1]);
  int server_fd,server_bind,server_listen;
  struct sockaddr_in address;
  size_t addrlen;
  
  
  initialize_server(portno, address, server_fd, server_bind, server_listen);

}

