#include <sys/socket.h>
#include <error.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>
#include <string.h>
#include <iostream>
#include <map>
#include <sstream>
#include <pthread.h>
#include <vector>

pthread_mutex_t Qmutex = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t DBmutex = PTHREAD_MUTEX_INITIALIZER;

using namespace std;

map<string, string> db;

struct ThreadArgs {
    int new_socket;
    pthread_t thread_id;
};

vector<ThreadArgs> thread_queue;

string db_read(string keyname) {
    auto it = db.find(keyname);
    return (it != db.end()) ? it->second : "";
}

int db_delete(string keyname) {
    auto it = db.find(keyname);
    if (it != db.end()) {
        db.erase(it);
        return 1;
    }
    return 0;
}

int db_write(string keyname, string value) {
    db.insert(pair<string, string>(keyname, value));
    return 1;
}

int db_count() {
    return db.size();
}

void process_data(int socket, string data) {
    istringstream iss(data);
    string op;
    char successmsg[] = "FIN\n";
    char failuremsg[] = "NULL\n";

    while (iss >> op) {
        if (op == "WRITE") {
            string keyname, value;
            iss >> keyname;
            iss >> value;
            pthread_mutex_lock(&DBmutex);
            if (db_write(keyname, value.substr(1))) {
                send(socket, successmsg, strlen(successmsg), 0);
            }
            pthread_mutex_unlock(&DBmutex);
        } else if (op == "READ") {
            string keyname;
            string resp;
            iss >> keyname;
            resp = db_read(keyname) + "\n";
            if (resp == "\n") {
                send(socket, failuremsg, strlen(failuremsg), 0);
            } else {
                send(socket, &resp[0], resp.size(), 0);
            }
        } else if (op == "END") {
            send(socket, "\n", 1, 0);
            close(socket);
        } else if (op == "DELETE") {
            string keyname;
            iss >> keyname;
            pthread_mutex_lock(&DBmutex);
            int del = db_delete(keyname);
            if (del) {
                send(socket, successmsg, strlen(successmsg), 0);
            } else {
                send(socket, failuremsg, strlen(failuremsg), 0);
            }
            pthread_mutex_unlock(&DBmutex);
        } else if (op == "COUNT") {
            int count = db_count();
            string res = to_string(count) + '\n';
            send(socket, res.c_str(), res.size(), 0);
        }
    }
}

void* handle_client(void* arg) {
    ThreadArgs* thread_args = static_cast<ThreadArgs*>(arg);
    int new_socket = thread_args->new_socket;

    char buffer[1024] = {0};
    ssize_t valread;

    while ((valread = read(new_socket, buffer, sizeof(buffer))) > 0) {
        process_data(new_socket, buffer);
        memset(buffer, 0, sizeof(buffer));
    }

    char newline[] = "\n";
    send(new_socket, newline, sizeof(newline), 0);

    close(new_socket);

    pthread_mutex_lock(&Qmutex);
    for (auto it = thread_queue.begin(); it != thread_queue.end(); ++it) {
        if (it->thread_id == pthread_self()) {
            thread_queue.erase(it);
            break;
        }
    }
    pthread_mutex_unlock(&Qmutex);

    delete thread_args;
    pthread_exit(NULL);
}

void initialize_server(int portno, struct sockaddr_in address, int server_fd, int server_bind, int server_listen) {
    int addrlen = sizeof(address);
    if ((server_fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
        perror("Cannot create socket\n");
        exit(EXIT_FAILURE);
    }

    if ((server_bind = bind(server_fd, (struct sockaddr*)&address, sizeof(address))) < 0) {
        perror("Could not bind\n");
        exit(EXIT_FAILURE);
    }

    if ((server_listen = listen(server_fd, 3)) < 0) {
        perror("Cannot listen\n");
        exit(EXIT_FAILURE);
    }

    while (true) {
        printf("---Waiting for connection---\n");
        int new_socket = accept(server_fd, (struct sockaddr*)&address, (socklen_t*)&addrlen);
        if (new_socket < 0) {
            perror("Cannot create new socket\n");
            exit(EXIT_FAILURE);
        }

        ThreadArgs* thread_args = new ThreadArgs{new_socket};
        pthread_t thread_id;

        pthread_mutex_lock(&Qmutex);
        thread_queue.push_back({new_socket, thread_id});
        pthread_mutex_unlock(&Qmutex);

        if (pthread_create(&thread_id, NULL, handle_client, (void*)thread_args) != 0) {
            perror("Failed to create thread\n");
            exit(EXIT_FAILURE);
        }
    }
}

int main(int argc, char** argv) {
    int portno;
    if (argc != 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        exit(1);
    }

    portno = atoi(argv[1]);
    int server_fd, server_bind, server_listen;
    struct sockaddr_in address;
  
    size_t addrlen = sizeof(address);
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(portno);

    initialize_server(portno, address, server_fd, server_bind, server_listen);

    return 0;
}
