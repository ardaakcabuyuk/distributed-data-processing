#include "CurlEasyPtr.h"
#include <iostream>
#include <sstream>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h>
#include <arpa/inet.h>
#include <sys/wait.h>
#include <signal.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <vector>
#include <map>
#include <queue>

using namespace std::literals;

#define BACKLOG 10   // how many pending connections queue will hold
#define MAXDATASIZE 1024 // max number of bytes we can get at once 
#define TRUE 1

void sigchld_handler(int s) {
   // waitpid() might overwrite errno, so we save and restore it:
   int saved_errno = errno;
   while(waitpid(-1, NULL, WNOHANG) > 0);
   errno = saved_errno;
   int temp = s;
   temp++;
}

void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}


/// Leader process that coordinates workers. Workers connect on the specified port
/// and the coordinator distributes the work of the CSV file list.
/// Example:
///    ./coordinator http://example.org/filelist.csv 4242
int main(int argc, char* argv[]) {
   //std::cout << "Coordinator started" << std::endl;
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <URL to csv list> <listen port>" << std::endl;
      return 1;
   }

   unsigned short int PORT = (unsigned short) atoi(argv[2]);
   int opt = TRUE;  
   int master_socket, addrlen, new_socket, client_socket[64], 
         max_clients = 64, activity, sd;  
   int max_sd;  
   long valread;
   struct sockaddr_in address;  
   std::map<int, unsigned long> worker_map;
   std::queue<unsigned long> failed;
   char buffer[1025];  //data buffer of 1K 
   
   //set of socket descriptors 
   fd_set readfds;  

   auto curlSetup = CurlGlobalSetup();
   auto listUrl = std::string(argv[1]);

   // Download the file list
   auto curl = CurlEasyPtr::easyInit();
   curl.setUrl(listUrl);
   auto fileList = curl.performToStringStream();

   unsigned long total = 0;
   // Iterate over all files

   std::vector<std::string> urls;
   std::string url;
   while (std::getline(fileList, url, '\n')) {
      // Line contains string of length > 0 then save it in vector
      if(url.size() > 0)
         urls.push_back(url);
   }
   unsigned long url_count = urls.size();

   //initialise all client_socket[] to 0 so not checked 
   for (int i = 0; i < max_clients; i++) {  
      client_socket[i] = 0;  
   }  

   //create a master socket 
   if((master_socket = socket(AF_INET , SOCK_STREAM , 0)) == 0) {  
   //   perror("coordinator: socket failed");  
      exit(EXIT_FAILURE);  
   } 

   //set coordinator socket to allow multiple connections , 
   //this is just a good habit, it will work without this 
   if(setsockopt(master_socket, SOL_SOCKET, SO_REUSEADDR, (char *)&opt, sizeof(opt)) < 0) {  
   //   perror("coordinator: setsockopt");  
      exit(EXIT_FAILURE);  
   }  

   //type of socket created 
   address.sin_family = AF_INET;  
   address.sin_addr.s_addr = INADDR_ANY;  
   address.sin_port = htons(PORT); 

   //bind the socket to localhost port 8888 
   if (bind(master_socket, (struct sockaddr *)&address, sizeof(address)) < 0) {  
   //   perror("coordinator: bind failed");  
      exit(EXIT_FAILURE);  
   }  
   //printf("coordinator: listening on port %d \n", PORT); 

   if (listen(master_socket, BACKLOG) < 0) {  
   //   perror("coordinator: listen");  
      exit(EXIT_FAILURE);  
   }  

   //accept the incoming connection 
   addrlen = sizeof(address);  
   //puts("coordinator: waiting for connections...");  

   unsigned url_idx = 0;
   unsigned received = 0;
   while (received < url_count) {  // main accept() loop
      //clear the socket set 
      FD_ZERO(&readfds);  

      //add master socket to set 
      FD_SET(master_socket, &readfds);  
      max_sd = master_socket;  
            
      //add child sockets to set 
      for (int i = 0; i < max_clients; i++) {  
         //socket descriptor 
         sd = client_socket[i];  
               
         //if valid socket descriptor then add to read list 
         if (sd > 0)  
            FD_SET(sd , &readfds);  
               
         //highest file descriptor number, need it for the select function 
         if (sd > max_sd)  
            max_sd = sd;  
      }  

      //wait for an activity on one of the sockets , timeout is NULL , 
      //so wait indefinitely 
      activity = select(max_sd + 1 , &readfds , NULL , NULL , NULL); 

      if ((activity < 0) && (errno != EINTR)) {  
      //   printf("coordinator: select error");  
      }  

      if (FD_ISSET(master_socket, &readfds)) {  
         if ((new_socket = accept(master_socket, (struct sockaddr *)&address, (socklen_t*)&addrlen)) < 0) {  
         //   perror("coordinator: accept");  
            exit(EXIT_FAILURE);  
         }  

         //inform user of socket number - used in send and receive commands 
         // printf("New connection, socket fd: %d, IP: %s, port: %d\n", 
         //    new_socket, inet_ntoa(address.sin_addr), ntohs(address.sin_port)); 

         //send new connection next url 
         const char* next_url = urls[url_idx].c_str();
         if(send(new_socket, next_url, strlen(next_url), 0) == 0)  {  
         //   perror("coordinator: send");  
         }  
         //std::cout << "coordinator: sent url: " << next_url << std::endl;
         worker_map[new_socket] = url_idx;
         url_idx++;

         //add new socket to array of sockets 
         for (int i = 0; i < max_clients; i++) {  
            //if position is empty 
            if(client_socket[i] == 0) {  
               client_socket[i] = new_socket;  
               //printf("Adding to list of sockets as %d\n", i);                 
               break;  
            }  
         }  
      }

      //else its some IO operation on some other socket
      for (int i = 0; i < max_clients; i++) {  
         sd = client_socket[i];  
               
         if (FD_ISSET(sd, &readfds)) {  
            //Check if it was for closing , and also read the 
            //incoming message 
            if ((valread = read(sd, buffer, MAXDATASIZE)) == 0) {  
               //Somebody disconnected , get his details and print 
               getpeername(sd, (struct sockaddr*)&address, (socklen_t*)&addrlen);  
               //printf("Host disconnected, IP: %s, port: %d \n", 
               //   inet_ntoa(address.sin_addr) , ntohs(address.sin_port));  
               
               if (worker_map.find(sd) != worker_map.end()) {
                  failed.push(worker_map[sd]);
               }
                     
               //Close the socket and mark as 0 in list for reuse 
               close(sd);  
               client_socket[i] = 0;  
            }  
                  
            //Process the result that came back
            else {  
               //set the string terminating NULL byte on the end 
               //of the data read 
               buffer[valread] = '\0';  
               std::string s(buffer);
               //unsigned long idx = stoul(s.substr(0, s.find(',')));
               unsigned long result = stoul(s.substr(s.find(',') + 1));
               //std::cout << "coordinator: received result: " << result << " for url: " << urls[idx] << std::endl;
               worker_map.erase(sd);
               total += result;
               received++;

               if (url_idx < url_count) {
                  //send new connection next url 
                  const char* next_url = urls[url_idx].c_str();
                  if(send(sd, next_url, strlen(next_url), 0) == 0)  {  
                     //perror("coordinator: send");  
                  }  
                  //std::cout << "coordinator: sent url: " << next_url << std::endl;
                  worker_map[new_socket] = url_idx;
                  url_idx++;
               }
               else if (failed.size() > 0) {
                  const char* next_url = urls[failed.front()].c_str();
                  if(send(sd, next_url, strlen(next_url), 0) == 0)  {  
                     //perror("coordinator: send");  
                  }
                  //std::cout << "coordinator: sent url: " << next_url << std::endl;
                  worker_map[new_socket] = failed.front();
                  failed.pop();
               }
            }  
         }  
      }  
   }
   //close socket
   close(master_socket);
   std::cout << total << std::endl;

   return 0;
}
