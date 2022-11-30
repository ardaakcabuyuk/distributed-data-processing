#include "CurlEasyPtr.h"
#include <iostream>
#include <sstream>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <string.h>
#include <netdb.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>

using namespace std::literals;

#define MAXDATASIZE 1024 // max number of bytes we can get at once 
#define MAXCONNATTEMPTS 5

void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

unsigned long get_url_index(char* url) {
   //url format is http://example.org/blah/blah/blah.urlindex.csv
   //so we need to find the last two . and get the number in between

   std::string url_str(url);
   unsigned long url_index = 0;
   unsigned int last_dot = 0;
   unsigned int second_last_dot = 0;

   for (unsigned int i = 0; i < url_str.length(); i++) {
      if (url_str[i] == '.') {
         second_last_dot = last_dot;
         last_dot = i;
      }
   }

   std::string url_index_str = url_str.substr(second_last_dot + 1, last_dot);
   url_index = std::stoul(url_index_str);

   return url_index;
}

/// Worker process that receives a list of URLs and reports the result
/// Example:
///    ./worker localhost 4242
/// The worker then contacts the leader process on "localhost" port "4242" for work
int main(int argc, char* argv[]) {
   //std::cout << "worker: start" << std::endl;
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   unsigned short int PORT = (unsigned short) atoi(argv[2]);
   int sock = 0, client_fd = -1;
   long valread;
   struct sockaddr_in serv_addr;
   char buffer[1024] = { 0 };
   auto curlSetup = CurlGlobalSetup();
   auto curl = CurlEasyPtr::easyInit();

   int retries = 0;
   while (client_fd < 0) {
      if ((sock = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
      //   printf("\n Socket creation error \n");
         return -1;
      }

      serv_addr.sin_family = AF_INET;
      serv_addr.sin_port = htons(PORT);

      // Convert IPv4 and IPv6 addresses from text to binary
      // form
      if (inet_pton(AF_INET, "127.0.0.1", &serv_addr.sin_addr) <= 0) {
      //   printf("\nInvalid address/ Address not supported. \n");
         return -1;
      }

      //std::cout << "worker: waiting for the server" << std::endl; 
      //sleep(2);
   
      if ((client_fd = connect(sock, (struct sockaddr*)&serv_addr, sizeof(serv_addr)))
         < 0) {
      //   printf("\nworker: connection failed. retrying...\n");
         retries++;
         close(sock);
         sleep(1);
      }

      if (retries > MAXCONNATTEMPTS) {
      //   printf("\nworker: connection failed. max retries reached. exiting...\n");
         return -1;
      }
   }

   while(1) {
      if ((valread = recv(sock, buffer, MAXDATASIZE-1, 0)) <= 0) {
         exit(1);
      }

      buffer[valread] = '\0';

      //std::cout << "worker: received: " << buffer << std::endl;
      
      size_t result = 0;
      curl.setUrl(buffer);
      // Download them
      auto csvData = curl.performToStringStream();
      for (std::string row; std::getline(csvData, row, '\n');) {
         auto rowStream = std::stringstream(std::move(row));

         // Check the URL in the second column
         unsigned columnIndex = 0;
         for (std::string column; std::getline(rowStream, column, '\t'); ++columnIndex) {
            // column 0 is id, 1 is URL
            if (columnIndex == 1) {
               // Check if URL is "google.ru"
               auto pos = column.find("://"sv);
               if (pos != std::string::npos) {
                  auto afterProtocol = std::string_view(column).substr(pos + 3);
                  if (afterProtocol.starts_with("google.ru/"))
                     ++result;
               }
               break;
            }
         }
      }

      unsigned long url_index = get_url_index(buffer);
      std::string result_with_index = std::to_string(url_index) + "," + std::to_string(result);

      //send result back to coordinator
      send(sock, result_with_index.c_str(), result_with_index.length(), 0);
      //   perror("send");

      //std::cout << "worker: result: " << result << std::endl;
   }

   close(sock);
   return 0;
}
