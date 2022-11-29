#include <iostream>
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

#define MAXDATASIZE 100 // max number of bytes we can get at once 

void *get_in_addr(struct sockaddr *sa) {
    if (sa->sa_family == AF_INET) {
        return &(((struct sockaddr_in*)sa)->sin_addr);
    }

    return &(((struct sockaddr_in6*)sa)->sin6_addr);
}

/// Worker process that receives a list of URLs and reports the result
/// Example:
///    ./worker localhost 4242
/// The worker then contacts the leader process on "localhost" port "4242" for work
int main(int argc, char* argv[]) {
   if (argc != 3) {
      std::cerr << "Usage: " << argv[0] << " <host> <port>" << std::endl;
      return 1;
   }

   // TODO:
   //    1. connect to coordinator specified by host and port
   //       getaddrinfo(), connect(), see: https://beej.us/guide/bgnet/html/#system-calls-or-bust
   //    2. receive work from coordinator
   //       recv(), matching the coordinator's send() work
   //    3. process work
   //       see coordinator.cpp
   //    4. report result
   //       send(), matching the coordinator's recv()
   //    5. repeat

   const char* PORT = argv[2];
   int sockfd;
   long numbytes;  
   char buf[MAXDATASIZE];
   struct addrinfo hints, *servinfo, *p;
   int rv;
   char s[INET6_ADDRSTRLEN];

   memset(&hints, 0, sizeof hints);
   hints.ai_family = AF_UNSPEC;
   hints.ai_socktype = SOCK_STREAM;

   if ((rv = getaddrinfo(argv[1], PORT, &hints, &servinfo)) != 0) {
      fprintf(stderr, "getaddrinfo: %s\n", gai_strerror(rv));
      return 1;
   }

   // loop through all the results and connect to the first we can
   for(p = servinfo; p != NULL; p = p->ai_next) {
      if ((sockfd = socket(p->ai_family, p->ai_socktype,
               p->ai_protocol)) == -1) {
         perror("client: socket");
         continue;
      }

      if (connect(sockfd, p->ai_addr, p->ai_addrlen) == -1) {
         close(sockfd);
         perror("client: connect");
         continue;
      }

      break;
   }

   if (p == NULL) {
      fprintf(stderr, "client: failed to connect\n");
      return 2;
   }

   inet_ntop(p->ai_family, get_in_addr((struct sockaddr *)p->ai_addr),
         s, sizeof s);
   printf("client: connecting to %s\n", s);

   freeaddrinfo(servinfo); // all done with this structure

   if ((numbytes = recv(sockfd, buf, MAXDATASIZE-1, 0)) == -1) {
      perror("recv");
      exit(1);
   }

   buf[numbytes] = '\0';

   printf("client: received '%s'\n",buf);

   close(sockfd);


   return 0;
}
