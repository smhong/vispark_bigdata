#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <netdb.h> 
#include <time.h>
#include <math.h>
#include <assert.h>
#include <arpa/inet.h> //inet_addr
#include <netdb.h> //hostent
#include <omp.h>

#include <iostream>
#include <fstream>
#include <string>
#include <regex>
#include <sstream>
#include <vector>
#include <iterator>
#include <map>
#include <chrono>
#include <thread>

#define PORT 5959
#define MSGSIZE 1024

void error(const char *msg)
{
    perror(msg);
    exit(0);
}


int main()
{
    //Variable
    int sockfd, newsockfd, portno;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    int n;
 
    //Socket Binding
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("ERROR opening socket");

    bzero((char *) &serv_addr, sizeof(serv_addr));
    portno = PORT;
    serv_addr.sin_family = AF_INET;
    serv_addr.sin_addr.s_addr = INADDR_ANY;
    serv_addr.sin_port = htons(portno);
    if (bind(sockfd, (struct sockaddr *) &serv_addr,
                sizeof(serv_addr)) < 0)
        error("ERROR on binding");
    listen(sockfd,64);

    //printf("Port %d Open \n",portno);

    int datasize = 256*1024*1024;

    char *buffer, *send_buf;
    buffer = new char[datasize];

    int cnt = 0;
    while(true)
    {
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);
        if (newsockfd < 0) 
            error("ERROR on accept");

        int total_recv = 0;
        while(true){
            n = read(newsockfd,buffer+total_recv,MSGSIZE);
            total_recv += n ;
            //if (n < 0) error("ERROR reading from socket");
            //else if (n < MSGSIZE) printf("Reading %d packet \n",n);
            if (n == 0) break;
        }

        printf("RECV %d \n",total_recv);
    }
}
