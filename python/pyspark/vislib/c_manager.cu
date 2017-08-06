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

#include <cuda.h>
#include <cuda_runtime.h>
#include "builtin_types.h"
#include "device_launch_parameters.h"

#define SOCK_REUSE 0
#define MSGSIZE (1024)
#define CHUNKSIZE (1024*1024*5)
#define PORT 4949

using namespace std;
using namespace chrono;

typedef unsigned int uint;
typedef unsigned char uchar;

template <typename T> class vispark_data; 

typedef pair<string,vispark_data<char>*> vpair;


#define RUN_LOG 1

#if defined(RUN_LOG) && RUN_LOG > 0
    #define log_print(fmt, args...) fprintf(stderr, "[%s] %s():%04d - " fmt, \
        host_name,__func__, __LINE__, ##args)
#else
    #define log_print(fmt, args...) /* Don't do anything in release builds */
#endif

#define NUM_THREADS 2

void omp_memcpy(char *dst, char *src, size_t len)
{
    #pragma omp parallel
    {
        int tid = omp_get_thread_num();
        int num_threads = omp_get_num_threads();

        size_t start = (len/num_threads)*tid;
        size_t end   = (len/num_threads)*(tid+1);

        if (tid == num_threads - 1)  
            end   = len;

        memcpy(dst + start , src + start , end-start);
    } 
} 


void call_memcpy(char *dst, char *src, int start, int end)
{
    memcpy(dst + start , src + start , end-start);
}

void mt_memcpy(char *dst, char *src, size_t len)
{
    auto th1 = thread(call_memcpy,dst,src,len*0.00,len*0.25);
    auto th2 = thread(call_memcpy,dst,src,len*0.25,len*0.50);
    auto th3 = thread(call_memcpy,dst,src,len*0.50,len*0.75);
    auto th4 = thread(call_memcpy,dst,src,len*0.75,len*1.00);

    th1.join();
    th2.join();
    th3.join();
    th4.join();
}

void error(const char *msg)
{
    perror(msg);
    exit(0);
}

void msg_print(vector<string> msg)
{
    for (auto n : msg)
        cout << n << " ";
    cout << endl;
}

vector<string> msg_parser(const char* msg_buffer)
{
    const string s(msg_buffer);
    istringstream ist(s);

    vector<string> tmp,ss;
    copy(istream_iterator<string>(ist), istream_iterator<string>(),
        back_inserter(tmp));

    for (auto n : tmp) {
        if (strcmp(n.c_str(),"END") == 0)
            break;
        ss.push_back(n);
    }
    
    return ss;
}

string RandomString(const char * prefix, int len, int type)
{
//    if (type > 0)
   string str = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
   string newstr(prefix);
   int pos;
   while(newstr.size() != len) {
        pos = ((rand() % (str.size() - 1)));
        newstr += str.substr(pos,1);
   }
   //newstr += '\n';
   return newstr;
}

class msg_create
{
    char *msg;
    size_t msg_size;
    int lenn;

    public:
    msg_create(uint num_msg = 1){

        uint size = (num_msg + 1)*MSGSIZE;
        this->msg_size = sizeof(char)*size;

        msg = new char[size];
        bzero(msg,size);
    }

    void set_head(string s){

        memcpy(msg,s.c_str(),s.size());
    }

    void set_msg(string s){
        memcpy(msg + MSGSIZE, s.c_str(),s.size());
    }

    char *ptr(){
        return msg;
    }
    
    size_t size(){
        return msg_size;
    }

    void print(){
        for (int i =0 ; i < msg_size ; i++)
            printf("%c",msg[i]);
        printf("\n");
    }

};

template <typename T>
class vispark_data 
{
    T* host_ptr = nullptr;
    T* dev_ptr = nullptr;
    size_t malloc_size = 0;
    size_t data_size = 0;
    string data_type = "char";  
    string data_key = "";    
    bool in_mem_flag = false;
    cudaStream_t *stream_list;
    int stream_num;

 
    public:
        vispark_data(uint data_size){
     //       printf("Constructor \n");
            this->malloc_size = data_size*sizeof(T) + MSGSIZE;
            this->data_size = data_size*sizeof(T);
            cudaHostAlloc((void**)&host_ptr, malloc_size, cudaHostAllocDefault); 
            //host_ptr = new T[malloc_size];
            cudaMalloc((void**)&dev_ptr, data_size); 

            int stream_num = data_size % CHUNKSIZE > 0 ? data_size/CHUNKSIZE + 1  : data_size/CHUNKSIZE;
            stream_list = new cudaStream_t[stream_num];

            for (int i = 0 ; i < stream_num ; i++)
                cudaStreamCreate(&(stream_list[i]));
        }

        ~vispark_data()
        {
       //     printf("Distructor \n");
            cudaFreeHost(host_ptr);
            cudaFree(dev_ptr);
        }


        vispark_data(const vispark_data &A)
        {
        //    printf("Copy \n");
/*
            this->malloc_size = A.malloc_size;
            this->data_size = A.data_size;
            cudaHostAlloc((void**)&host_ptr, malloc_size, cudaHostAllocDefault); 
    //        host_ptr = new T[malloc_size];
            cudaMalloc((void**)&dev_ptr, malloc_size); 
            
            memcpy(this->host_ptr,A.host_ptr,malloc_size);
*/
        }


        //void htod(cudaStream_t stream){
        void htod(){
            //cudaMemsetAsync(dev_ptr,0,malloc_size,stream);
            cudaMemcpy(dev_ptr,host_ptr,data_size,cudaMemcpyHostToDevice);
            in_mem_flag = true;
        } 
    
        void dtoh(){
            cudaMemcpy(host_ptr,dev_ptr,data_size,cudaMemcpyDeviceToHost);
            //cudaMemsetAsync(dev_ptr,0,malloc_size,stream);
        }

        vector<cudaStream_t*> dtoh_stream(){
            vector<cudaStream_t*> stream_list;
        
            int offset =0;
            while(offset < data_size){
                cudaStream_t *stream = new cudaStream_t;
                cudaStreamCreate(stream);


                cudaMemcpyAsync(host_ptr + offset,dev_ptr+offset,CHUNKSIZE,cudaMemcpyDeviceToHost,*stream);
               
                offset += CHUNKSIZE;
                stream_list.push_back(stream);
            }
         
            return stream_list;
        }


        size_t getMallocSize(){
            return malloc_size;
        }
        
        uint getDataSize(){
            return data_size;
        }

        T* getHostPtr(){
            return host_ptr;
        }
        
        T* getDevPtr(){
            return dev_ptr;
        }

        void setDataKey(string data_key){
            this->data_key = data_key;
        }

        string getDataKey(){
            return data_key;
        }
        
        bool inGPU(){
            return in_mem_flag;
        }
        
        void setInGPU(bool flag){
            in_mem_flag = flag;
        }           
 
}; 


//   -*-   -*-   -*-

CUcontext context;
CUdevice device;
CUfunction kernelfunc;
CUmodule module;

//   -*-   -*-   -*-

CUresult kernel_call(vispark_data<char>* out_data, vispark_data<char>* in_data, vector< tuple<string,int,char*> >* args) {
    CUdeviceptr devInArr1, devOutArr1;
    CUresult err;

    devInArr1 = (CUdeviceptr) in_data->getDevPtr(); 
    devOutArr1 = (CUdeviceptr) out_data->getDevPtr(); 

    vector<void*> kernelParams;
    kernelParams.push_back(&devOutArr1);
    kernelParams.push_back(&devInArr1);

    for (auto n : *args){

        string type = get<0>(n);
        int    len  = get<1>(n);
        char * data = get<2>(n);

        if (strcmp(type.c_str(),"int") == 0){
            int *data_ptr = (int *) data;
            if (len == 1)  
                kernelParams.push_back(const_cast<int*>(data_ptr));
            else{

                CUdeviceptr* local_arr = new CUdeviceptr;
                cuMemAlloc(local_arr, sizeof(int) * len);
                cuMemcpyHtoD(*local_arr, data_ptr, sizeof(int) * len);
                kernelParams.push_back(local_arr);
            } 
        }

        if (strcmp(type.c_str(),"double") == 0){
            double *data_ptr = (double *) data;
            if (len == 1)  
                kernelParams.push_back(const_cast<double*>(data_ptr));
            else{

                CUdeviceptr* local_arr = new CUdeviceptr;
                cuMemAlloc(local_arr, sizeof(double) * len);
                cuMemcpyHtoD(*local_arr, data_ptr, sizeof(double) * len);
                kernelParams.push_back(local_arr);
            } 
                 
        }

        if (strcmp(type.c_str(),"float") == 0){
            float *data_ptr = (float *) data;
            if (len == 1)  
                kernelParams.push_back(const_cast<float*>(data_ptr));
            else{

                CUdeviceptr* local_arr = new CUdeviceptr;
                cuMemAlloc(local_arr, sizeof(float) * len);
                cuMemcpyHtoD(*local_arr, data_ptr, sizeof(float) * len);
                kernelParams.push_back(local_arr);
            } 
                 
        }

    }
    
    //in_data->htod();
    //auto host_ptr = in_data->getHostPtr();
    //memset(host_ptr,0,512*512*3*sizeof(char));
   /*
     vector<cudaStream_t*> stream_list;
    for (int i = 0 ; i < 16 ; i++){
        cudaStream_t* stream = new cudaStream_t;
        cudaStreamCreate(stream);
        stream_list.push_back(stream);
    } 
*/
//    for (int i = 0 ; i < 16 ; i++){
    err = cuLaunchKernel(kernelfunc, 256,256,1,  16, 16, 1,  0, 0, &kernelParams[0], 0);
    if (err != CUDA_SUCCESS) return err;

    out_data->setInGPU(true);
    //out_data->dtoh();
    //in_data->dtoh();

    return err;
};

//    map<string,vispark_data<char>*> data_dict;
//    map<string,vector<tuple<string,int,char*>>*> args_dict;

int GPU_TEST(const char *ptxfile, const char* func_name, vispark_data<char>* out_data, vispark_data<char>* in_data, vector<tuple<string,int,char*>>* args) {
    CUresult err;
    
    /*
    int deviceCount = 0;


    //err = cuInit(0);
    if (err != CUDA_SUCCESS) { printf("cuInit error... .\n"); return err; }
    err = cuDeviceGetCount(&deviceCount);
    if (err != CUDA_SUCCESS) { printf("cuDeviceGetCount error... .\n"); return err; }
    if (deviceCount == 0) { printf("No CUDA-capable devices... .\n"); return err; } 
    err = cuDeviceGet(&device, 0);
    if (err != CUDA_SUCCESS) { printf("cuDeviceGet error... .\n"); return err; }
    err = cuCtxCreate(&context, 0, device);
    if (err != CUDA_SUCCESS) { printf("cuCtxCreate error... .\n"); return err; }
    */
    err = cuModuleLoad(&module, ptxfile);
    if (err != CUDA_SUCCESS) { printf("cuModuleLoad error... .\n"); return err; }
    err = cuModuleGetFunction(&kernelfunc, module, func_name);
    if (err != CUDA_SUCCESS) { printf("cuModuleGetFunction error... .\n"); return err; }

    err = kernel_call(out_data,in_data,args);
    if (err != CUDA_SUCCESS) { printf("Kernel invocation failed... .\n"); return err; }
  //  for (int i = 0; i < 10; ++i) printf("%d + %d = %d\n", inArr1[i], inArr2[i], outArr1[i]);
    //cuCtxSynchronize();
    //cuCtxDetach(context);
    return 0;
}


vector<string> workers;
int num_workers;
char *host_name;
int w_idx;

int main(int argc, char* argv[])
{
    srand(getpid());

    //omp_set_num_threads(NUM_THREADS);

    //Argument
    if (argc > 3){

        char *slave_name = argv[1];  
        host_name = argv[2];
        char *pid_name = argv[3];    

        string line; 
        ifstream slave_file(slave_name);
        while (getline(slave_file,line))
        {
            line.erase(std::remove(line.begin(), line.end(), ' '), line.end());
            line = line.substr(0,line.find("#"));

            if (line.size() > 0)
                workers.push_back(line);
        }

        num_workers = workers.size();    

        for (int i = 0 ; i < workers.size() ; i++){
            auto n = workers[i];
            if (strcmp(n.c_str(),host_name) == 0){
                w_idx = i;   
                break;
            }
        }

        ofstream pid_file(pid_name,ios::trunc);
                 pid_file << getpid();
                 pid_file.close();
    
        log_print("Launch Process among %d/%d (%d)\n",w_idx,num_workers,getpid());
    }
    
    //Dict  
    map<string,vispark_data<char>*> data_dict;
    map<string,vector<tuple<string,int,char*>>*> args_dict;
    map<string,string> code_dict; 

    //Variable
    int sockfd, newsockfd, portno;
    socklen_t clilen;
    struct sockaddr_in serv_addr, cli_addr;
    int n;
    //int lenn;

    //Timer
    time_point<system_clock> start, end;
    double etime;
    int throughput;

    
    //CUDA
    CUresult err;
    int deviceCount = 0;

    err = cuInit(0);
    if (err != CUDA_SUCCESS) { printf("cuInit error... .\n"); return err; }
    err = cuDeviceGetCount(&deviceCount);
    if (err != CUDA_SUCCESS) { printf("cuDeviceGetCount error... .\n"); return err; }
    if (deviceCount == 0) { printf("No CUDA-capable devices... .\n"); return err; } 
    err = cuDeviceGet(&device, 0);
    if (err != CUDA_SUCCESS) { printf("cuDeviceGet error... .\n"); return err; }
    err = cuCtxCreate(&context, 0, device);
    if (err != CUDA_SUCCESS) { printf("cuCtxCreate error... .\n"); return err; }

    //Socket Binding
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0)
        error("ERROR opening socket");


#if SOCK_REUSE 
    //add reuse
    int enable = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &enable, sizeof(int)) < 0)
    error("setsockopt(SO_REUSEADDR) failed");
#endif

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

   // const char* test = "SEND 8 9 uchar END 00000000000";

   
    char *buffer, *send_buf;
    buffer = new char[MSGSIZE];
    send_buf= new char[MSGSIZE];
    memset(buffer,0,sizeof(char)*MSGSIZE);
    memset(send_buf,0,sizeof(char)*MSGSIZE);


    while(true)
    {
        vector<string> log_msg; 
        clilen = sizeof(cli_addr); 
        newsockfd = accept(sockfd, (struct sockaddr *) &cli_addr, &clilen);

        start = system_clock::now();

        if (newsockfd < 0) 
            error("ERROR on accept");


        string data_key = RandomString("Task_",24,0);
  

        n = read(newsockfd,buffer,MSGSIZE);
        if (n < 0) error("ERROR reading from socket");
       
        auto msg = msg_parser(buffer);
        //msg_print(msg);
        vector<string>::iterator msg_iter= msg.begin();
        int total_lenn = stoi(msg_iter[1]);
        int total_recv = 0;  
        
        //log_print("Start %s (%d) \n",data_key.c_str(),total_lenn);

        char *data_ptr = new char[total_lenn * MSGSIZE];

        while(true){
            n = read(newsockfd,data_ptr+total_recv,MSGSIZE);
            total_recv += n ;
            //if (n < 0) error("ERROR reading from socket");
            //else if (n < MSGSIZE) printf("Reading %d packet \n",n);
            if (n == 0) break;
        }

        
        if (total_lenn*MSGSIZE != total_recv){
            log_print("%d != %d \n",total_lenn*MSGSIZE,total_recv);
        }
        assert(total_lenn*MSGSIZE == total_recv);


        end = system_clock::now();

        duration<double> elapsed = end-start;
        throughput = (total_lenn*MSGSIZE)/(1024*1024);
        etime = elapsed.count();
        log_print("[MSGRECV] %f MB/s (%d / %f)\n",throughput/etime,total_lenn*MSGSIZE,etime);

        //while(false)
        //for (int read_ptr = 0 ; read_ptr < total_recv ; read_ptr += MSGSIZE)
        //start = system_clock::now();
        int read_ptr =0;

        while (read_ptr < total_recv) 
        {
            start = system_clock::now();
           
             memcpy(buffer,data_ptr+read_ptr,sizeof(char)*MSGSIZE);
            read_ptr += MSGSIZE;
            auto msg = msg_parser(buffer);


            vector<string>::iterator iter = msg.begin();
            auto cmd = *iter;
            

            if (strcmp(cmd.c_str(),"SEND")==0){

                int lenn     = stoi(iter[1]);
                int data_len = stoi(iter[2]);
                string data_type = iter[3];

                //cout << lenn<< " " << data_len << " "<<data_type <<endl;

                auto data = new vispark_data<char>(data_len);

                char* dest_ptr = data->getHostPtr();

                memcpy(dest_ptr,data_ptr + read_ptr,lenn*MSGSIZE*sizeof(char));  
                read_ptr += lenn*MSGSIZE;
                /*
                for (int i = 0 ; i < lenn; i++){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                }
                */
                /*
                while (true){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                    if (n == 0) break;
                }   */

                data->htod();
                data_dict.insert(vpair(data_key,data));

            }
            else if (strcmp(cmd.c_str(),"SEQ")==0){

                int lenn     = stoi(iter[1]);
                int data_len = stoi(iter[2]);
                string data_type = iter[3];

            
                vector<string> target_list;

                for (int i = 0 ; i < lenn; i++){
                    memcpy(buffer,data_ptr+read_ptr,sizeof(char)*MSGSIZE);
                    read_ptr += MSGSIZE;
                    auto local_msg= msg_parser(buffer);
                    auto local_iter = local_msg.begin();
                    target_list.push_back(local_iter[1]); 
                } 

                //for ( auto n : target_list)
                //    cout<<n<<endl; 

                data_len = 0;
                
                for ( auto target_key : target_list){
                    auto struct_ptr = data_dict.find(target_key)->second;
                    
                    data_len += struct_ptr->getDataSize();
                }

                auto data = new vispark_data<char>(data_len);
                //char* dest_ptr = data->getHostPtr();
                char* dest_ptr = data->getDevPtr();
                char* source_ptr;

                int copy_off = 0;
                for ( auto target_key : target_list){
                    auto struct_ptr = data_dict.find(target_key)->second;
                    auto source_size = struct_ptr->getDataSize();

                    if (struct_ptr->inGPU() == true){
                        source_ptr = struct_ptr->getDevPtr();
                        cudaMemcpy(dest_ptr + copy_off, source_ptr,source_size*sizeof(char),cudaMemcpyDeviceToDevice);                  
                    }else {
                        source_ptr = struct_ptr->getHostPtr();
                        cudaMemcpy(dest_ptr + copy_off, source_ptr,source_size*sizeof(char),cudaMemcpyHostToDevice);                  
                    }
                    copy_off += source_size;
                }

                string result_key = RandomString("Task_",24,0);
                data ->setDataKey(result_key);
                data -> setInGPU(true); 
                
                data_key = result_key;

                //data->htod();
                data_dict.insert(vpair(result_key,data));

                //memcpy(dest_ptr,data_ptr + read_ptr,lenn*MSGSIZE*sizeof(char));  
                //read_ptr += lenn*MSGSIZE;
                /*
                for (int i = 0 ; i < lenn; i++){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                }
                */
                /*
                while (true){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                    if (n == 0) break;
                }   */



            }
            else if (strcmp(cmd.c_str(),"CHECK")==0){

                int lenn     = stoi(iter[1]);
                int data_len = stoi(iter[2]);
                string data_type = iter[3];

            
                vector<string> target_list;

                for (int i = 0 ; i < lenn; i++){
                    memcpy(buffer,data_ptr+read_ptr,sizeof(char)*MSGSIZE);
                    read_ptr += MSGSIZE;
                    auto local_msg= msg_parser(buffer);
                    auto local_iter = local_msg.begin();
                    target_list.push_back(local_iter[1]); 
                } 

                //cout<<"REQUIRED"<<endl;
                //for ( auto n : target_list)
                //    printf("[%s] REQURIED %s \n",host_name,n.c_str());


                int missing_cnt = 0;
                
                for ( auto target_key : target_list){
                    auto struct_iter = data_dict.find(target_key);
                    if (struct_iter == data_dict.end())
                        missing_cnt ++;
                }
               
                if (missing_cnt == 0)
                    n = write(newsockfd,"CHECKED",7);
                else 
                    n = write(newsockfd,"NOT",3);
                shutdown(newsockfd,SHUT_WR);
                
                //log_print("MISSING CNT %d \n",missing_cnt);

            }
            else if (strcmp(cmd.c_str(),"FILL")==0){

                int lenn     = stoi(iter[1]);
                int data_len = stoi(iter[2]);
                string data_type = iter[3];

            
                vector<string> target_list;

                for (int i = 0 ; i < lenn; i++){
                    memcpy(buffer,data_ptr+read_ptr,sizeof(char)*MSGSIZE);
                    read_ptr += MSGSIZE;
                    auto local_msg= msg_parser(buffer);
                    auto local_iter = local_msg.begin();
                    target_list.push_back(local_iter[1]); 
                } 

                //cout<<"REQUIRED"<<endl;
                //for ( auto n : target_list)
                //    printf("[%s] REQURIED %s \n",host_name,n.c_str());

                vector<string> missing_list;
                
                for ( auto target_key : target_list){
                    auto struct_iter = data_dict.find(target_key);
                    if (struct_iter == data_dict.end())
                        missing_list.push_back(target_key);  
                }

                //cout<<"MISSING"<<endl;
               // for ( auto n : missing_list)
               //     printf("[%s] MISSING %s \n",host_name,n.c_str());

    
                if (missing_list.size() > 0){
 
                    auto send_obj =msg_create();
                    string head = "Start 1 END ";
                    string cont = "REQUEST ";

                    for (auto n : missing_list)
                        cont = cont + n + " ";
        
                    cont += "END ";
           
                    //while (head.size() < MSGSIZE)
                    //    head += '0';
 
                    //while (cont.size() < MSGSIZE)
                     //   cont += '0';

   
                    //string send_obj = head+cont;

                    //cout<<send_obj<<endl;
                    //cout<<send_obj.size()<<endl;
                    send_obj.set_head(head);          
                    send_obj.set_msg(cont);          

                    //send_obj.print();

                    //cout<<send_obj.size()<<endl;
                    
                    auto send_ptr = send_obj.ptr();
                    auto send_len = send_obj.size();

                    for (auto address : workers){
    
                        if (strcmp(address.c_str(),host_name) == 0)
                            continue;

                        struct sockaddr_in other_addr;

                        //Create socket
                        auto send_sock = socket(AF_INET , SOCK_STREAM , 0);
                        bzero((char *) &other_addr, sizeof(other_addr));
                        
                        //setup address structure
                        if(inet_addr(address.c_str()) == -1)
                        {
                            struct hostent *he;
                            struct in_addr **addr_list;

                            //resolve the hostname, its not an ip address
                            if ( (he = gethostbyname( address.c_str() ) ) == NULL)
                            {
                                //gethostbyname failed
                                herror("gethostbyname");
                                //cout<<"Failed to resolve hostname\n";

                                return false;
                            }

                            //Cast the h_addr_list to in_addr , since h_addr_list also has the ip address in long format only
                            addr_list = (struct in_addr **) he->h_addr_list;

                            for(int i = 0; addr_list[i] != NULL; i++)
                            {
                                //strcpy(ip , inet_ntoa(*addr_list[i]) );
                                other_addr.sin_addr = *addr_list[i];

                                //cout<<address<<" resolved to "<<inet_ntoa(*addr_list[i])<<endl;
                                break;
                            }
                        }

                        //plain ip address
                        else
                        {
                            other_addr.sin_addr.s_addr = inet_addr( address.c_str() );
                        }

                        other_addr.sin_family = AF_INET;
                        other_addr.sin_port = htons( portno );
                        
                        //cout<<"TRY TO SEND REQUEST to "<<address<<endl;

                        //Connect to remote server
                        if (connect(send_sock , (struct sockaddr *)&other_addr , sizeof(other_addr)) >= 0)
                        {
                            //log_print("Connected %s and %s \n",host_name,address.c_str());
 
                            for (uint offset = 0 ; offset < send_len; offset += MSGSIZE)
                            {
                                n = write(send_sock,send_ptr + offset,MSGSIZE);
                                //if (n < MSGSIZE) error("ERROR reading from socket 1");
                            }
                            shutdown(send_sock,SHUT_WR);

                        }
                        else
                            ; 
                            //log_print("Fail to Connected %s and %s \n",host_name,address.c_str());
                    }

                }
            }
            else if (strcmp(cmd.c_str(),"RECV")==0){

                //            string data_key = iter[1];
                //cout<<"RECV KEY "<< data_key <<endl;
                auto struct_ptr = data_dict.find(data_key)->second;
                if (struct_ptr->inGPU() == true)
                    struct_ptr->dtoh();
                char* data_ptr = struct_ptr->getHostPtr();
                //uint send_len  = struct_ptr->getMallocSize();
                uint data_len  = struct_ptr->getDataSize();
               // uint lenn     = send_len / MSGSIZE;


                for (uint offset = 0 ; offset < data_len ; offset += MSGSIZE)
                {
                    uint send_size = min(MSGSIZE,data_len-offset);
                    n = write(newsockfd,data_ptr + offset,send_size);
                    //if (n < 0) error("ERROR reading from socket 1");
                    //else if (n < MSGSIZE) printf("Sending %d packet \n",n);
                }
                shutdown(newsockfd,SHUT_WR);

                //cout<<"Finish Task : "<<data_key<<endl; 
            }
            else if (strcmp(cmd.c_str(),"VIEWER")==0){

                //            string data_key = iter[1];
                //cout<<"RECV KEY "<< data_key <<endl;
                string location = iter[1];      
                int    loc_port = stoi(iter[2]);
 
                auto struct_ptr = data_dict.find(data_key)->second;
                if (struct_ptr->inGPU() == true)
                    struct_ptr->dtoh();
                char* data_ptr = struct_ptr->getHostPtr();
                //uint send_len  = struct_ptr->getMallocSize();
                uint data_len  = struct_ptr->getDataSize();
               // uint lenn     = send_len / MSGSIZE;

                struct sockaddr_in other_addr;
                auto send_sock = socket(AF_INET , SOCK_STREAM , 0);
                bzero((char *) &other_addr, sizeof(other_addr));

                other_addr.sin_addr.s_addr = inet_addr("192.168.1.11");
                other_addr.sin_family = AF_INET;
                other_addr.sin_port = htons( loc_port );

                if (connect(send_sock , (struct sockaddr *)&other_addr , sizeof(other_addr)) >= 0)
                {
                    for (uint offset = 0 ; offset < data_len; offset += MSGSIZE){
                        uint send_size = min(MSGSIZE,data_len-offset);
                        n = write(send_sock,data_ptr + offset,send_size);
                    }
                    shutdown(send_sock,SHUT_WR);
                }
                else
                    ; 
    
                n = write(newsockfd,data_key.c_str(),data_key.size());
                shutdown(newsockfd,SHUT_WR);



                //cout<<"Finish Task : "<<data_key<<endl; 
            }

            else if (strcmp(cmd.c_str(),"RUN")==0){

                int lenn1     = stoi(iter[1]);
                int code_len  = stoi(iter[2]);
                int lenn2     = stoi(iter[3]);
                int data_len  = stoi(iter[4]);
                string func_name = iter[5];      
                int result_len = stoi(iter[6]);
                
                //cout<<func_name<<" "<<result_len<<endl;
 
                char* code_ptr;
                char* args_ptr;

                //cout<<lenn1<<" "<<code_len<<endl;
                //cout<<lenn2<<" "<<data_len<<endl;

                code_ptr = new char[(lenn1)*MSGSIZE];
                args_ptr = new char[(lenn2)*MSGSIZE];


                memcpy(code_ptr,data_ptr + read_ptr, lenn1*MSGSIZE);
                read_ptr += lenn1*MSGSIZE; 

                memcpy(args_ptr,data_ptr + read_ptr, lenn2*MSGSIZE);
                read_ptr += lenn2*MSGSIZE; 
                /*
                int total_recv = 0;
                for (int i = 0 ; i < lenn1; i++){
                    n = read(newsockfd,code_ptr+i*MSGSIZE,MSGSIZE);
                    total_recv += n ;
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                    //if (n == 0) break;
                }



                int total_recv1 = 0;
                for (int i = 0 ; i < lenn2; i++){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    total_recv1 += n ;
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                    //if (n == 0) break;
                }

                cout <<total_recv<<" "<<total_recv1<<endl;
                */
                string cuda_code(code_ptr,0,code_len);
                int offset = 0;

                auto args_data = new vector<tuple<string,int,char*>>;

                for (auto arg_iter = iter+7 ; arg_iter != msg.end() ; arg_iter++)
                {
                    string elem_type = *arg_iter;

                    int elem_len = 1;
                    //cout<< elem_type<<endl;               
 
                    if (strcmp(elem_type.c_str(),"int") == 0)
                        elem_len = 4;
                    else if (strcmp(elem_type.c_str(),"double") == 0)
                        elem_len = 8;
                    else if (strcmp(elem_type.c_str(),"float") == 0)
                        elem_len = 4;
                    else
                        continue;

                    int    elem_num  = stoi(*(arg_iter+1));
                    arg_iter++;
                //    cout<<elem_type<<" "<<elem_len << " " <<elem_num<<" " <<offset<<endl;
                    
                    char* data_read = new char[elem_len * elem_num];
                    memcpy(data_read,args_ptr+offset,elem_num*elem_len*sizeof(char));
 
                    //args_data->push_back(make_pair(n.first,string(data_read)));
                    args_data->push_back(make_tuple(elem_type,elem_num,data_read));
                    offset += elem_num*elem_len;
       
                }
       
                args_dict.insert(make_pair(data_key,args_data));


                /***************************************/
                /* CUDA compile */
                /***************************************/
                string filename = RandomString("/tmp/cuda_",16,code_len);
                string cudafile = filename + ".ptx";
                //string ptxfile  = filename + ".ptx";
                //cout<<cudafile<<" "<<cuda_code.size()<<endl;


                ofstream file(cudafile.c_str(),ios::trunc);
                file << cuda_code;
                file.close();

                /*            
                if ( access(ptxfile.c_str(),F_OK) != 0){
                    string command = "nvcc -ptx " + cudafile + " -o " + ptxfile ;
                    ofstream file(cudafile.c_str());
                    file << cuda_code;
                    file.close();
               
                    n = system(command.c_str()); 
                }*/

                /***************************************/
                /* CUDA RUN */
                /***************************************/

                auto result_data = new vispark_data<char>(result_len);
                string result_key = RandomString("Task_",24,0);
                result_data->setDataKey(result_key);
                
                auto data_elem = data_dict.find(data_key)->second;
                auto args_elem = args_dict.find(data_key)->second;

                n = GPU_TEST(cudafile.c_str(),func_name.c_str(),result_data,data_elem,args_elem); 
            
                data_dict.insert(vpair(result_key,result_data));

                data_key = result_key;

            }
            else if (strcmp(cmd.c_str(),"HIT")==0){
                data_key = iter[1];      
                //cout<<"HIT KEY "<< data_key <<endl;
            }
            else if (strcmp(cmd.c_str(),"ACT")==0){

                n = write(newsockfd,data_key.c_str(),data_key.size());
                //if (n < 0) error("ERROR reading from socket 1");
                //else if (n < MSGSIZE) printf("Sending %d packet \n",n);

                shutdown(newsockfd,SHUT_WR);
                //log_print("End %s (%d) \n",data_key.c_str(),n);
                //cout<<"Finish Task : "<<data_key<<endl; 
            }
            else if (strcmp(cmd.c_str(),"REQUEST")==0){

                vector<string> recv_list;

                for (auto n = iter+1; n != msg.end(); n++){

                    auto struct_iter = data_dict.find(*n);
                    if (struct_iter != data_dict.end())
                        recv_list.push_back(*n);
                }

                //cout<<inet_ntoa(cli_addr.sin_addr)<<endl;
                //cout<<cli_addr.sin_addr.s_addr<<endl;

                //for ( auto n : recv_list)
                //    printf("[%s] REQUESTED %s \n",host_name,n.c_str());
                
               

                for ( auto n : recv_list)
                {
                    auto struct_ptr = data_dict.find(n)->second;
                    //struct_ptr->dtoh();
                    //auto stream_iter = struct_ptr->dtoh_stream().begin();
                    auto stream_list = struct_ptr->dtoh_stream();
                    auto stream_iter = stream_list.begin();
                    //auto stream_end = struct_ptr->dtoh_stream().end();
                    int  proc_size = 0;
//                    cudaDeviceSynchronize();
                   
                    /* 
                    for (auto stream_iter : struct_ptr->dtoh_stream()){
                        auto err =  cudaStreamSynchronize(*(stream_iter));
                        if (err != CUDA_SUCCESS) { log_print("Stream error... .\n"); }
                        proc_size += CHUNKSIZE;
                        log_print("PROCESSED %d \n",proc_size);
                    }
                    */

                    //log_print("NUM STREAM %d \n",stream_list.size());
                    int host_len = struct_ptr->getDataSize();
                    char* host_ptr = struct_ptr->getHostPtr();
                    int lenn =  host_len%MSGSIZE == 0 ? host_len/MSGSIZE : host_len/MSGSIZE + 1;

//                    log_print("Send %d/%d data \n",lenn,host_len);

                    auto send_obj =msg_create();
                    string head = "Start "+ to_string(lenn+1) + " END";
                    string cont = "Transfer " + n + " " + to_string(lenn) + " " 
                                +  to_string(host_len) + " END";

                    send_obj.set_head(head);          
                    send_obj.set_msg(cont);         
 
                    struct sockaddr_in other_addr;
                    auto send_sock = socket(AF_INET , SOCK_STREAM , 0);
                    bzero((char *) &other_addr, sizeof(other_addr));
                    
                    other_addr.sin_addr.s_addr = cli_addr.sin_addr.s_addr;
                    other_addr.sin_family = AF_INET;
                    other_addr.sin_port = htons( portno );

                    if (connect(send_sock , (struct sockaddr *)&other_addr , sizeof(other_addr)) >= 0)
                    {
                        char *send_ptr = send_obj.ptr();
                        for (uint offset = 0 ; offset < 2*MSGSIZE; offset += MSGSIZE)
                            n = write(send_sock,send_ptr + offset,MSGSIZE);

                        for (uint offset = 0 ; offset < lenn*MSGSIZE; offset += MSGSIZE){
                            if (offset >= proc_size){
                                auto err =  cudaStreamSynchronize(*(*stream_iter));
                                if (err != CUDA_SUCCESS) { log_print("Stream error...[%d]\n",err); }
                                proc_size += CHUNKSIZE;
                                //log_print("PROCESSED %d \n",proc_size);
                                stream_iter++;
                            }
                            n = write(send_sock,host_ptr + offset,MSGSIZE);
                        }
                        shutdown(send_sock,SHUT_WR);
                    }
                    else
                        ; 
                       // log_print("Fail to Connected  \n");
                }

            }
            else if (strcmp(cmd.c_str(),"Transfer")==0){

                string recv_key = iter[1];
                int lenn     = stoi(iter[2]);
                int data_len = stoi(iter[3]);
                //string data_type = iter[3];

                //cout << lenn<< " " << data_len << " "<<data_type <<endl;

                auto data = new vispark_data<char>(data_len);

                char* dest_ptr = data->getHostPtr();

                //memcpy(dest_ptr,data_ptr + read_ptr,lenn*MSGSIZE*sizeof(char));  
                //adv_memcpy(dest_ptr,data_ptr + read_ptr,lenn*MSGSIZE*sizeof(char));  
                mt_memcpy(dest_ptr,data_ptr + read_ptr,lenn*MSGSIZE*sizeof(char));  
                //memmove(dest_ptr,data_ptr + read_ptr,lenn*MSGSIZE*sizeof(char));  
                read_ptr += lenn*MSGSIZE;

                //log_print("GET %s (%d) \n",recv_key.c_str(),lenn);
                /*
                for (int i = 0 ; i < lenn; i++){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                }
                */
                /*
                while (true){
                    n = read(newsockfd,data_ptr+i*MSGSIZE,MSGSIZE);
                    if (n < 0) error("ERROR reading from socket");
                    else if (n < MSGSIZE) printf("Reading %d packet \n",n);
                    if (n == 0) break;
                }   */


                data_dict.insert(vpair(recv_key,data));

            }
            else {
               log_print("ERROR %s \n",cmd.c_str()); 
            //    break;
            }

//            fflush(stdout); 
 //           cout.flush();
            //log_print("[%s] Recv : %f MB/s (%d / %f)\n",cmd.c_str(),throughput/etime,total_lenn*MSGSIZE,etime);
            end = system_clock::now();
            elapsed = end-start;
            etime = elapsed.count();
            log_print("[%s] %f MB/s (%d / %f)\n",cmd.c_str(),throughput/etime,total_lenn*MSGSIZE,etime);
        }
    
        //log_print("EXEC Bandwidth : %f MB/s (%d / %f)\n",throughput/etime,total_lenn*MSGSIZE,etime);

                /*  
        GetTimeDiff(0);
        //Data transfer
        for (int i = 0 ; i < lenn ;i++){
            int offset = i*MSGSIZE;

            n = read(newsockfd,buffer + offset,MSGSIZE);
            if (n < 0) error("ERROR reading from socket");
        } 
        n = write(newsockfd,"I got your message",18);
        if (n < 0) error("ERROR writing to socket");

        clock_gettime(CLOCK_REALTIME, &spec);
        s  = spec.tv_sec;
        ms = round(spec.tv_nsec / 1.0e6); // Convert nanoseconds to milliseconds

        printf("%ld.%03ld\n", (intmax_t)s, ms);
        */
    }
    return 0;

}

