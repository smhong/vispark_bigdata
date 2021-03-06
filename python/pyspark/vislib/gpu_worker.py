"""
Vispark GPU Worker
"""
import os
import sys
import time
import socket
import traceback
import cProfile
import pstats
#from pyspark.vislib.package import VisparkMeta
from package import VisparkMeta

# vispark socket
from socket import *

import string
import random

from worker_assist import *
import pycuda.driver as cuda
from pycuda.compiler import SourceModule

_id_len = 24
data_id_prefix = "GPU_"
tag_id_prefix = "TAG_"

msg_size =512

SPARK_HOME=os.environ["SPARK_HOME"]
PYSPARK_PATH = "%s/python/"%SPARK_HOME

def read_conf():
    CUDA_ARCH=''
    logging=False

    with open("%s/conf/gpu_conf"%SPARK_HOME,'r') as fp:
        for line in fp:
            if line.find("#") != -1:
                continue
            if line.find("CUDA_ARCH") != -1:
                CUDA_ARCH=line[10:-1]
            if line.find("LOG") != -1:
                if line[4:-1] == "console":
                    logging=True 

    return CUDA_ARCH, logging

CUDA_ARCH, logging=read_conf()

class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

#host_name = gethostname()

global_mod=None
global_kernel_code=""


def print_green(source):
    if logging:
        print bcolors.OKGREEN,source, host_name,  bcolors.ENDC

def print_red(source):
    if logging:
        print bcolors.FAIL,source, host_name, bcolors.ENDC

def print_blue(source):
    if logging:
        print bcolors.OKBLUE,source, host_name, bcolors.ENDC

def print_bblue(source):
    if logging:
        print bcolors.BOLD, bcolors.OKBLUE,source, host_name, bcolors.ENDC

def print_yellow(source):
    if logging:
        print bcolors.WARNING,source, host_name, bcolors.ENDC

def print_purple(source):
    if logging:
        print bcolors.HEADER,source, host_name, bcolors.ENDC

def print_bold(source):
    if logging:
        print bcolors.BOLD,source, host_name, bcolors.ENDC

def print_bred(source):
    if logging:
        print bcolors.BOLD, bcolors.FAIL, source, host_name, bcolors.ENDC

def check_device_mem():
    free, total = cuda.mem_get_info()
    return free 

def print_time(msg="",addr="local"):
    print "[%s] %s : %f"%(addr,msg,time.time())

def id_generator(prefix=data_id_prefix,size=_id_len,chars=string.ascii_letters+string.digits):
    genKey= "".join(random.choice(chars) for _ in range(size))
    return prefix+genKey



def id_check(data):
    try :   
        if data[:len(data_id_prefix)] == data_id_prefix:
            return data[:_id_len+len(data_id_prefix)],True
    except :
        pass

    data_id = id_generator()
    return data_id,False

def get_tag_code(msg):
    return msg[:len(tag_id_prefix)+_id_len]

def send_signal_socket(signal,tag,address,requester,port=3939,reply=False):
    addr = gethostbyname(address)
    
    clisock = socket(AF_INET, SOCK_STREAM)
    clisock.connect((addr,port))
 
    send_str='%s**%s**%s**'%(signal,tag,requester)
    send_str+='0'*msg_size
    send_str=send_str[:msg_size]

    clisock.send(send_str) 

    if reply :
        msg = clisock.recv(msg_size)
        reply = msg[:msg.find('**')]
 
    return reply


def send_terminal(key):
    address = "/tmp/shuffle_ack"
    clisock = socket(AF_UNIX, SOCK_STREAM)
    clisock.connect(address)

    sending_str = "%s*"%(str(key))
    sending_str += '0'*msg_size

    clisock.send(sending_str[:msg_size])


def send_signal_new(signal,data_id,address,etc=[],port=int(4949),reply=True):
    #clisock = socket(AF_INET, SOCK_STREAM)
    #clisock.connect((address, port))
    
    #clisock = socket(AF_UNIX, SOCK_STREAM)
    #clisock.connect(address)
    #host_name = gethostname()
    clisock = socket(AF_INET, SOCK_STREAM)
    clisock.connect((address,4949))

    sending_str = "%s**%s**"%(str(data_id),signal)

    for elem in etc:
        sending_str += "%s**"%(str(elem))
        
    sending_str += '0'*msg_size

    clisock.send(sending_str[:msg_size])

    if reply: 
        msg = clisock.recv(msg_size)
    #reply = msg[:msg.find('**')]
 
    #
      
    return True


def send_signal(signal,data_id,etc=[],address='127.0.0.1',port=int(3939),reply=True):
    #clisock = socket(AF_INET, SOCK_STREAM)
    #clisock.connect((address, port))
    
    address = "/tmp/gpu_manager"
    #clisock = socket(AF_UNIX, SOCK_STREAM)
    #clisock.connect(address)
    host_name = gethostname()
    clisock = socket(AF_INET, SOCK_STREAM)
    clisock.connect((host_name,4949))

    sending_str = "%s**%s**"%(str(data_id),signal)

    for elem in etc:
        sending_str += "%s**"%(str(elem))
        
    sending_str += '0'*msg_size

    clisock.send(sending_str[:msg_size])

    if reply: 
        msg = clisock.recv(msg_size)
        reply = msg[:msg.find('**')]
 
    return reply
 
def recv_halo(data_id,data_str,address='127.0.0.1',port=int(3939)):
    #result = send_signal("run",data_id,address,port)

    sending_str = "%s**%s**"%(str(data_id),"append")
    sending_str += '0'*msg_size
    #clisock.send(sending_str[:msg_size])
    data_str+=sending_str[:msg_size]

    return (data_id,data_str)

  

def run_gpu(data_id,data_str,args_list,numiter = 1, address='127.0.0.1',port=int(3939)):
    #result = send_signal("run",data_id,address,port)
    #print_bblue("Run : %s"%(data_id))

    if args_list is not None:
        import cPickle as pickle
        args_str = pickle.dumps((data_id,args_list),-1)

        args_len=len(args_str)
        args_str += '0'*msg_size
        lenn = len(args_str)/msg_size
    else :
        args_len = 1
        args_str = ''
        lenn   = 0

    for i in range(numiter):
        sending_str = "%s**%s**%s**%s**"%(str(data_id),"run",str(args_len),str(lenn))
        sending_str += '0'*msg_size
        #clisock.send(sending_str[:msg_size])
        data_str+=sending_str[:msg_size]

        for sent_num in range(lenn):
            data_str+=args_str[sent_num*msg_size:(sent_num+1)*msg_size]

    return (data_id,data_str)

 
def send_count(data_id,address='127.0.0.1',port=int(3939)):
    result = send_signal("count",data_id,address,port)

def drop_cuda(address='127.0.0.1',port=int(3939)):
    try: 
        address = "/tmp/gpu_manager"
        clisock = socket(AF_UNIX, SOCK_STREAM)
        clisock.connect(address)

        sending_str = "%s**%s**"%('drop','drop')
        sending_str += '0'*msg_size
        clisock.send(sending_str[:msg_size])
    except:
        pass
 
def shuffle_halo(data_id,address='127.0.0.1',port=int(3939)):
    result = send_signal("shuffle",data_id,address,port)

def clear_devicemem(data_id,address='127.0.0.1',port=int(3939)):
    result = send_signal("clear",data_id,address,port)



def send_halo(data_id,data_str,indata_meta,address='127.0.0.1',port=int(3939)):
#    result = send_signal("halo",data_id,address,port)
    retry_flag = True
    #while retry_flag:
    if retry_flag:
        #try:
        if True:
            #clisock = socket(AF_INET, SOCK_STREAM)
            #clisock.connect((address, int(port)))

            print_bold("CPU send data to GPU worker %s"%data_id)
            address = "/tmp/gpu_manager"
            clisock = socket(AF_UNIX, SOCK_STREAM)
            clisock.connect(address)
            
              
            #print "Recv Ready"
            #data_len=len(data_str)
            #data_str += '0'*msg_size
            #lenn = len(data_str)/msg_size

            import cPickle as pickle
            args_str = pickle.dumps(indata_meta,-1)

            args_len=len(args_str)
            args_str += '0'*msg_size
            args_lenn = len(args_str)/msg_size

            msg_tag="halo"
            sending_str = "%s**%s**%s**%s**"%(str(data_id),msg_tag,str(args_len),str(args_lenn))
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str += sending_str[:msg_size]
                
            print_bold(sending_str[:100])


            #for elem in range(lenn):
            #    data_str += clisock.recv(msg_size)
            #        recv_num += 1
            #print data_id, data_str[:30]
            lenn = len(data_str)/msg_size
            sent_num = 0
            while sent_num < lenn:
                sent_flag = clisock.send(data_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1

            #clisock.close()

            #return None 
            sent_num = 0
            while sent_num < args_lenn:
                sent_flag = clisock.send(args_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1


            #msg = clisock.recv(msg_size)
            #reply = msg[:msg.find('**')]
            #msg = msg[msg.find('**')+2:]
 
            #return data_id,''

            #print "Recv ACK" , data_id, reply
        else :
        #except:
            print "Error in halo data"
            pass

    return (data_id,'')


#def send_data(data_id,data_str,args_list=None,address='127.0.0.1',port=int(3939)):
def send_data(data_id,data_array,halo=0,address='127.0.0.1',port=int(3939)):

    #print_bblue("Send : %s [%d]"%(data_id,len(data_array)))
    
    #print data_array

    shape_dict={}
    shape_dict['indata_shape'] = data_array.shape
    shape_dict['indata_type']  = data_array.dtype
    shape_dict['data_halo']    = halo

    send_str =''
    if True:
        import cPickle as pickle
        args_str = pickle.dumps((data_id,shape_dict),-1)

        args_len=len(args_str)
        args_str += '0'*msg_size
        lenn1 = len(args_str)/msg_size
    else :
        args_len = 1
        args_str = ''
        lenn1   = 0
            

    print_time("sendGPU_1")
    data_str = data_array.tostring()
    
    print_time("sendGPU_2")
 
    data_len=len(data_str)
    data_str += '0'*msg_size
    lenn2 = len(data_str)/msg_size
    

    msg_tag="send"
    sending_str = "%s**%s**%s**%s**%s**%s**"%(str(data_id),msg_tag,str(args_len),str(lenn1),str(data_len),str(lenn2))
    sending_str += '0'*msg_size
            
    send_str += sending_str[:msg_size]
    
    #for sent_num in range(lenn1):
    #    send_str += args_str[sent_num*msg_size:(sent_num+1)*msg_size]
    send_str += args_str[:lenn1*msg_size]
    send_str += data_str[:lenn2*msg_size]
    
    print_time("sendGPU_3")

    #for sent_num in range(lenn2):
    #    send_str += data_str[sent_num*msg_size:(sent_num+1)*msg_size]


    return (data_id,send_str)


def send_request(data_id,data_array,halo=0,address='127.0.0.1',port=int(3939)):
    import time
    #print_bblue("Send Seq: %s [%d]"%(data_id,len(data_array)))

    host_name = gethostname()
    clisock = socket(AF_INET, SOCK_STREAM)
    clisock.connect((host_name,4949))

    send_str=''
    data_str = ''

    #for i in range(len(data_array)):
    for elem in data_array:
        data_str += elem
 

    data_len=len(data_str)
    data_str += '0'*msg_size
    lenn1 = len(data_str)/msg_size
    num_elems = len(data_array)

    
    msg_tag="ready"
    sending_str = "%s**%s**%s**%s**%s**"%(str(data_id),msg_tag,str(data_len),str(lenn1),str(num_elems))
    sending_str += '0'*msg_size
            
    send_str += sending_str[:msg_size]
    send_str += data_str[:lenn1*msg_size]

    lenn = len(send_str)/msg_size

    temp_time = time.time()
    sent_num = 0
    while sent_num < lenn:
        sent_flag = clisock.send(send_str[sent_num*msg_size:(sent_num+1)*msg_size])
        if sent_flag == 0:
            raise RuntimeError("Run connection broken")
        sent_num += 1
    bandwidth = (sent_num)*msg_size / (time.time() - temp_time) / (1048576.0)
 
    return True


def send_data_seq2(data_id,data_array,halo=0,address='127.0.0.1',port=int(3939)):
    import time
    #print_bblue("Send Seq: %s [%d]"%(data_id,len(data_array)))


    send_str=''
    data_str = ''

    #for i in range(len(data_array)):
    for elem in data_array:
        data_str += elem
 

    data_len=len(data_str)
    data_str += '0'*msg_size
    lenn1 = len(data_str)/msg_size
    num_elems = len(data_array)

    
    msg_tag="send_seq2"
    sending_str = "%s**%s**%s**%s**%s**"%(str(data_id),msg_tag,str(data_len),str(lenn1),str(num_elems))
    sending_str += '0'*msg_size
            
    send_str += sending_str[:msg_size]
    send_str += data_str[:lenn1*msg_size]


    return (data_id,send_str)




def send_data_seq(data_id,data_array,halo=0,address='127.0.0.1',port=int(3939)):

    import time
    #print_bblue("Send Seq: %s [%d]"%(data_id,len(data_array)))
    print data_array

    shape_dict={}
    shape_dict['indata_shape'] = data_array[0].shape
    shape_dict['indata_type']  = data_array[0].dtype
    shape_dict['indata_num']   = len(data_array)
    shape_dict['data_halo']    = halo

    send_str =''
    data_str = ''
    if True:
        import cPickle as pickle
        args_str = pickle.dumps((data_id,shape_dict),-1)

        args_len=len(args_str)
        args_str += '0'*msg_size
        lenn1 = len(args_str)/msg_size
    else :
        args_len = 1
        args_str = ''
        lenn1   = 0

    #128 MB 0.3s

    #tmp = []
    #for elem in data_array:
    #    tmp.append(elem)
    #data_str = numpy.array(tmp).tostring()
    #print len(tmp), len(data_array), data_array[0].shape

    for i in range(len(data_array)):
        data_str += data_array[i].tostring()
 

    data_len=len(data_str)
    data_str += '0'*msg_size
    lenn2 = len(data_str)/msg_size

    msg_tag="send_seq"
    sending_str = "%s**%s**%s**%s**%s**%s**"%(str(data_id),msg_tag,str(args_len),str(lenn1),str(data_len),str(lenn2))
    sending_str += '0'*msg_size
            
    send_str += sending_str[:msg_size]
    
    
    #for sent_num in range(lenn1):
    #    send_str += args_str[sent_num*msg_size:(sent_num+1)*msg_size]
    send_str += args_str[:lenn1*msg_size]
    
    #128 MB 1.5s

    send_str += data_str[:lenn2*msg_size]
    #for sent_num in range(lenn2):
    #    send_str += data_str[sent_num*msg_size:(sent_num+1)*msg_size]
    


    return (data_id,send_str)



def get_cache(data_id,address='127.0.0.1',port=int(3939)):
    retry_flag = True
    #while retry_flag:
    if retry_flag:
        #try:
        if True:
            #clisock = socket(AF_INET, SOCK_STREAM)
            #clisock.connect((address, int(port)))
            #print "CPU send data to GPU worker ", data_id
            #address = "/tmp/gpu_manager"
            #clisock = socket(AF_UNIX, SOCK_STREAM)
            #clisock.connect(address)
            
            #print_bblue("Get Cache : %s"%(data_id))
            #lenn = len(data_str)/msg_size
    
    
 
            msg_tag="hit"
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str = sending_str[:msg_size]
            
           
            return (data_id,data_str)
 

def gpu_persist(data_id,data_str,address='127.0.0.1',port=int(3939)):
    retry_flag = True
    #while retry_flag:
    if retry_flag:
        #try:
        if True:
            #clisock = socket(AF_INET, SOCK_STREAM)
            #clisock.connect((address, int(port)))
            #print "CPU send data to GPU worker ", data_id
            address = "/tmp/gpu_manager"
            #clisock = socket(AF_UNIX, SOCK_STREAM)
            #clisock.connect(address)
            host_name = gethostname()
            clisock = socket(AF_INET, SOCK_STREAM)
            clisock.connect((host_name,4949))
       
            
            #print_bblue("Persist : %s"%(data_id))
            #lenn = len(data_str)/msg_size
    
   
 
            msg_tag="persist"
            #sending_str = "%s**%s**"%(msg_tag,str(data_id))
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str += sending_str[:msg_size]
          
            lenn = len(data_str)/msg_size
            

            #for elem in range(lenn):
            #    data_str += clisock.recv(msg_size)
            #        recv_num += 1

            #print data_id, data_str[:30]


            sent_num = 0
            while sent_num < lenn:
                sent_flag = clisock.send(data_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1
            

            #msg = clisock.recv(msg_size)
            
            #print_time("persistGPU_5")
 
 
            return (data_id,'')



  
def shuffleGPU(data_str,address='127.0.0.1',port=int(3939)):

    data_id,InGPU = id_check(data_str) 

    if InGPU == True:
        if True:

            #clisock = socket(AF_INET, SOCK_STREAM)
            #clisock.connect((address, int(port)))
            #print "CPU send data to GPU worker ", data_id
            address = "/tmp/gpu_manager"
            #clisock = socket(AF_UNIX, SOCK_STREAM)
            #clisock.connect(address)
            host_name = gethostname()
            clisock = socket(AF_INET, SOCK_STREAM)
            clisock.connect((host_name,4949))


            #data_id = data 
 
            #print_bblue("Recv : %s"%(data_id))
                
            #print "Recv Ready"
            #data_len=len(data_str)
            #data_str += '0'*msg_size
            #lenn = len(data_str)/msg_size

            msg_tag="shuffleGPU"
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str += sending_str[:msg_size]

            lenn = len(data_str)/msg_size
            

            #for elem in range(lenn):
            #    data_str += clisock.recv(msg_size)
            #        recv_num += 1

            #print data_id, data_str[:30]
             
            temp_time = time.time()
            sent_num = 0
            while sent_num < lenn:
                sent_flag = clisock.send(data_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1
            bandwidth = (sent_num)*msg_size / (time.time() - temp_time) / (1048576.0)
            #print_bold( "Worker bandwidth for (%s,%s) : %f"%("send",data_id,bandwidth))


            print_time("recvGPU_1")

 
            #print "Recv Meta" , sending_str[:100]
 
            #1.2 sec                   
            msg = clisock.recv(msg_size)
            #reply = msg[:msg.find('**')]
            #msg = msg[msg.find('**')+2:]
            
            print_time("recvGPU_2")
           
            return msg 
        else :
        #except:
            print "Error in recv data"
            pass
    else :
        return data_str


def action_data(data_str,address='127.0.0.1',port=int(3939)):

    data_id,InGPU = id_check(data_str) 

    if InGPU == True:
        if True:

            address = "/tmp/gpu_manager"
            #clisock = socket(AF_UNIX, SOCK_STREAM)
            #clisock.connect(address)
            host_name = gethostname()
            clisock = socket(AF_INET, SOCK_STREAM)
            clisock.connect((host_name,4949))


            msg_tag="action"
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str += sending_str[:msg_size]

            lenn = len(data_str)/msg_size
            
            temp_time = time.time()
            sent_num = 0
            while sent_num < lenn:
                sent_flag = clisock.send(data_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1
            bandwidth = (sent_num)*msg_size / (time.time() - temp_time) / (1048576.0)
            #print_bold( "Worker bandwidth for (%s,%s) : %f"%("send",data_id,bandwidth))


            print_time("actionGPU_1")

 
            #1.2 sec                   
            msg = clisock.recv(msg_size)
            reply = msg[:msg.find('**')]
            msg = msg[:msg.find('**')+2]
            
            print_time("actionGPU_2")
            
            #print reply
            if reply == 'absent':
                return None
            else:
                return msg

def sock_recv(clisock,lenn):
   
    #import StringIO 
    #str_buf = StringIO.StringIO()
    
    import cStringIO 
    str_buf = cStringIO.StringIO()

    for i in xrange(lenn):
        str_buf.write(clisock.recv(msg_size)) 

    return str_buf.getvalue()


def sock_recv1(clisock,lenn):
    
    str_buf = []
    #i = 0
    #while i < lenn:
    for i in xrange(lenn):
        str_buf.append(clisock.recv(msg_size)) 
        #i += 1

    return "".join(str_buf)

  
def recv_data_new(data_str,address='127.0.0.1',port=int(3939)):

    data_id,InGPU = id_check(data_str) 

    if InGPU == True:
        if True:

            #clisock = socket(AF_INET, SOCK_STREAM)
            #clisock.connect((address, int(port)))
            #print "CPU send data to GPU worker ", data_id
            address = "/tmp/gpu_manager"
            #clisock = socket(AF_UNIX, SOCK_STREAM)
            #clisock.connect(address)
            host_name = gethostname()
            clisock = socket(AF_INET, SOCK_STREAM)
            clisock.connect((host_name,4949))


            #data_id = data 
 
            #print_bblue("Recv : %s"%(data_id))
                
            #print "Recv Ready"
            #data_len=len(data_str)
            #data_str += '0'*msg_size
            #lenn = len(data_str)/msg_size

            msg_tag="recv"
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str += sending_str[:msg_size]

            lenn = len(data_str)/msg_size
            

            #for elem in range(lenn):
            #    data_str += clisock.recv(msg_size)
            #        recv_num += 1

            #print data_id, data_str[:30]
             
            temp_time = time.time()
            sent_num = 0
            while sent_num < lenn:
                sent_flag = clisock.send(data_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1
            bandwidth = (sent_num)*msg_size / (time.time() - temp_time) / (1048576.0)
            #print_bold( "Worker bandwidth for (%s,%s) : %f"%("send",data_id,bandwidth))


            print_time("recvGPU_1")

 
            #print "Recv Meta" , sending_str[:100]
 
            #1.2 sec                   
            msg = clisock.recv(msg_size)
            reply = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]
            
            print_time("recvGPU_2")
            
            #print reply
            if reply == 'absent':
                retry_flag = False
                #print "Data Absent" 
                #return reply, None
                return None
            elif reply == 'exist':
                #print "Data Exist" 

                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
                #msg = msg[msg.find('**')+2:]
            
                #print "Recv num %d"%(lenn)
                #print "Recv size %d"%(data_len)
 

                #args_str = ''
 
                temp_time = time.time()
                recv_num = 0
                #while recv_num < lenn1:
                #    args_str += clisock.recv(msg_size)
                args_str = sock_recv(clisock,lenn1)
                
                    #recv_flag = data_str += clisock.recv(msg_size)
                    #if recv_flag == 0:
                    #    raise RuntimeError("Recv connection broken")
                    #recv_num += 1
              
                data_str = sock_recv(clisock,lenn2)
                #data_str = ''
                #for elem in range(lenn):
                recv_num = 0
                #while recv_num < lenn2:
                #    data_str += clisock.recv(msg_size)
                    #recv_flag = data_str += clisock.recv(msg_size)
                    #if recv_flag == 0:
                    #    raise RuntimeError("Recv connection broken")
                #    recv_num += 1
 
                bandwidth = (recv_num)*msg_size / (time.time() - temp_time) / (1048576.0)
                #print_bold( "Worker bandwidth for (%s,%s) : %f"%("recv",data_id,bandwidth))



                #clisock.close()
                data_str = data_str[:data_len]
                import cPickle as pickle
                shape_dict= pickle.loads(args_str)
                data_shape = shape_dict['outdata_shape']
                data_type  = shape_dict['outdata_type']
 
                import numpy
                data_array = numpy.fromstring(data_str,dtype=data_type).reshape(data_shape)
                

                #print data_array.shape , data_shape, data_type

                retry_flag = False
                #return 'success', data_str 

                return data_array

        else :
        #except:
            print "Error in recv data"
            pass
    else :
        return data_str

def recv_data(data_id,data_str,address='127.0.0.1',port=int(3939)):
    retry_flag = True
    #while retry_flag:
    if retry_flag:
        #try:
        if True:

            #clisock = socket(AF_INET, SOCK_STREAM)
            #clisock.connect((address, int(port)))
            #print "CPU send data to GPU worker ", data_id
            address = "/tmp/gpu_manager"
            #clisock = socket(AF_UNIX, SOCK_STREAM)
            #clisock.connect(address)
            host_name = gethostname()
            clisock = socket(AF_INET, SOCK_STREAM)
            clisock.connect((host_name,4949))

            #print_bblue("Recv : %s"%(data_id))
                
            #print "Recv Ready"
            #data_len=len(data_str)
            #data_str += '0'*msg_size
            #lenn = len(data_str)/msg_size

            msg_tag="recv"
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
            #clisock.send(sending_str[:msg_size])
            data_str += sending_str[:msg_size]

            lenn = len(data_str)/msg_size

            #for elem in range(lenn):
            #    data_str += clisock.recv(msg_size)
            #        recv_num += 1

            #print data_id, data_str[:30]
             
            temp_time = time.time()
            sent_num = 0
            while sent_num < lenn:
                sent_flag = clisock.send(data_str[sent_num*msg_size:(sent_num+1)*msg_size])
                if sent_flag == 0:
                    raise RuntimeError("Run connection broken")
                sent_num += 1
            bandwidth = (sent_num)*msg_size / (time.time() - temp_time) / (1048576.0)
            print_bold( "Worker bandwidth for (%s,%s) : %f"%("send",data_id,bandwidth))

 
            #print "Recv Meta" , sending_str[:100]
                    
            msg = clisock.recv(msg_size)
            reply = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]
            
            #print reply
            if reply == 'absent':
                retry_flag = False
                #print "Data Absent" 
                #return reply, None
                return None
            elif reply == 'exist':
                #print "Data Exist" 

                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
                #msg = msg[msg.find('**')+2:]
            
                #print "Recv num %d"%(lenn)
                #print "Recv size %d"%(data_len)
 

                args_str = ''
 
                temp_time = time.time()
                recv_num = 0
                while recv_num < lenn1:
                    args_str += clisock.recv(msg_size)
                    #recv_flag = data_str += clisock.recv(msg_size)
                    #if recv_flag == 0:
                    #    raise RuntimeError("Recv connection broken")
                    recv_num += 1
              
                data_str = ''
                #for elem in range(lenn):
                recv_num = 0
                while recv_num < lenn2:
                    data_str += clisock.recv(msg_size)
                    #recv_flag = data_str += clisock.recv(msg_size)
                    #if recv_flag == 0:
                    #    raise RuntimeError("Recv connection broken")
                    recv_num += 1
 
                bandwidth = (recv_num)*msg_size / (time.time() - temp_time) / (1048576.0)
                print_bold( "Worker bandwidth for (%s,%s) : %f"%("recv",data_id,bandwidth))



                #clisock.close()
                data_str = data_str[:data_len]
                import cPickle as pickle
                shape_dict= pickle.loads(args_str)
                data_shape = shape_dict['outdata_shape']
                data_type  = shape_dict['outdata_type']
 
                import numpy
                data_array = numpy.fromstring(data_str,dtype=data_type).reshape(data_shape)

                #print data_array.shape , data_shape, data_type

                retry_flag = False
                #return 'success', data_str 

 

                return (data_id, data_array)

        else :
        #except:
            print "Error in recv data"
            pass

def send_args(data_id,data_args,address='127.0.0.1',port=int(3939)):
 
    args_list = []
    args_list.append(data_args)

    import cPickle as pickle
    pickled_args = pickle.dumps((data_id,args_list),-1)
 
    return send_data("args**"+data_id,pickled_args,address,port)
 
def save_halo(target_data, target_split, target_key, target_loc, halo_dict, flag=False):
    #print target_split, target_key, target_loc, target_data.shape, type(target_data), target_data.dtype, target_data[:10]
    
    """    
    key = target_key
    location = target_loc
    try :
        data = target_data.tostring()
    except :
        data = target_data
    ori_len = len(data)
    str_ss = str(target_split)
       
    if key in halo_dict: pass
    else: halo_dict[key] = {}
    if str_ss in halo_dict[key]: pass
    else: halo_dict[key][str_ss] = {}
    if location in halo_dict[key][str_ss]: pass
    else: halo_dict[key][str_ss][location] = []
    # string data
    #halo_dict[key][str_ss][location] = [ori_len,data]

    # numpy array data
    try:
        del halo_dict[key][str_ss][location]
    except:
        pass
    halo_dict[key][str_ss][location] = [ori_len,target_data]

    #print "Save Halo"
    """

    key="%s**%s**%s"%(target_key,target_loc,target_split)
    #print key
    halo_dict[key] = target_data

     

def read_halo(halo_dict, target_split, target_loc, target_key):

    key="%s**%s**%s"%(target_key,target_loc,target_split)
    #print key

    if key in halo_dict:
        target_data = halo_dict[key]
        return 'exist', target_data
    else :
        return 'absent',None
    
    #target_split = str(target_split)

    """
    if target_key in halo_dict:
        if target_split in halo_dict[target_key]:
            if target_loc in halo_dict[target_key][target_split]:
                ori_len, data = halo_dict[target_key][target_split][target_loc]
           
                return 'exist',data
        
    """

def extract_halo_cpu(array_data, target_id, args_dict, halo_dict, halo_dirt_dict):
    

    #vm_in =  args_dict['vm_in']
    ImgDim      = args_dict['full_data_shape']
    ImgSplit    = args_dict['split']
    #target_id   = args_dict['target_id']
    data_halo   = args_dict['halo']
   
    #print "extract halo from CPU ",target_id 
    try:
        target_id = int(target_id)
    except:
        target_id = target_id[target_id.rfind('_')+1:]
        target_id = int(target_id)

    #target_id = int(target_id)
    args_dict['target_id'] = target_id
    #args_dict['halo'] = data_halo

    data_shape  = [ImgDim[elem] for elem in ['z','y','x'] if elem in ImgDim]
    input_split = numpy.array([ImgSplit[elem] for elem in ['z','y','x'] if elem in ImgSplit])

    full_data_shape = ImgDim
  

    input_data_shape, input_data_split = generate_data_shape(target_id, ImgDim, ImgSplit, data_halo)



    comm_type='full'

    neighbor_indi_z = ['u','c','d'] if 'z' in full_data_shape else ['-']
    neighbor_indi_y = ['u','c','d'] if 'y' in full_data_shape else ['-']
    neighbor_indi_x = ['u','c','d'] if 'x' in full_data_shape else ['-']

    if 'origin' not in halo_dirt_dict: halo_dirt_dict['origin'] = {}
    halo_dirt_dict['origin'][str(input_data_split)] = True
 
    for _z in neighbor_indi_z:
        for _y in neighbor_indi_y:
            for _x in neighbor_indi_x:
    
                data_cpy_str = "target_data = array_data["
                #ret = check_in_area(full_data_shape, input_data_shape, _x, _y, _z, data_halo, 'write',comm_type)
                #ret = check_in_area(data_shape, input_data_shape, _x, _y, _z, data_halo, 'write',comm_type)
                ret = check_in_area(ImgDim, input_data_shape, _x, _y, _z, data_halo, 'write',comm_type)

                if ret != None:
                    data_cpy_str += ret + "]"
                    exec data_cpy_str in globals(), locals()
                    target_split = input_data_split

                    #target_key = elem.name
                    #target_key = elem.name
                    target_key = 'origin'
                    target_loc = 'z%sy%sx%s'%(_z,_y,_x)

                    #print target_data.shape
                     #print target_split, target_loc, target_key
                    save_halo(target_data, target_split, target_key, target_loc, halo_dict)
               
 
#def append_halo_gpu(devptr_dict, halo_dict, ctx,stream):
def append_halo_gpu(vm_indata, target_id, args_dict, halo_dict, ctx, stream):

 
    ######################################################
    #Prepare (CUDA function)
    ImgDim      = vm_indata.data_shape
    ImgSplit    = args_dict['split']
    data_halo   = args_dict['halo']

    try:
        target_id = int(target_id)
    except:
        target_id = target_id[target_id.rfind('_')+1:]
        #print target_id
        target_id = int(target_id)
        #_target_id = target_id[target_id.rfind('/')+1:target_id.rfind('.')]
        #target_id = int(_target_id)


    #return 
               
    input_data_split = generate_data_split(target_id, ImgSplit)


    neighbor_indi_z = ['u','c','d'] if 'z' in ImgDim else ['-']
    neighbor_indi_y = ['u','c','d'] if 'y' in ImgDim else ['-']
    neighbor_indi_x = ['u','c','d'] if 'x' in ImgDim else ['-']
    split_pattern = {'u':-1, 'c':0, 'd':1}

    comm_type='full'
 
    for _z in neighbor_indi_z:
        for _y in neighbor_indi_y:
            for _x in neighbor_indi_x:
    
                ret = check_in_area(ImgDim, ImgDim, _x, _y, _z, data_halo, 'advanced_read',comm_type)
                if ret != None:
                    target_split = dict(input_data_split)

                    if _x != '-' :
                        target_split['x'] += split_pattern[_x]
                    if _y != '-' :
                        target_split['y'] += split_pattern[_y]
                    if _z != '-' :
                        target_split['z'] += split_pattern[_z]


                    target_key = 'origin'
                    target_loc = 'z%sy%sx%s'%(_z,_y,_x)
                    #print target_split, target_loc, target_key

                    #read_halo(target_data, target_split, target_key, target_loc, halo_dict)
                    flag, data = read_halo(halo_dict, target_split, target_loc, target_key)

                    if data is None:
                        #print '\033[1m', "Missing halo for %s %s"%(target_split,target_loc) ,'\033[0m' 
                        continue

                    print_str = ''

                    #data = numpy.fromstring(data, dtype=numpy.float32)
                    #print data.shape
                    #exec "work_range = {" + ret + "}"
                    work_range = eval("{" + ret + "}")

                    #import pycuda.driver as cuda
                    buffer_size = data.nbytes
                    halo_data_devptr = cuda.mem_alloc(buffer_size)
                    cuda.memcpy_htod(halo_data_devptr, data)

                    print_str += str(data.shape)

                    output = 1

                    def dim_dr_to_range(in_range, axis):
                        if axis in in_range:
                            return numpy.array((in_range[axis][0], in_range[axis][1]),dtype=numpy.uint32)
                        else:
                            return numpy.array((0, 1),dtype=numpy.uint32)

                    cuda_args =  [vm_indata.data]
                    cuda_args += [halo_data_devptr]
                    cuda_args += [cuda.In(dr_dict_to_list(ImgDim))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'x'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'y'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'z'))]
                    cuda_args += [numpy.int32(output)]

                    print_str +=  'x'+ str(dim_dr_to_range(work_range, 'x'))
                    print_str +=  'y'+ str(dim_dr_to_range(work_range, 'y'))
                    print_str +=  'z'+ str(dim_dr_to_range(work_range, 'z'))

                    block, grid = get_block_grid(work_range)
                    #print time.sleep(3)


                    func_name = 'append_halo'
                    #from pycuda.compiler import SourceModule
                    #mod =  SourceModule(open('halo.cu').read(), no_extern_c = True, arch=CUDA_ARCH,options = ["-use_fast_math", "-O3", "-w"])
                    #from pyspark.vislib.package import VisparkMeta
                    #mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch='sm_52',options = ["-use_fast_math", "-O3", "-w"])
                    mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch=CUDA_ARCH,options = ["-use_fast_math", "-O3", "-w"])
                    func = mod.get_function(func_name)
                    #dim = ret.count(':')

                    func(*cuda_args, block=block, grid=grid, stream=stream)

                    ctx.synchronize()

                    halo_data_devptr.free()

                else :
                    pass
                    #print "WTF!!!!!!!!!!!"

def extract_halo_gpu_simple(vm_indata, target_id, args_dict, halo_dict, halo_dirt_dict, ctx, stream):

    #ImgDim      = vm_indata.data_shape
    #ImgSplit    = args_dict['split']
    #data_halo   = args_dict['halo']
    #vm_indata   = args_dict['vm_indata']

    #try:
    #    target_id = int(target_id)
    #except:
    #    _target_id = target_id[target_id.rfind('_')+1:]
    #    #print _target_id
    #    target_id = int(_target_id)

    block_id, dic = args_dict 

    buffershape = dic["buffer_data_shape"]
    datashape   = dic["local_data_shape"]

    halosize = dic["halo_size"]
    halobyte = dic["halo_byte"] 
    halotype = dic["halo_type"]   
 

    result_shape = datashape[1:]
    result_buffer = cuda.pagelocked_empty(shape=((halosize,)+result_shape), dtype=halotype)

    indata = int(vm_indata.data)
    basis= reduce(lambda x,y:x*y,result_shape)
    
    ###################
    #Up case
    ###################

    offset = halosize*halobyte*basis

    cuda.memcpy_dtoh_async(result_buffer,indata+offset,stream)
    stream.synchronize()

    target_data = result_buffer
    target_split = str(block_id)
    target_key = 'origin'
    target_loc = 'z_up'
   
    if False:
        f = open("/home/whchoi/vispark_halo_up_%d.raw"%block_id,"w")
        f.write(result_buffer.astype(numpy.uint8))
        f.close()
    #print result_buffer

    #print "Save Halo",target_data.shape,target_split,target_key,target_loc 
    save_halo(target_data, target_split, target_key, target_loc, halo_dict, flag=True)

    ###################
    #Down case
    ###################

    result_buffer = cuda.pagelocked_empty(shape=((halosize,)+result_shape), dtype=halotype)
    offset = datashape[0]*halobyte*basis

    cuda.memcpy_dtoh_async(result_buffer,indata+offset,stream)
    stream.synchronize()

    target_data = result_buffer
    target_split = str(block_id)
    target_key = 'origin'
    target_loc = 'z_dn'
   
    if False:
        #f = open("/tmp/vispark_block_%d.raw"%block_id,"w")
        f = open("/home/whchoi/vispark_halo_dn_%d.raw"%block_id,"w")
        f.write(result_buffer.astype(numpy.uint8))
        f.close()
    #print result_buffer

    #print "Save Halo",target_data.shape,target_split,target_key,target_loc 
    save_halo(target_data, target_split, target_key, target_loc, halo_dict, flag=True)

def append_halo_gpu_simple(vm_indata, target_id, args_dict, halo_dict, halo_dirt_dict, ctx, stream):

    #ImgDim      = vm_indata.data_shape
    #ImgSplit    = args_dict['split']
    #data_halo   = args_dict['halo']
    #vm_indata   = args_dict['vm_indata']

    #try:
    #    target_id = int(target_id)
    #except:
    #    _target_id = target_id[target_id.rfind('_')+1:]
    #    #print _target_id
    #    target_id = int(_target_id)

    block_id, dic = args_dict 

    buffershape = dic["buffer_data_shape"]
    datashape   = dic["local_data_shape"]

    halosize = dic["halo_size"]
    halobyte = dic["halo_byte"] 
    halotype = dic["halo_type"]   
 

    result_shape = datashape[1:]
    result_buffer = cuda.pagelocked_empty(shape=((halosize,)+result_shape), dtype=halotype)

    indata = int(vm_indata.data)
    basis= reduce(lambda x,y:x*y,result_shape)
    
    ###################
    #Up case
    ###################

    #flag, data = read_halo(halo_dict, target_split, target_loc, target_key)
    offset = 0*halobyte*basis

    #target_data = result_buffer
    target_split = str(block_id-1)
    target_key = 'origin'
    target_loc = 'z_dn'
   
    flag, data = read_halo(halo_dict, target_split, target_loc, target_key)
    
    if data != None:
        if False:
            f = open("/home/whchoi/%s_%s.raw"%(target_split,target_loc),"w")
            f.write(data.astype(numpy.uint8))
            f.close()

        #print "Read Halo",data.shape,target_split,target_key,target_loc 
        #print "Read Halo for",block_id, data.shape,target_split,target_key,target_loc 
        #result_buffer[:] = data
        #cuda.memcpy_htod_async(indata+offset,result_buffer,stream)
        cuda.memcpy_htod_async(indata+offset,data,stream)
        stream.synchronize()



    ###################
    #Down case
    ###################

    offset = (datashape[0]+halosize)*halobyte*basis

    #target_data = result_buffer
    target_split = str(block_id+1)
    target_key = 'origin'
    target_loc = 'z_up'
   
    flag, data = read_halo(halo_dict, target_split, target_loc, target_key)
    
    if data != None:
        #print "Read Halo for",block_id, data.shape,target_split,target_key,target_loc 
        if False:
            f = open("/home/whchoi/%s_%s.raw"%(target_split,target_loc),"w")
            f.write(data.astype(numpy.uint8))
            f.close()


        #result_buffer[:] = data
        #cuda.memcpy_htod_async(indata+offset,result_buffer,stream)
        cuda.memcpy_htod_async(indata+offset,data,stream)
        stream.synchronize()
    #else :
    #    print "Fail to Read",target_split,target_key,target_loc 


    if False:
        result_shape = buffershape
        result_buffer = cuda.pagelocked_empty(shape=(buffershape), dtype=halotype)
        cuda.memcpy_dtoh_async(result_buffer,indata,stream)
        stream.synchronize()
        f = open("/tmp/vispark_block_%d.raw"%block_id,"w")
        f.write(result_buffer.astype(numpy.uint8))
        f.close()
    #print result_buffer

    #print "Save Halo",target_data.shape,target_split,target_key,target_loc 
    #save_halo(target_data, target_split, target_key, target_loc, halo_dict, flag=True)


 




def extract_halo_gpu(vm_indata, target_id, args_dict, halo_dict, halo_dirt_dict, ctx, stream):

    ImgDim      = vm_indata.data_shape
    ImgSplit    = args_dict['split']
    data_halo   = args_dict['halo']
    #vm_indata   = args_dict['vm_indata']

    try:
        target_id = int(target_id)
    except:
        _target_id = target_id[target_id.rfind('_')+1:]
        #print _target_id
        target_id = int(_target_id)


    input_data_split = generate_data_split(target_id, ImgSplit)

    if 'origin' not in halo_dirt_dict: halo_dirt_dict['origin'] = {}
    halo_dirt_dict['origin'][str(input_data_split)] = True

 
    comm_type='full'

    neighbor_indi_z = ['u','c','d'] if 'z' in ImgDim else ['-']
    neighbor_indi_y = ['u','c','d'] if 'y' in ImgDim else ['-']
    neighbor_indi_x = ['u','c','d'] if 'x' in ImgDim else ['-']

  
    for _z in neighbor_indi_z:
        for _y in neighbor_indi_y:
            for _x in neighbor_indi_x:
    
                ret = check_in_area(ImgDim, ImgDim, _x, _y, _z, data_halo, 'advanced_write',comm_type)

                if ret != None:

                    work_range = eval("{" + ret + "}")

                    def create_result_buffer(ds):
                        ret_size = 1
                        for elem in ds:
                            ret_size = ret_size * (ds[elem][1] - ds[elem][0])
                        return ret_size
 
                    #import pycuda.driver as cuda
            
                    try:
                        output = args_dict['output'][1] if len(args_dict['output'])>0 else 1
                    except:
                        #print '745 should be fixed'
                        output = 1
                    
                    # 4 : float
                    buffer_size  = create_result_buffer(work_range)
                    buffer_size *= output

                    #print work_range
                    #print work_range

                    #print buffer_size
                    #print buffer_size

                    try:
                        target_devptr = cuda.mem_alloc(int(buffer_size * 4))
                    except:
                        print work_range
                        print buffer_size
                        print check_device_mem()
                        exit()

                    #ctx.synchronize()
                    #extract_halo(float* out, float* in, int* in_size, int *_x, int *_y, int *_z, int len_vec)
                    def dim_dr_to_range(in_range, axis):
                        if axis in in_range:
                            return numpy.array((in_range[axis][0], in_range[axis][1]),dtype=numpy.uint32)
                        else:
                            return numpy.array((0, 1),dtype=numpy.uint32)

                    cuda_args =  [target_devptr]
                    cuda_args += [vm_indata.data]
                    cuda_args += [cuda.In(dr_dict_to_list(ImgDim))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'x'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'y'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'z'))]
                    cuda_args += [numpy.int32(output)]


                    block, grid = get_block_grid(work_range)

                    # 
                    func_name = 'extract_halo'
                    #from pycuda.compiler import SourceModule
                    #mod =  SourceModule(open('halo.cu').read(), no_extern_c = True, arch="sm_37", options = ["-use_fast_math", "-O3", "-w"])
                    #mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch='sm_52',options = ["-use_fast_math", "-O3", "-w"])
                    mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch=CUDA_ARCH,options = ["-use_fast_math", "-O3", "-w"])
                    #mod =  SourceModule(open('pyspark/vislib/halo.cu').read(), no_extern_c = True, arch="sm_52", options = ["-use_fast_math", "-O3", "-w"])
                    func = mod.get_function(func_name)

                    func(*cuda_args, block=block, grid=grid, stream=stream)
                    #ctx.synchronize()
    
                    # synchronous
                    result_buffer = cuda.pagelocked_empty(shape=(buffer_size), dtype=numpy.float32)


                    cuda.memcpy_dtoh_async(result_buffer, target_devptr,stream)
                    stream.synchronize()

                    target_devptr.free()

                    local_data_shape = []
                    for axis in ['z', 'y', 'x']:
                        if axis in work_range:
                            start, end =  work_range[axis]
                            local_data_shape.append(end-start)

                    if output != 1:
                        local_data_shape.append(output)

                    target_data = result_buffer.reshape(local_data_shape)

                    target_split = input_data_split
                    target_key = 'origin'
                    target_loc = 'z%sy%sx%s'%(_z,_y,_x)
                    #print target_split, target_loc, target_key
                    #print "Extract halo for %s %s"%(target_split,target_loc)
                    save_halo(target_data, target_split, target_key, target_loc, halo_dict, flag=True)
 




def reshape_data(indata, args_dict):
    import numpy
  
    indata_shape    = args_dict['indata_shape']
    indata_type     = args_dict['indata_type']
    data_halo       = args_dict['data_halo']
    
    #indata = numpy.fromstring(indata, dtype=data_type).reshape(data_shape)
    if 'indata_num' in args_dict:
        indata_shape = (args_dict['indata_num'],)+ indata_shape

    indata = numpy.fromstring(indata, dtype=indata_type).reshape(indata_shape)

    return indata
    

def append_halo_cpu(array_data, halo_info, halo_dict, ctx, stream):


    #vm_in     =  args_dict['vm_in']
    target_id   = halo_info['target_id']
    ImgDim      = halo_info['full_data_shape']
    data_halo   = halo_info['halo']
    ImgSplit    = halo_info['split']

    print "append halo to CPU ",target_id 

    data_shape  = [ImgDim[elem] for elem in ['z','y','x'] if elem in ImgDim]
    input_split = numpy.array([ImgSplit[elem] for elem in ['z','y','x'] if elem in ImgSplit])

    full_data_shape = ImgDim



    input_data_shape, input_data_split = generate_data_shape(target_id, ImgDim, ImgSplit, data_halo)
    
    
    s_t1 = time.time()

                           
    neighbor_indi_z = ['u','c','d'] if 'z' in full_data_shape else ['-']
    neighbor_indi_y = ['u','c','d'] if 'y' in full_data_shape else ['-']
    neighbor_indi_x = ['u','c','d'] if 'x' in full_data_shape else ['-']
    split_pattern = {'u':-1, 'c':0, 'd':1}
    #comm_type = elem.comm_type
    comm_type='full'
            
    for _z in neighbor_indi_z:
        for _y in neighbor_indi_y:
            for _x in neighbor_indi_x:

                ret = check_in_area(full_data_shape, input_data_shape, _x, _y, _z, data_halo, 'read', comm_type)
                 
                #print_red(ret)
                if ret != None:
                    target_split = dict(input_data_split)
                                    
                    if _x != '-' :
                        target_split['x'] += split_pattern[_x]
                    if _y != '-' :
                        target_split['y'] += split_pattern[_y]
                    if _z != '-' :
                        target_split['z'] += split_pattern[_z]
                                
                    #target_key = elem.name
                    target_key = 'origin'
                    target_loc = 'z%sy%sx%s'%(_z,_y,_x)

                    #print target_split, target_loc, target_key
                    flag, data = read_halo(halo_dict, target_split, target_loc, target_key)

                    if data != None:
                        #print "Find Halo !!!", target_split, target_loc, target_key
                        #data = reshape_str(data, input_data_shape, target_loc, data_halo, full_data_shape)
                        array_data = attach_halo(array_data, data, input_data_shape, target_loc, data_halo, full_data_shape)
                    else :
                        print '\033[1m', "Missing halo for %s %s"%(target_split,target_loc) ,'\033[0m' 
                        continue


    return array_data

def reading_args(args_dict, pickled_args):
    
    ######################################################
    #Get Data , Information     
    import cPickle as pickle
    (target_id, args_list) = pickle.loads(pickled_args)
  
    #print args_list
    """
    if type(target_id) == int:
        pass
    else:
        try:
            _target_id = target_id[target_id.rfind('_')+1:]
            if _target_id.find('.') > 0: 
                _target_id = _target_id[:_target_id.find('.')]
            _target_id = int(_target_id)
        except:
            _target_id = _target_id[_target_id.rfind('/')+1:_target_id.rfind('.')]
            _target_id = int(_target_id)
    """
    
    #garget_id = int('%04d'%(int(target_id[target_id.rfind('_')+1:])))
    #print target_id, len(args_list[0]['func_args'])

    #vm_out        = args_list[0]['func_args'][target_id][0]
    #vm_in         = args_list[0]['func_args'][target_id][1]
    #vm_local      = args_list[0]['func_args'][target_id][2:]

    full_data_range = args_list['func_args']['meta']['full_data_shape']
    split           = args_list['func_args']['meta']['split']
    halo            = args_list['func_args']['meta']['halo']

    vm_local      = args_list['func_args']['vm_local']
    function_name = args_list['function_name']
    code          = args_list['code']
    num_iter      = args_list['num_iter']
    extern_code   = args_list['extern_code']
    output        = args_list['output']
    work_range    = args_list['work_range']

    #args_dict ={}
    #args_dict['full_data_range']  = full_data_range
    #args_dict['split']            = split
    args_dict['halo']             = halo


    #args_dict['vm_in']     = vm_in
    #args_dict['vm_out']    = vm_out


    args_dict['vm_local']  = vm_local
    
    args_dict['target_id'] = target_id
    args_dict['function_name'] = function_name
    args_dict['code']          = code
    args_dict['extern_code']   = extern_code
    args_dict['output']        = output
    args_dict['num_iter']      = num_iter
    args_dict['work_range']    = work_range
    #args_dict['args']          = args_list[0]

def clear_mem(args_dict):
    for key in args_dict:
        target = args_dict[key]['vm_indata'].data
        target.free()
        del args_dict[key]

    print "cleared"
        
    

def extract_halo_gpu_new(vm_indata, target_id, halo_info, args_dict, halo_dict, halo_dirt_dict, ctx, stream):

    #ImgDim      = vm_indata.data_shape
    #ImgSplit    = args_dict['split']
    #data_halo   = args_dict['halo']
    #vm_indata   = args_dict['vm_indata']

    #try:
    #    target_id = int(target_id)
    #except:
    #    _target_id = target_id[target_id.rfind('_')+1:]
    #    #print _target_id
    #    target_id = int(_target_id)


    #input_data_split = generate_data_split(target_id, ImgSplit)

    #if 'origin' not in halo_dirt_dict: halo_dirt_dict['origin'] = {}
    #halo_dirt_dict['origin'][str(input_data_split)] = True

 
    #comm_type='full'

    #neighbor_indi_z = ['u','c','d'] if 'z' in ImgDim else ['-']
    #neighbor_indi_y = ['u','c','d'] if 'y' in ImgDim else ['-']
    #neighbor_indi_x = ['u','c','d'] if 'x' in ImgDim else ['-']

   
    #for _z in neighbor_indi_z:
        #for _y in neighbor_indi_y:
            #for _x in neighbor_indi_x:
    
                #ret = check_in_area(ImgDim, ImgDim, _x, _y, _z, data_halo, 'advanced_write',comm_type)

                #if ret != None:
    for elem in halo_info:

        target_split, target_loc, target_key, ret = elem  
        
        if True : 
            if True : 
                if True : 
                    work_range = eval("{" + ret + "}")

                    def create_result_buffer(ds):
                        ret_size = 1
                        for elem in ds:
                            ret_size = ret_size * (ds[elem][1] - ds[elem][0])
                        return ret_size
 
                    #import pycuda.driver as cuda
            
                    try:
                        output = args_dict['output'][1] if len(args_dict['output'])>0 else 1
                    except:
                        #print '745 should be fixed'
                        output = 1
                    
                    # 4 : float
                    buffer_size  = create_result_buffer(work_range)
                    buffer_size *= output

                    #print work_range
                    #print work_range

                    #print buffer_size
                    #print buffer_size

                    try:
                        target_devptr = cuda.mem_alloc(int(buffer_size * 4))
                    except:
                        print work_range
                        print buffer_size
                        print check_device_mem()
                        exit()

                    #ctx.synchronize()
                    #extract_halo(float* out, float* in, int* in_size, int *_x, int *_y, int *_z, int len_vec)
                    def dim_dr_to_range(in_range, axis):
                        if axis in in_range:
                            return numpy.array((in_range[axis][0], in_range[axis][1]),dtype=numpy.uint32)
                        else:
                            return numpy.array((0, 1),dtype=numpy.uint32)

                    cuda_args =  [target_devptr]
                    cuda_args += [vm_indata.data]
                    cuda_args += [cuda.In(dr_dict_to_list(ImgDim))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'x'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'y'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'z'))]
                    cuda_args += [numpy.int32(output)]


                    block, grid = get_block_grid(work_range)

                    # 
                    func_name = 'extract_halo'
                    #from pycuda.compiler import SourceModule
                    #mod =  SourceModule(open('halo.cu').read(), no_extern_c = True, arch="sm_37", options = ["-use_fast_math", "-O3", "-w"])
                    #mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch='sm_52',options = ["-use_fast_math", "-O3", "-w"])
                    mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch=CUDA_ARCH,options = ["-use_fast_math", "-O3", "-w"])
                    #mod =  SourceModule(open('pyspark/vislib/halo.cu').read(), no_extern_c = True, arch="sm_52", options = ["-use_fast_math", "-O3", "-w"])
                    func = mod.get_function(func_name)

                    func(*cuda_args, block=block, grid=grid, stream=stream)
                    #ctx.synchronize()
    
                    # synchronous
                    result_buffer = cuda.pagelocked_empty(shape=(buffer_size), dtype=numpy.float32)


                    cuda.memcpy_dtoh_async(result_buffer, target_devptr,stream)
                    stream.synchronize()

                    target_devptr.free()

                    local_data_shape = []
                    for axis in ['z', 'y', 'x']:
                        if axis in work_range:
                            start, end =  work_range[axis]
                            local_data_shape.append(end-start)

                    if output != 1:
                        local_data_shape.append(output)

                    target_data = result_buffer.reshape(local_data_shape)

                    #target_split = input_data_split
                    #target_key = 'origin'
                    #target_loc = 'z%sy%sx%s'%(_z,_y,_x)
                    #print target_split, target_loc, target_key
                    #print "Extract halo for %s %s"%(target_split,target_loc)
                    save_halo(target_data, target_split, target_key, target_loc, halo_dict, flag=True)


def append_halo_gpu_new(vm_indata, target_id,halo_info, args_dict, halo_dict, ctx, stream):

     
#    ######################################################
#    #Prepare (CUDA function)
#    ImgDim      = vm_indata.data_shape
#    ImgSplit    = args_dict['split']
#    data_halo   = args_dict['halo']
#
#    try:
#        target_id = int(target_id)
#    except:
#        target_id = target_id[target_id.rfind('_')+1:]
#        #print target_id
#        target_id = int(target_id)
#        #_target_id = target_id[target_id.rfind('/')+1:target_id.rfind('.')]
#        #target_id = int(_target_id)
#
#
#    #return 
#               
#    input_data_split = generate_data_split(target_id, ImgSplit)
#
#
#    neighbor_indi_z = ['u','c','d'] if 'z' in ImgDim else ['-']
#    neighbor_indi_y = ['u','c','d'] if 'y' in ImgDim else ['-']
#    neighbor_indi_x = ['u','c','d'] if 'x' in ImgDim else ['-']
#    split_pattern = {'u':-1, 'c':0, 'd':1}
#
#    comm_type='full'
# 
#    for _z in neighbor_indi_z:
#        for _y in neighbor_indi_y:
#            for _x in neighbor_indi_x:
#    
#                ret = check_in_area(ImgDim, ImgDim, _x, _y, _z, data_halo, 'advanced_read',comm_type)
#                if ret != None:
#
#
#                    target_split = dict(input_data_split)
#
#                    if _x != '-' :
#                        target_split['x'] += split_pattern[_x]
#                    if _y != '-' :
#                        target_split['y'] += split_pattern[_y]
#                    if _z != '-' :
#                        target_split['z'] += split_pattern[_z]
#
#
#                    target_key = 'origin'
#                    target_loc = 'z%sy%sx%s'%(_z,_y,_x)
#                    #print target_split, target_loc, target_key
#
    for elem in halo_info:

        target_split, target_loc, target_key, ret = elem  
        
        if True : 
            if True : 
                if True : 

                    #read_halo(target_data, target_split, target_key, target_loc, halo_dict)
                    flag, data = read_halo(halo_dict, target_split, target_loc, target_key)

                    if data is None:
                        #print '\033[1m', "Missing halo for %s %s"%(target_split,target_loc) ,'\033[0m' 
                        continue

                    print_str = ''

                    #data = numpy.fromstring(data, dtype=numpy.float32)
                    #print data.shape
                    #exec "work_range = {" + ret + "}"
                    work_range = eval("{" + ret + "}")

                    #import pycuda.driver as cuda
                    buffer_size = data.nbytes
                    halo_data_devptr = cuda.mem_alloc(buffer_size)
                    cuda.memcpy_htod(halo_data_devptr, data)

                    print_str += str(data.shape)

                    output = 1

                    def dim_dr_to_range(in_range, axis):
                        if axis in in_range:
                            return numpy.array((in_range[axis][0], in_range[axis][1]),dtype=numpy.uint32)
                        else:
                            return numpy.array((0, 1),dtype=numpy.uint32)

                    cuda_args =  [vm_indata.data]
                    cuda_args += [halo_data_devptr]
                    cuda_args += [cuda.In(dr_dict_to_list(ImgDim))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'x'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'y'))]
                    cuda_args += [cuda.In(dim_dr_to_range(work_range,'z'))]
                    cuda_args += [numpy.int32(output)]

                    print_str +=  'x'+ str(dim_dr_to_range(work_range, 'x'))
                    print_str +=  'y'+ str(dim_dr_to_range(work_range, 'y'))
                    print_str +=  'z'+ str(dim_dr_to_range(work_range, 'z'))

                    block, grid = get_block_grid(work_range)
                    #print time.sleep(3)


                    func_name = 'append_halo'
                    #from pycuda.compiler import SourceModule
                    #mod =  SourceModule(open('halo.cu').read(), no_extern_c = True, arch=CUDA_ARCH,options = ["-use_fast_math", "-O3", "-w"])
                    #from pyspark.vislib.package import VisparkMeta
                    #mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch='sm_52',options = ["-use_fast_math", "-O3", "-w"])
                    mod =  SourceModule(open('%s/pyspark/vislib/halo.cu'%PYSPARK_PATH).read(), no_extern_c = True, arch=CUDA_ARCH,options = ["-use_fast_math", "-O3", "-w"])
                    func = mod.get_function(func_name)
                    #dim = ret.count(':')

                    func(*cuda_args, block=block, grid=grid, stream=stream)

                    ctx.synchronize()

                    halo_data_devptr.free()

                else :
                    pass
                    #print "WTF!!!!!!!!!!!"



def gpu_htod(array_data, args_dict,ctx,stream):
    #print "NICE TO MEET YOU"
    #print "I DID NOT YOU TO RE-MATCH"
    

    d_A = cuda.mem_alloc(len(array_data))
    

    #d_A_sh = data_range_to_cuda_in(vm_in.data_shape, vm_in.full_data_shape,vm_in.buffer_shape,data_halo=vm_in.data_halo, cuda=cuda)

    #h_A[:] = array_data
    

    #print h_A.shape, h_A.dtype
    #cuda.memcpy_htod_async(d_A, h_A,stream)
    cuda.memcpy_htod_async(d_A, array_data,stream)

    #array_data.shape = (2,256,256)

    array_data_dtype = args_dict['indata_type']
    array_data_shape = args_dict['indata_shape']
    if 'indata_num' in args_dict:
        array_data_shape = (args_dict['indata_num'],)+ array_data_shape

    vm_indata =  VisparkMeta()
    vm_indata.data_kind  = 'devptr'
    vm_indata.data       = d_A
    vm_indata.data_type  = array_data_dtype
    vm_indata.data_shape = shape_tuple_or_list_to_dict(array_data_shape)
    vm_indata.dirt_flag  = False
    vm_indata.isPersist  = False
    vm_indata.result_size =array_data_shape
    vm_indata.comma_cnt = 0
    vm_indata.kernel_code =""
    vm_indata.mod = None
    #if True:
    #    vm_indata.ori_data = array_data
    
    channel = len(vm_indata.data_shape) - len(array_data_shape)
    channel = '' if channel == 0 else array_data_shape[channel]
    vm_indata.cu_dtype   = numpy_dtype_to_vispark_dtype(array_data_dtype, channel)

    # other infomations
    #array_data_shape = array_data.shape

    #print "DATA SHAPE %s from %s"%(str(vm_indata.data_shape), gethostname())
    #print "FLAG : %s"%vm_indata.dirt_flag
   
    import copy 
    
    args_dict['vm_indata'] = vm_indata
    #args_dict['vm_out'] = copy.copy(vm_indata)
    args_dict['vm_out'] = vm_indata


#    print args_dict['vm_indata']
#    print args_dict['vm_out']

#    print args_dict['vm_indata'].keys()
#    print args_dict['vm_out'].keys()


    #vm_in.data_sh   = d_A_sh
    #vm_in.ori_datatype   = vm_in.data_type

    #vm_in.kernel_code = kernel_code

    #args_dict['vm_in'] = vm_in
       
   
default_cnt = 0

def gpu_run(args_dict, in_process, ctx,stream, key=-1):

    

    #vm_in     =  args_dict['vm_in']
    #vm_out    =  args_dict['vm_out']
    vm_indata =  args_dict['vm_out']

    vm_local  =  args_dict['vm_local']
    #vm_in, vm_out = generate_vm(vm_indata, args_dict)
    target_id =  args_dict['target_id']

    d_A = vm_indata.data 
    #d_A_sh = vm_in.data_sh
    data_shape      = vm_indata.data_shape


    #full_data_shape = args_dict['full_data_shape']
    halo            = args_dict['halo']


    ######################################################
    #Source to Source  compile
    function_name = args_dict['function_name']
    #function_dtype = vm_indata.data_type
    #print vm_indata.data_type
    #function_dtype = numpy_dtype_to_vispark_dtype(vm_indata.data_type)
    function_dtype = vm_indata.cu_dtype
    #print function_dtype
    code      = args_dict['code']
    cuda_code = args_dict['extern_code']


    num_iter = args_dict['num_iter']
    #args = args_dict['args']
      
    #function_name = args_dict['function_name']

    if cuda_code != None:
        kernel_code = cuda_code
        #pickled_mod = cuda_code
        #import cPickle as pickle
        #mod = pickle.loads(pickled_mod)
    
        comma_cnt = 0
        #print kernel_code

    else:
        func_dict = get_function_dict(code)

        from translator.vi2cu_translator.main import vi2cu_translator
        from translator.common import load_GPU_attachment

        local_dict=create_local_dict(func_dict[function_name].strip(), vm_indata, vm_local, target_id, function_dtype)

    #print local_dict

        cuda_function_code, comma_cnt, result_class, lambda_func = vi2cu_translator(vivaldi_code=func_dict[function_name], local_dict=local_dict)
    
        attachment = load_GPU_attachment()

        kernel_code = attachment + result_class + 'extern "C"{\n'                                                                    
        kernel_code += cuda_function_code
        kernel_code += '\n}'

        function_name = str(function_name+function_dtype)

    ######################################################
    #Source to Object  compile
    global global_kernel_code
    global global_mod

    if vm_indata.kernel_code == kernel_code:
        mod = vm_indata.mod
    elif global_kernel_code == kernel_code:
        mod = global_mod
    else :        
        mod = SourceModule(kernel_code, no_extern_c = True, arch=CUDA_ARCH ,options = ["-use_fast_math", "-O3", "-w"])
    #mod = SourceModule(kernel_code, no_extern_c = True, arch=CUDA_ARCH)
    #mod = SourceModule(kernel_code, no_extern_c = True, arch="sm_52" ,options = ["-use_fast_math", "-O3", "-w"])
    
    #while True:
    #    try :
    #        mod = SourceModule(kernel_code, no_extern_c = True, arch="sm_37" ,options = ["-use_fast_math", "-O3", "-w"])
    #        break
    #    except :
    #        time.sleep(0.05)
    #        pass

    # store vm
    #vm_indata.mod = mod
    
    #print function_name+function_dtype 

    #print 'FUNCTION NAME', str(function_name+function_dtype)
    cuda_func = mod.get_function(function_name)
    
    ######################################################
    #GPU memory allocation and data copy

    func_args_name = []
    for elem in vm_local:
        func_args_name.append(elem.name)


    def create_result_buffer(meta):
        ret_size = 1
        #for elem in meta.buffer_shape:
            #ret_size = ret_size * (meta.buffer_shape[elem][1] - meta.buffer_shape[elem][0])
        for elem in meta.data_shape:
            if elem in func_args_name:
                ret_size = ret_size * meta.data_shape[elem][1]
        return ret_size
            
    # prepare result_buffer_shape

    vm_out = VisparkMeta()
    vm_out.data_type  = numpy.float32 

    vm_out.kernel_code = kernel_code
    vm_out.mod = mod
    global_kernel_code = kernel_code    
    global_mod = mod



    work_range = args_dict['work_range']
    if work_range == None:
        print "Enter ? "
        out_data_shape = {}
        for elem in vm_indata.data_shape:
            if elem in func_args_name:
            
            # Kmeans Clustering
                #out_data_shape['x'] = vm_indata.data_shape[elem]
                out_data_shape[elem] = vm_indata.data_shape[elem]
    
        #print out_data_shape

        result_size = create_result_buffer(vm_indata)
   
        #import pycuda.driver as cuda
        d_B = cuda.mem_alloc(result_size*(comma_cnt+1)*4)

        d_B_sh = data_range_to_cuda_in(out_data_shape, out_data_shape, out_data_shape, cuda=cuda)
        d_A_sh = data_range_to_cuda_in(data_shape, data_shape, data_shape, data_halo=halo, cuda=cuda)

        wr  = out_data_shape
 
        cuda_args = [d_B, d_B_sh, d_A, d_A_sh]

    else:

        #print_green("Avail Mem %d ", check_device_mem()/1048576) 
        #print_green("Avail Mem %d "%(check_device_mem()/1048576)) 

        output = args_dict['output']
        out_data_shape = {}
        for elem in ['z', 'y', 'x']:
            if elem in work_range:
                out_data_shape[elem] = work_range[elem]

        def create_rb(shape):
            ret_size = 1
            for elem in shape:
                ret_size = ret_size * shape[elem][1]

            return ret_size


        if len(output) == 0:
            nbytes = 4
            size   = 1
            result_size = create_rb(out_data_shape) * size
        elif output[0] == "raw":
            nbytes = 4
            result_size = output[1] 
        else :

            if output[0] == float:
                nbytes = 4
                vm_out.data_type  = numpy.float32 
            elif output[0] == int:
                nbytes = 4
                vm_out.data_type  = numpy.int32 
            elif output[0] == "uchar":
                nbytes = 1
                vm_out.data_type  = numpy.uint8 
 
            size   = output[1]
            result_size = create_rb(out_data_shape) * size

        vm_out.result_size = result_size
        #print vm_out.result_size, vm_out.data_type
        
        #result_size = create_result_buffer(out_data_shape)
       
     
        #result_size = create_rb(out_data_shape)
        #comma_cnt = size - 1
        comma_cnt = 0

        #print "RESULT SIZE !!" ,  out_data_shape, result_size , nbytes, size

        #import pycuda.driver as cuda
        #d_B = cuda.mem_alloc(result_size*nbytes*size)
        d_B = cuda.mem_alloc(result_size*nbytes)

        if nbytes == 4:
            cuda.memset_d32(d_B,0,result_size)
        elif nbytes == 2:
            cuda.memset_d16(d_B,0,result_size)
        elif nbytes == 1:
            cuda.memset_d8(d_B,0,result_size)


        d_B_sh = None

        wr  = out_data_shape

        cuda_args = [d_B, d_A]
      
    vm_out.data_kind   = 'devptr'
    vm_out.data        = d_B
    vm_out.data_sh     = d_B_sh
    vm_out.comma_cnt   = comma_cnt
    vm_out.data_shape  = out_data_shape
    vm_out.cu_dtype    =  function_dtype

        #print_green("Avail Mem %d "%(check_device_mem()/1048576)) 

#    if args_dict['vm_out'] != args_dict['vm_indata']:
#        try :
#            args_dict['vm_out'].data.free()
#            del args_dict['vm_out']
#        except:
#            pass

            
    args_dict['vm_out'] = vm_out
    local_func_args = vm_local
    


    #local_func_args = args['func_args'][target_id][2:]
    for elem in local_func_args:
        #if elem.name in ['x','y','z']:
        #    cuda_args.append(numpy.int32(data_shape[elem.name][0]))
        #    cuda_args.append(numpy.int32(data_shape[elem.name][1]))
        #    continue

        if elem.data_kind in ['list', numpy.ndarray]:
            data = elem.data
            dev_ptr = cuda.mem_alloc(numpy.array(data).nbytes)
            cuda.memcpy_htod_async(dev_ptr, data,stream)
            #cuda.memcpy_dtoh_async(result_buffer, d_B,stream)
            cuda_args.append(dev_ptr)

        elif elem.data_kind is numpy.float32:
            cuda_args.append(numpy.float32(elem.data))
        elif elem.data_kind is numpy.int32:
            cuda_args.append(numpy.int32(elem.data))


    ######################################################
    #Prepare (Execute function)
    block, grid = get_block_grid(wr)
    #print block, grid
    #print block, grid


    #print cuda_args
    #print cuda_args
    #stream.synchronize()

    #cuda_func(*cuda_args, block=block, grid=grid)
    #print cuda_args
    cuda_func(*cuda_args, block=block, grid=grid, stream=stream)
    #stream.synchronize()
  
    #print function_name ,"Done"
    #    stream.synchronize()
    #except:
    #    print function_name
    #    exit()

    #if in_process == True:
    if False:
    #if True:
        vm_indata.data_kind = 'devptr'
        vm_indata.data      = d_B
        vm_indata.data_sh   = d_B_sh
        vm_indata.data_type = 'float'
        vm_indata.dirt_flag = True

        #if vm_indata.isPersist == False:
        #    d_A.free()
        vm_indata.isPersist = False
        vm_indata.comma_cnt   = vm_out.comma_cnt
        vm_indata.result_size = vm_out.result_size
        vm_indata.data_shape  = vm_out.data_shape
    


def gpu_dtoh(outdata, args_dict, ctx,stream):


    #vm_in     =  args_dict['vm_in']
    #vm_indata =  args_dict['vm_indata']
    #vm_out    =  args_dict['vm_indata']
    vm_out    =  args_dict['vm_out']
    #vm_local  =  args_dict['vm_local']


    result_size = vm_out.result_size
    comma_cnt   = vm_out.comma_cnt
    #data_type = vm_out.cu_dtype
    data_type = vm_out.data_type
    


    #d_A = vm_indata.data
    d_B = vm_out.data

    #print "Result ", d_B

    ######################################################
    #DtoH Indata (result_buffer)

    #stream.synchronize()    


    if comma_cnt == 0:
        #result_buffer = numpy.ndarray(((result_size)),dtype=numpy.float32)
        #result_buffer = cuda.pagelocked_empty(shape=(result_size), dtype=numpy.float32)
        result_buffer = cuda.pagelocked_empty(shape=(result_size), dtype=data_type)
    else:
        #result_buffer = numpy.ndarray(((result_size),comma_cnt+1),dtype=numpy.float32)
        #result_buffer = cuda.pagelocked_empty(shape=(result_size,comma_cnt+1), dtype=numpy.float32)
        result_buffer = cuda.pagelocked_empty(shape=(result_size,comma_cnt+1), dtype=data_type)


    #print result_buffer

    #cuda.memcpy_dtoh(result_buffer, d_B)
    cuda.memcpy_dtoh_async(result_buffer, d_B,stream)
    #ctx.synchronize()
    stream.synchronize()

    local_data_shape = []
    for axis in ['z', 'y', 'x']:
        if axis in vm_out.data_shape:
            start, end =  vm_out.data_shape[axis]
            local_data_shape.append(end-start)

    if comma_cnt > 0:
        local_data_shape.append(comma_cnt+1)

    #print "Local_data_shape" ,local_data_shape

    # FFLOAT case
    #aaaaaa = result_buffer.reshape(local_data_shape)
    aaaaaa = result_buffer
    data_type = args_dict['indata_type']
    #aaaaaa = aaaaaa.astype(data_type)

    
    
    #print local_data_shape
    
    ######################################################
    #Debugging Print
    if False:    
    #if True:    
        from PIL import Image
        target_id =  args_dict['target_id']
        Image.fromstring('L',(local_data_shape[1],local_data_shape[0]),aaaaaa).save("/tmp/vispark_output_%04d.png"%(int(target_id)))
    if False:
        target_id =  args_dict['target_id']
        f = open("/tmp/vispark_output_%04d.raw"%(int(target_id)),"w+")
        #f.write(aaaaaa)
        f.write(aaaaaa[:,:,:,0].tostring())
        f.close()
    if False:
    #if True:
        print aaaaaa[:50]

    if False:
        aaaaaa = map(lambda key, value: [key]+ value + [1], aaaaaa, vm_indata.ori_data)
        aaaaaa = numpy.array(aaaaaa, dtype=numpy.float32)
        



    try :
        pass
        #d_A.free()
        #d_B.free()
    except :
        pass

    #print comma_cnt

    #if comma_cnt == 0:
    if True:
        ######################################################
        #Reshape Result Buffer ( test_array = outdata )
        
        #data_shape      = vm_in.data_shape
        #full_data_shape = vm_in.full_data_shape

        """
        full_data_shape = args_dict['full_data_range']
        data_shape      = get_data_shape(args_dict['target_id'], args_dict)
        data_halo       = args_dict['halo']

        array_cpy_str = 'test_array=aaaaaa['
        for axis in ['z','y','x']:
            if axis in data_shape:
                start = data_shape[axis][0]
                end   = data_shape[axis][1]
        
                if start != 0 and data_halo != 0:
                    array_cpy_str += '%d:'%data_halo
                else:
                    array_cpy_str += ':'

                if end != full_data_shape[axis] and data_halo != 0:
                    array_cpy_str += '-%d,'%data_halo
                else:
                    array_cpy_str += ','

        array_cpy_str += ']'
        
        exec array_cpy_str in globals(), locals()
        """

        test_array = aaaaaa
 
        #s_t7 = time.time()
        #print_time("05.Vispark reshape data : %f sec"%(s_t7-s_t6))

        #outdata = test_array.tostring()
        #return test_array.tostring()
        return test_array
        #return test_array.tostring()

