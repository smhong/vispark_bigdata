# NEW PROJECT
# GL RENDERING WITH VISPARK
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

SPARK_HOME=os.environ["SPARK_HOME"]
PYSPARK_PATH = "%s/python/"%SPARK_HOME

data_id_prefix = "GL_"
_id_len = 24
msg_size =512

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


def recv_data_gl(data_str,address='127.0.0.1',port=int(3939)):

    data_id,InGPU = id_check(data_str) 

    if InGPU == True:
        if True:
            address = "/tmp/gpu_manager"
            host_name = gethostname()
            clisock = socket(AF_INET, SOCK_STREAM)
            clisock.connect((host_name,4950))

            msg_tag="recv"
            sending_str = "%s**%s**"%(str(data_id),msg_tag)
            sending_str += '0'*msg_size
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


            print_time("recvGPU_1")

 
            #1.2 sec                   
            msg = clisock.recv(msg_size)
            reply = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]
            
            print_time("recvGPU_2")
            
            #print reply
            if reply == 'absent':
                retry_flag = False
                return None
            elif reply == 'exist':

                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
 
                temp_time = time.time()
                recv_num = 0

                args_str = sock_recv(clisock,lenn1)
                
                data_str = sock_recv(clisock,lenn2)
                recv_num = 0
 
                bandwidth = (recv_num)*msg_size / (time.time() - temp_time) / (1048576.0)

                #clisock.close()
                data_str = data_str[:data_len]
                import cPickle as pickle
                shape_dict= pickle.loads(args_str)
                data_shape = shape_dict['outdata_shape']
                data_type  = shape_dict['outdata_type']
 
                import numpy
                data_array = numpy.fromstring(data_str,dtype=data_type).reshape(data_shape)

                retry_flag = False

                return data_array

        else :
        #except:
            print "Error in recv data"
            pass
    else :
        return data_str



def sock_recv(clisock,lenn):
    import cStringIO 
    str_buf = cStringIO.StringIO()

    for i in xrange(lenn):
        str_buf.write(clisock.recv(msg_size)) 

    return str_buf.getvalue()

def send_data_gl(data_id,data_array,halo=0,address='127.0.0.1',port=int(3939)):
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





def run_gl(data_id, data_str, args_list, address='127.0.0.1', port=int(3940)):
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

    sending_str = "%s**%s**%s**%s**"%(str(data_id),"run",str(args_len),str(lenn))
    sending_str += '0'*msg_size
        #clisock.send(sending_str[:msg_size])
    data_str+=sending_str[:msg_size]

    for sent_num in range(lenn):
        data_str+=args_str[sent_num*msg_size:(sent_num+1)*msg_size]

    return (data_id,data_str)


