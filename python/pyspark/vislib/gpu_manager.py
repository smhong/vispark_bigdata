import time
import math
import sys
from socket import *
import subprocess
from mpi4py import MPI

import os
SPARK_HOME=os.environ["SPARK_HOME"]
PYSPARK_PATH = "%s/python/"%SPARK_HOME
#PY4J_PATH ="%s/python/lib/py4j-0.8.2.1-src.zip"%SPARK_HOME
PY4J_PATH ="%s/python/lib/py4j-0.10.4-src.zip"%SPARK_HOME

#print PYSPARK_PATH
#print PY4J_PATH


#time.sleep(10)

def get_proc_list():
    procs = []

    #command = "ps -eo pid,command | grep gpu_manager | grep mpirun"
    command = "ps -eo pid,command | grep gpu_manager | grep python"
    output = subprocess.check_output(command,shell=True)
    #print output

    lists = [elem for elem in output.split('\n')][:-1]
   
    lists = [int(elem[:5]) for elem in lists if elem.find('ps') == -1 and elem.find('mpirun') == -1]

    return lists

pid = os.getpid()
host_name = gethostname()
while False:
    cnt = 0
    
    isRunning = get_proc_list()
    
    for elem in isRunning:
        if pid != elem :
            cnt+=1

    if cnt > 0:
        print "[%s:%d] Another GPU manager is working"%(host_name,pid), isRunning 
        time.sleep(3)
    else: 
        break


if PYSPARK_PATH not in sys.path:
    sys.path.append(PYSPARK_PATH)
if PY4J_PATH not in sys.path:
    sys.path.append(PY4J_PATH)


#from gpu_worker import gpu_run, gpu_htod, gpu_dtoh, \
from pyspark.vislib.gpu_worker import gpu_run, gpu_htod, gpu_dtoh, \
                       extract_halo_gpu, append_halo_gpu ,extract_halo_cpu, append_halo_cpu, \
                       reading_args, reshape_data, \
                       check_device_mem, \
                       save_halo, read_halo, \
                       clear_mem

#from gpu_worker import get_halos
class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'

logging = True

def print_green(source):
    if logging:
        print bcolors.OKGREEN,source, host_name,  bcolors.ENDC

def print_(source):
    if logging:
        print source, host_name

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


comm = MPI.COMM_WORLD
mpi_rank = comm.Get_rank()
mpi_size = comm.Get_size()


import pycuda.driver as cuda
cuda.init()
dev_id = 0
maxDevice = cuda.Device.count()
#maxDevice = 1 
dev = cuda.Device(dev_id)
ctx = dev.make_context()
stream, event={},{}

if not logging:
    print "Launch Process among %d/%d [Number of CUDA Device = %d] silence"%(mpi_rank,mpi_size,maxDevice)
else:
    print "Launch Process among %d/%d [Number of CUDA Device = %d] chatter"%(mpi_rank,mpi_size,maxDevice)

#svrsock = socket(AF_INET, SOCK_STREAM)
svrsock = socket(AF_UNIX, SOCK_STREAM)
#svrsock = socket(AF_UNIX, SOCK_DGRAM)
#svrsock.setsockopt(SOL_SOCKET, TCP_NODELAY, 1)
#svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
#svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, "ib0"+"\0")
#svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, "lo"+"\0")

#port = int(sys.argv[1])
#port = int(3939)
#svrsock.bind(('192.168.1.11', port))
try :
    os.remove("/tmp/gpu_manager")
except OSError:
    pass

#svrsock.bind(('127.0.0.1', port))
svrsock.bind("/tmp/gpu_manager")
#svrsock.listen(0)
#svrsock.listen(1024)
#svrsock.listen(4096)
svrsock.listen(16)

#success=False
#while success == False:
#    try:
#        f.write(str(port))
#        f.close()
#        success = True
#    except:
#        pass


# global values

# POLICY
# 0 = demand
# 1 = with_execution
POLICY = 1

num_send =0
num_recv =0

#def get_gpus(program_name):
#    total_resource = 0
#    if POLICY == 0:
#        for elem in VIVALDI_HISTORY:
#            total_resource += VIVALDI_HISTORY[elem][0]
#        
#        print "FROM GET_GPUS", program_name, (int(math.ceil(NUM_GPUS * VIVALDI_HISTORY[program_name][0] / total_resource)), VIVALDI_HISTORY[program_name][0])
#        
#        return min(int(math.ceil(NUM_GPUS * VIVALDI_HISTORY[program_name][0] / total_resource)), VIVALDI_HISTORY[program_name][0])
#    elif POLICY == 1:
#        for elem in VIVALDI_HISTORY:
#            total_resource += VIVALDI_HISTORY[elem][0]*VIVALDI_HISTORY[elem][1]
#        
#        return min(int(math.ceil(NUM_GPUS * VIVALDI_HISTORY[program_name][0]*VIVALDI_HISTORY[program_name][1] / total_resource)), VIVALDI_HISTORY[program_name][0])

#def refresh_history():
#    #print "REFRESHED"
#    curr = time.time()
#    for elem in VIVALDI_HISTORY:
#        if VIVALDI_HISTORY[elem][2] - curr > 1000:
#            del VIVALDI_HISTORY[elem]

#dpk_size = 16*1024
#msg_size = 16*1024
msg_size = 4*1024
#msg_size = 2*1024
data_dict = {}
data_cnt_dict = {}
halo_dict = {}
halo_dirt_dict = {}
args_dict = {}
devptr_dict = {}
halo_info_dict = {}

data_dict_cache={}
args_dict_cache={}

count = 0
loop = True
in_process = True
#in_process = False
Seal=True
start_time=0
temp_time=0
finish_time=0
total_comm_time=0.0
shuffle_num = 0

print_flag = False
second_flag = False

max_block_num=0
block_num=0
alive = True

#halo_flag = False

#svrsock.settimeout(1)
while loop:
    #try:
    if True:
        clisock, addr = svrsock.accept()

        #print_( "Manager connected %s"%str(addr))
        
        while alive:
       
            msg = clisock.recv(msg_size)
            #if msg == 'quit':
                #break
            num_recv += 1       
 
            temp_time = time.time()
            #if command == "GPUS":
            #elif command == "TRAIN":
            #print msg[:msg.rfind('**')]
            command = msg[:msg.find('**')]

            if command =='':
                #print_red(msg)
                break

                            #exit()
            #svrsock.close()

            msg = msg[msg.find('**')+2:]
            if command == 'clear':
                data_dict = {}
                args_dict = {}
                #print "cleared"
                continue

            elif command == 'suicide':
                loop = False
                alive = False
                svrsock.close()
                ctx.detach()
                #cuda.stop() 
                MPI.Finalize()
                exit()     
        
            elif command == 'drop':
                try:
                    ctx.detach()
                    cuda.stop() 
                except :
                    pass
                print_green("Manager receive %s "%(str(command)))
                
                loop = False
                alive = False
                svrsock.close()
                MPI.Finalize()
                exit()     


            key = msg[:msg.find('**')]
            dev_id = hash(key)%maxDevice
            dev = cuda.Device(dev_id)
            print_green("Manager receive %s for %s [%d] "%(str(command),key,dev_id))

            if command == 'persist':

                if 'vm_indata' not in args_dict[key]:
                    print_blue("Upload in Persist %s"%(key))
                    gpu_htod(data_dict[key],args_dict[key],ctx,stream[key])
                    data_dict[key] = None
              
                if args_dict[key]['vm_indata'] != args_dict[key]['vm_out']:
                    try :
                        gpu_data = args_dict[key]['vm_indata']
                        gpu_data.data.free()
                        del gpu_data   
                    except:
                        pass
            
            
 
                args_dict[key]['vm_indata'] = args_dict[key]['vm_out']
                #args_dict[key]['vm_indata'].isPersist = True
                print_bold("Cached %s"%args_dict[key]['vm_indata'].data)

                import copy
        
                args_dict_cache[key] = copy.copy(args_dict[key])

                sending_str = "done**"
                sending_str += '0'*msg_size
                clisock.send(sending_str[:msg_size])
       

            if command == 'hit':

                import copy
        
                if 'vm_indata' in args_dict_cache[key]:
                    print_blue("Cache hit %s"%(key))
                    args_dict[key] = copy.copy(args_dict_cache[key])
                    data_dict[key] = None

                    #args_dict[key]['vm_out'] = copy.copy(args_dict[key]['vm_indata'])
                    args_dict[key]['vm_out'] = args_dict[key]['vm_indata']

                    print_bold( "Hit %s"%args_dict[key]['vm_out'].data)
                
            if command == 'halo_add':
                msg = msg[msg.find('**')+2:]
    
                target_split = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                target_key = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                target_loc = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn = int(msg[:msg.find('**')])

                target_data =''
                for elem in range(lenn):
                    target_data += clisock.recv(msg_size)
                target_data = target_data[:data_len]
                
                #print target_split, target_key, target_loc, data_len, lenn
 
                save_halo(target_data, target_split, target_key, target_loc, halo_dict)
                #block_num += 1 
                
                #print "Process [%d/%d] has block %d now "%(mpi_rank,mpi_size,block_num)


            if command == 'halo_recv':
                msg = msg[msg.find('**')+2:]
    
                target_split = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                target_key = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                target_loc = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]

                flag,data = read_halo(halo_dict, target_split, target_loc, target_key)

                if data == None:
                    print "Missing Halo for %s %s"%(target_split, target_loc)
                #else  
                #    print "Finding Halo for %s %s"%(target_split, target_loc)

                try: 
                    send_str = data.tostring()
                except:
                    send_str = data

                data_len = len(send_str)
                send_str += '0'*msg_size
                lenn = len(send_str)/msg_size 
    
                arg_str = '%s**%s**'%(str(data_len),str(lenn))
                arg_str += '0'*msg_size

                clisock.send(arg_str[:msg_size])
                
                for i in range(lenn):
                    flag = clisock.send(send_str[i*msg_size:(i+1)*msg_size])
                    if flag == 0:
                        raise RuntimeError("Connection broken")
 


            if command == 'send':

                #key = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
        
                                    
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
                
                temp_time = time.time()
                args_str =''
                for elem in range(lenn1):
                    args_str += clisock.recv(msg_size)
                    num_recv += 1       
 

                data_str =''
                for elem in range(lenn2):
                    data_str += clisock.recv(msg_size)
                    num_recv += 1       
                bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
                
                args_str = args_str[:args_len]
                data_str = data_str[:data_len]
          
                print_blue("Manager bandwidth for (%s,%s) [%d] : %f"%(str(command),key,dev_id,bandwidth))

 
                if key in args_dict : pass
                else : args_dict[key] = {}

                import cPickle as pickle
                target_id, shape_dict= pickle.loads(args_str)
                #target_id = int('%04d'%(int(target_id[target_id.rfind('_')+1:])))
                _target_id = target_id

                if type (target_id) == int:
                    pass
                else :
                    try:
                        target_id = target_id[target_id.rfind('_')+1:]

                        #print target_id
                        # kmeans
                        if target_id.find('.') > 0 :
                            target_id = target_id[:target_id.find('.')]
                        target_id = int(target_id)
                    except:
                        target_id = _target_id[_target_id.rfind('/')+1:_target_id.rfind('.')]
                        target_id = int(target_id)
                #args_dict[key]['vm_in'] = args_list[target_id]
                args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                args_dict[key]['indata_type']  = shape_dict['indata_type']
                args_dict[key]['data_halo']    = shape_dict['data_halo']
                args_dict[key]['target_id']    = target_id
                
                stream[key] = cuda.Stream()

                data_dict[key] = reshape_data(data_str,args_dict[key])

                #if in_process == True :
                if False:
                    gpu_htod(data_dict[key],args_dict[key],ctx,stream[key])

                    data_dict[key] = None
                    
                #print "Send    : %s"%key
                
            elif command == 'recv':
                        
                # send split_position location send_cnt data
                #key = msg[:msg.find('**')]

                if key not in data_dict:
                    sending_str = "absent**"
                    sending_str += '0'*msg_size
                    clisock.send(sending_str[:msg_size])
                else :

                    #if key in devptr_dict:
                    #if in_process == True:
                    if True:
                        #data_dict[key]   = gpu_dtoh(args_dict[key],ctx)
                        data_dict[key]  = gpu_dtoh(data_dict[key],args_dict[key],ctx,stream[key])
            

                    data_array = data_dict[key]

                    shape_dict={}
                    shape_dict['outdata_shape'] = data_array.shape
                    shape_dict['outdata_type'] = data_array.dtype

                    import cPickle as pickle
                    args_str = pickle.dumps(shape_dict,-1)

                    args_len=len(args_str)
                    args_str += '0'*msg_size
                    lenn1 = len(args_str)/msg_size

                    data_str = data_array.tostring()
                    data_len = len(data_str)
                    data_str += '0'*msg_size
                    lenn2 = len(data_str)/msg_size

                    sending_str = "exist**"
                    sending_str += "%s**%s**%s**%s**"%(str(args_len), str(lenn1), str(data_len),str(lenn2))
                    sending_str += '0'*msg_size
                    clisock.send(sending_str[:msg_size])
  
                    temp_time = time.time()
                    for elem in range(lenn1):
                        clisock.send(args_str[elem*msg_size:(elem+1)*msg_size])

                    for elem in range(lenn2):
                        clisock.send(data_str[elem*msg_size:(elem+1)*msg_size])
                    #clisock.close()
                    #print_bred("NEVER CALL ME")

                    bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
                    print_blue("Manager bandwidth for (%s,%s) [%d] : %f"%(str(command),key,dev_id,bandwidth))


                    del args_dict[key]
                    del data_dict[key]
                    
                    #print "Return  : %s"%key
                    break
                    #msg = msg[msg.find('**')+2:]
                    #lenn = int(msg[:msg.find('**')])
                    #msg = msg[msg.find('**')+2:]
                    #print split_position, location
    
                    #data_str =''
                    #for elem in range(lenn):
                        #print "BEFORE"
                        #data.append(conn.recv(msg_size))
                    #    data_str += clisock.recv(msg_size)
                    #    num_recv += 1       
                    #data_str = data_str[:data_len]
                        #print "AFTER"
                    #print send_count
        
                    #str_ss = str(split_position)
 
                #print "RECV", key, split_position, location
                #msg = msg[msg.find('**')+2:]
                #split_position = msg[:msg.find('**')]
                #msg = msg[msg.find('**')+2:]
                #try:
                #    exec "split_position = dict(%s)"%split_position
                #except:
                #    #print "error"
                #    split_position = int(split_position)
                #    pass
                #location = msg[:msg.find('**')]
                #msg = msg[msg.find('**')+2:]

        
                #str_ss = str(split_position)
                #if key in data_dict:
                #    if str_ss in data_dict[key]:
                #        #print key, data_dict[key][str_ss].keys(), str_ss
                #        if location in data_dict[key][str_ss]:
                #            ori_len, data = data_dict[key][str_ss][location]
            
                #            sending_str = ''
                #            sending_str += "exist" + "**"
                #            sending_str += ori_len + "**"
                #            sending_str += str(len(data))+ "**"
                #            sending_str += '0' * msg_size
                #            conn.send(sending_str[:msg_size])
                #            num_send += 1
                #            #print "sending",str_ss, location
                #            for elem in range(len(data)):
                #                conn.send(data[elem])
                #                num_send += 1
                #        else:
                #            sending_str = ''
                #            sending_str += "absent" + "**"
                #            sending_str += '0' * msg_size
                #            conn.send(sending_str[:msg_size])
                #    else:
                #        sending_str = ''
                #        sending_str += "absent" + "**"
                #        sending_str += '0' * msg_size
                #        conn.send(sending_str[:msg_size])
                # 
                count += 1

            elif command == 'append':

                if 'origin'  in halo_dict : 
                    if 'vm_indata' not in args_dict[key]:
                        data_dict[key] = append_halo_cpu(data_dict[key],halo_info_dict[key],halo_dict,ctx,stream[key])

                    else : 
                        append_halo_gpu(args_dict[key]['vm_indata'], key, halo_info_dict[key], halo_dict, ctx, stream[key])

 
            elif command == 'run':

                #key = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
        
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                args_str =''
                for elem in range(lenn):
                    args_str += clisock.recv(msg_size)
                    num_recv += 1       
                args_str = args_str[:args_len]
 
                #args_dict[key] = reading_args(args_str)
                #print args_dict.keys()
                reading_args(args_dict[key],args_str)
    
                if 'vm_indata' not in args_dict[key]:
                    print_blue("Upload in Run %s"%(key))
                    gpu_htod(data_dict[key],args_dict[key],ctx,stream[key])
                    data_dict[key] = None

                gpu_run(args_dict[key],in_process,ctx,stream[key], key=key)
                #else:
                    #print_bold( str(halo_dict.keys())+key)
                    #print_bold( halo_dict)
                    #if 'origin' in halo_dict : 
                    #    append_halo_cpu(data_dict[key],indata_dict[key],halo_dict,ctx,stream[key])
                        #del halo_dict[key]
                    #else :

                    #gpu_htod(data_dict[key],args_dict[key],ctx,stream[key])
                    #data_dict[key] = None
                    #gpu_run(args_dict[key],in_process,ctx,stream[key])
                    #data_dict[key]  = gpu_dtoh(data_dict[key],args_dict[key],ctx,stream[key])
                    #del args_dict[key]['vm_indata']
                    #gpu_dtoh(data_dict[key],args_dict[key],ctx)

                #sending_str = "done**"
                #sending_str += '0'*msg_size
                #clisock.send(sending_str[:msg_size])
                #print "Execute : %s"%(key)                    

 
            elif command == 'count':
                block_num += 1 
                #pass
                sending_str = "done**"
                sending_str += '0'*msg_size
                clisock.send(sending_str[:msg_size])
 
                print_( "Process [%d/%d] has block %d now "%(mpi_rank,mpi_size,block_num))

            
            elif command == 'shuffle':
   
                block_num -= 1
                print_("Process [%d/%d] receive signal %d now "%(mpi_rank,mpi_size,block_num))

 
                if (block_num == 0):
                    #for halo_dict_key in halo_dict['origin'].keys():
                    #    if halo_dict_key not in halo_dirt_dict['origin']:
                    #        del halo_dict['origin'][halo_dict_key]

                    import math
                    iter_num = int(math.log(mpi_size,2))
                    for ii in range(iter_num):
                    
                        print_blue("Process [%d/%d] enters shuffle[%d/%d]"%(mpi_rank,mpi_size,ii+1,iter_num))
                        #comm.Barrier()
             
                        send_loc = int(mpi_rank + math.pow(2,ii)) % int(mpi_size)
                        recv_loc = int(mpi_rank - math.pow(2,ii)) % int(mpi_size)

 
                        import cPickle as pickle
                        pickled_dict = pickle.dumps(halo_dict,-1)
           
                        print_red("Size of the halo_dict %s"%len(pickled_dict))
                        #print_red("Process %d send shuffle to %d"%(mpi_rank,send_loc))
                        
                       # comm.Isend(pickled_dict,dest=send_loc,tag=ii + shuffle_num*mpi_size)
                        #comm.Barrier()
                        ##while True :
                        ##    try:
                        ##        comm.Isend(pickled_dict,dest=send_loc,tag=ii + shuffle_num*mpi_size)
                        ##        break
                        ##    except:
                        ##        pass
                        ##comm.Isend([pickled_dict, MPI.CHAR],dest=send_loc,tag=ii + shuffle_num*mpi_size)
                        ##print "Process [%d/%d] send halo_shuffle to [%d/%d]"%(mpi_rank,mpi_size,send_loc,mpi_size)
                        #
                        #print_purple("Process %d recv shuffle from %d"%(mpi_rank,recv_loc))
                        #recv_dict = comm.recv(source=recv_loc,tag = ii + shuffle_num*mpi_size)
                
                        st = time.time()
                        recv_dict = comm.sendrecv(pickled_dict, dest=send_loc, sendtag=ii, source=recv_loc, recvtag=ii)
                        #print 'comm %.03f'%(time.time()-st)
                    
                        #recv_dict = {}
                        recv_dict = pickle.loads(recv_dict)
           
                        for key in recv_dict:
        
                            if key in halo_dict: pass
                            else: halo_dict[key] = {}

                            for str_ss in recv_dict[key]:
          
                                if str_ss in halo_dict[key]: pass
                                else: halo_dict[key][str_ss] = {}

                                for location in recv_dict[key][str_ss]:
            
                                    if location in halo_dict[key][str_ss]: pass
                                    else: halo_dict[key][str_ss][location] = []

                                    try :
                                        del halo_dict[key][str_ss][location]
                                    except:
                                        pass
                                    halo_dict[key][str_ss][location] = recv_dict[key][str_ss][location]
                         
                            """
                            comm_num = 1
                            pickled_dict = ''
                            chunk_size = 1024*1024*1024
 
                            if mpi_rank == send_loc :                        
                                import cPickle as pickle
                                pickled_dict = pickle.dumps(halo_dict,-1)
          
                                data_len = len(pickled_dict)
                                comm_num = int(math.ceil(1.0*data_len/chunk_size))
                                print_red("Size of the halo_dict %s [%d]"%len(pickled_dict),comm_num)
 
                            comm_num = comm.sendrecv(comm_num, dest=send_loc, sendtag=ii, source=recv_loc, recvtag=ii)
                             
                            recv_str = ''

                            for recv_num in range(comm_num-1):
                                recv_str += comm.sendrecv(pickled_dict[recv_num*chunk_size:(recv_num+1)*chunk_size], dest=send_loc, sendtag=ii, source=recv_loc, recvtag=ii)
                   
                            recv_str += comm.sendrecv(pickled_dict[(comm_num-1)*chunk_size:], dest=send_loc, sendtag=ii, source=recv_loc, recvtag=ii)
                    
 
                            if mpi_rank == recv_loc:
                                recv_dict = pickle.loads(recv_dict)
        
                            else :
                                recv_dict = {}
       
                            for key in recv_dict:
        
                                if key in halo_dict: pass
                                else: halo_dict[key] = {}

                                for str_ss in recv_dict[key]:
          
                                    if str_ss in halo_dict[key]: pass
                                    else: halo_dict[key][str_ss] = {}

                                    for location in recv_dict[key][str_ss]:
            
                                        if location in halo_dict[key][str_ss]: pass
                                        else: halo_dict[key][str_ss][location] = []

                                        halo_dict[key][str_ss][location] = recv_dict[key][str_ss][location]
                                
                            comm.Barrier()
                            """       
                        #else: recv_dict[key][str_ss][location] = []
                        #recv_dict[key][str_ss][location] = [ori_len,data]

     
                        #print "Process %d send to %d , recv from %d"%(mpi_rank,send_loc,recv_loc)

                        #comm.Barrier()
                        #shuffle_num += 1
                        #halo_flag = True
                        #block_num = max_block_num
                        #print "Process [%d/%d] has signal %d now "%(mpi_rank,mpi_size,block_num)
                else :
                    pass
                    #print "Process [%d/%d] current signal %d now "%(mpi_rank,mpi_size,block_num)

                sending_str = "done**"
                sending_str += '0'*msg_size
                clisock.send(sending_str[:msg_size])
       
                finish_time=time.time()
        
            # WOOHYUK
            elif command == 'clear':
                clear_mem(args_dict)


            elif command == 'halo':
   
                #block_num -= 1
                #block_num += 1 
                #print msg[:100]              
 
                #key = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                args_lenn = int(msg[:msg.find('**')])
        
                args_str =''
                for elem in range(args_lenn):
                    args_str += clisock.recv(msg_size)
                    num_recv += 1       
                args_str = args_str[:args_len]
 
                import cPickle as pickle
                halo_info_dict[key] = pickle.loads(args_str)
 
                #print "Process [%d/%d] receive signal %d for %s "%(mpi_rank,mpi_size,block_num,key)
                #print "Key for Run %s"%(key)                    
    
                #if key not in args_dict and key not in data_dict and key not in devptr_dict:
                if key not in args_dict and key not in data_dict:
                    #sending_str = "absent**"
                    #sending_str += '0'*msg_size
                    #clisock.send(sending_str[:msg_size])
                    pass
                else :

                    #if in_process == True:
                    #if 'vm_indata' in indata_dict[key]:
                    if 'vm_indata' in args_dict[key]:
                        #extract_halo_gpu(devptr_dict[key],halo_dict, ctx,stream[key])
                        extract_halo_gpu(args_dict[key]['vm_indata'],key, halo_info_dict[key], halo_dict, halo_dirt_dict, ctx, stream[key])
                    else :
                        extract_halo_cpu(data_dict[key],key, halo_info_dict[key],halo_dict, halo_dirt_dict)
                    #print "Key exist "
                    #print args_dict
                    #data_dict[key] = gpu_process(data_dict[key],args_dict[key],clisock)
                    #get_halos(key,data_dict[key],args_dict[key],halo_dict)
     
                    #sending_str = "done**"
                    #sending_str += '0'*msg_size
                    #clisock.send(sending_str[:msg_size])
                
                    #print "Halo Prepare : %s"%(key)                    
    
                    #sending_str = "done**"
                    #sending_str += '0'*msg_size
                    #clisock.send(sending_str[:msg_size])
            
                    if block_num > max_block_num :
                        max_block_num = block_num
       
                finish_time=time.time()
                       
            elif command == 'sendhalo':
                if Seal:
                    start_time = time.time()
                    Seal=False
                    
                    #print_flag = False
                #print addr, msg[:msg.rfind('**')]
                # send split_position location send_cnt data
                #key = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                split_position = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                try:
                    exec "split_position = dict(%s)"%split_position
                except:
                    #print "error"
                    pass
                location = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                send_count = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                ori_len = msg[:msg.find('**')]
                msg = msg[msg.find('**')+2:]
                #print split_position, location
    

                data = ''
                for elem in range(int(send_count)):
                    data += clisock.recv(msg_size)
                data = data[:int(ori_len)]
                #data = []
                #for elem in range(int(send_count)):
                    #print "BEFORE"
                #    data.append(clisock.recv(msg_size))
                #    num_recv += 1       
                    #print "AFTER"
                #print send_count
 
                sending_str = "done**"
                sending_str += '0'*msg_size
                clisock.send(sending_str[:msg_size])
       
                str_ss = str(split_position)
                if key in halo_dict: pass
                else: halo_dict[key] = {}
                if str_ss in halo_dict[key]: pass
                else: halo_dict[key][str_ss] = {}
                if location in halo_dict[key][str_ss]: pass
                else: halo_dict[key][str_ss][location] = []
                halo_dict[key][str_ss][location] = [ori_len,data]
                #halo_dict[key][str_ss][location] = [ori_len,data[:int(ori_len)]]
                #halo_dict[key][str_ss][location] = [ori_len,int(ori_len),len(data[0]),data[0][:int(ori_len)]]
                #print halo_dict[key][str_ss][location]
                #print data
                #print key, data_dict[key].keys()
                clisock.close()

                finish_time=time.time()
            
            total_comm_time += time.time() - temp_time        
            
            #if count % 100 == 0:
                #print data_dict['origin'].keys()
                #print key, str_ss, location
            #time.sleep(0.01)
            #print "COMM DONE"
            #break
                                        
    #except:
    else:
        print "NUM recv %d"%(num_recv*msg_size)
        print "NUM send %d"%(num_send*msg_size)
        print "Communication sync %s"%(finish_time - start_time)
        print "Communication frag sync %s"%total_comm_time

