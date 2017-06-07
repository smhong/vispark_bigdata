# 
#  Additional implementation about binding RDDs 
#
#


import os
import shutil
import sys
from threading import Lock
from tempfile import NamedTemporaryFile
from itertools import chain, ifilter, imap
import atexit
import numpy
import time

from pyspark import accumulators
from pyspark.accumulators import Accumulator                                                                                                                                                                                                 
from pyspark.broadcast import Broadcast
from pyspark.conf import SparkConf
from pyspark.files import SparkFiles
from pyspark.java_gateway import launch_gateway
from pyspark.serializers import PickleSerializer, BatchedSerializer, UTF8Deserializer, \
    PairDeserializer, AutoBatchedSerializer, NoOpSerializer
from pyspark.storagelevel import StorageLevel
from pyspark.rdd import RDD, PipelinedRDD
#from vrdd import RDD, PipelinedRDD
from pyspark.traceback_utils import CallSite, first_spark_call

from py4j.java_collections import ListConverter

from pyspark.vislib.package import VisparkMeta
#from package import VisparkMeta
from pyspark.vislib.assist  import profiler

from hdfs import InsecureClient
import getpass
from socket import *

#from pyspark.vislib.gpu_worker import send_data, send_args, send_halo, recv_data, run_gpu, shuffle_halo, drop_cuda, send_count, recv_halo, gpu_persist, get_cache, clear_devicemem, send_data_seq
from pyspark.vislib.gpu_worker import *
from pyspark.vislib.worker_assist import generate_data_shape

# GL RENDERING
from pyspark.vislib.gl_worker import *

__all__ = ["VisparkRDD"]
result_cnt = 0

Noprof = profiler(work=False)


def read_meta(path, halo=0):
    #ImageName = path[path.rfind('/')+1:]
    ImageName = path[path.rfind('/')+1:]
    client = InsecureClient('http://emerald1:50070',user=getpass.getuser())

    client.delete(ImageName + '/output', True)
    client.delete(ImageName + '/tmp', True)

    ImgDim = {}
    ImgSplit = {}
    ImgDtype = 'uchar'
    with client.read(ImageName +'/.meta', encoding='utf-8') as reader:
        content = reader.read().split('\n')
        for elem in content:
            if elem.startswith('X : '):
                ImgDim['x'] = int(elem[4:])
            if elem.startswith('Y : '):
                ImgDim['y'] = int(elem[4:])
            if elem.startswith('Z : '):
                ImgDim['z'] = int(elem[4:])
            if elem.startswith('X split : '):
                ImgSplit['x'] = int(elem[10:])
            if elem.startswith('Y split : '):
                ImgSplit['y'] = int(elem[10:])
            if elem.startswith('Z split : '):
                ImgSplit['z'] = int(elem[10:])
            if elem.startswith('Dtype : '):
                ImgDtype = elem[8:]
                   

    #data_shape = [ImgDim[elem] for elem in ['z','y','x'] if elem in ImgDim]

    #return data_shape, ImgDim
    return ImgDim, ImgSplit


def vispark_gl_workrange(function_name, func_args= [], etc_args={}, work_range=[], uniforms={}, halo=0, comm_type='full', code=None, main_data={}, extern_code=None, output=[],path='', proto_type = True):
    #for elem in main_data:
        #if elem not in locals():
            #locals()[elem] = main_data[elem]


    # work_range : screen size
    # output     : data type (always RGBA)
    return {'function_name':function_name, 'func_args':func_args, 'etc_args': etc_args, 'uniforms': uniforms, 'code':code, 'extern_code':extern_code, 'output':output, 'work_range':work_range}


def vispark_workrange(function_name, func_args= [], etc_args={}, work_range=[], halo=0, comm_type='full', code=None, main_data={}, num_iter=1, extern_code=None, output=[],path='', proto_type = True):

    #self._comm_type = comm_type
    #self._halo = halo
    # Spliting work_range is needed to implement (change below for loop)
    #print self._data_shape
    #wr_exist_flag = True

    ########################################            
    # Work range is Image Dim
    #if work_range == None:
    #    work_range = {}
    #    for elem in ImgDim:
    #        work_range[elem] = [0, int(ImgDim[elem])]
    #    wr_exist_flag = False
    #    #work_range = full_data_shape
    for elem in main_data:
        if elem not in locals():
            locals()[elem] = main_data[elem]


    # create data_packages for vars func_args (Without Input Data and Output Data)
    func_args_name = list(func_args)
    _func_args = []
    for elem in func_args_name:
        #Remove Data in func_args
        #if proto_type is True:
        #    if elem == func_args[0]::
        #        continue
        if elem in ['x', 'y', 'z']:
            VM = VisparkMeta()
            VM.name = elem
            VM.data_shape= {}
            VM.buffer_shape = {}
            VM.full_data_shape = {}
            VM.data_type = numpy.int32
            VM.data_kind = numpy.int32

            _func_args.append(VM)
            continue

        VM = VisparkMeta()
        VM.name = elem

        #try:
        #    _data = eval(elem)
        #except:
        #    #if elem not in locals():
        #    #    locals()[elem] = main_data[elem]
        _data = eval("elem")

        if type(_data) in [list, numpy.ndarray]:
            if type(_data) == list:
                if type(_data[0]) == float or type(_data[0]) == numpy.float32:
                    _data = numpy.array(_data, dtype=numpy.float32)
                elif type(_data[0]) == int or type(_data[0]) == numpy.int32:
                    _data = numpy.array(_data, dtype=numpy.int32)

            ds = _data.shape
            VM.data_shape       = {}
            VM.full_data_shape  = {}
            VM.buffer_shape     = {}
            if len(ds) == 2:
                axis = ['x', 'y']
                for _elem in range(2):
                    VM.data_shape[axis[_elem]]      = ds[_elem]
                    VM.full_data_shape[axis[_elem]] = ds[_elem]
                    VM.buffer_shape[axis[_elem]]    = ds[_elem]
            elif len(ds) == 3:
                axis = ['z','y','x']
                for _elem in range(3):
                    VM.data_shape[axis[_elem]]      = ds[_elem]
                    VM.full_data_shape[axis[_elem]] = ds[_elem]
                    VM.buffer_shape[axis[_elem]]    = ds[_elem]
            else:
                axis = ['x']
                _elem = 0
                VM.data_shape[axis[_elem]]      = ds[_elem]
                VM.full_data_shape[axis[_elem]] = ds[_elem]
                VM.buffer_shape[axis[_elem]]    = ds[_elem]

            VM.data_kind = type(_data)
            if _data.dtype == numpy.float32:
                VM.data_type = 'float'
            elif _data.dtype == numpy.int32:
                VM.data_type = 'int'
            elif _data.dtype == numpy.uint8:
                VM.data_type = 'uchar'
            VM.data      = _data
    
        elif type(_data) in [int]:
            VM.data_kind = type(numpy.int32(_data))
            VM.data_type = type(numpy.int32(_data))
        elif type(_data) in [float]:
            VM.data_kind = type(numpy.float32(_data))
            VM.data_type = type(numpy.float32(_data))
        else:
            pass
            #print type(_data)

        VM.data = _data

        _func_args.append(VM)
        func_args.remove(elem)
            
        
    #self._work_range = work_range
    #global result_cnt

    # local_func_args contains
    # 1. vm_local  (without input and output data infomation)
    # 2. vm        (infomation about input data)

    target_func_args = {}
    target_func_args['vm_local'] = _func_args

    
    indata_meta = {}
    indata_meta['full_data_shape'] = None
    indata_meta['split']           = None
    indata_meta['halo']            = halo

    # Necessary meta data
    if halo > 0: 
        ImgDim, ImgSplit = read_meta(path,halo)
    
        indata_meta['full_data_shape'] = ImgDim
        indata_meta['split']           = ImgSplit
    target_func_args['meta']       = indata_meta


    return {'function_name':function_name, 'func_args':target_func_args, 'etc_args': etc_args, 'code':code, 'num_iter':num_iter, 'extern_code':extern_code, 'output':output, 'work_range':work_range}, indata_meta



def evaluate_work_range(work_range,main_data={}):
    new_work_range = {}

    for elem in main_data:
        if elem not in locals():
            locals()[elem] = main_data[elem]

    for axis in work_range:
        vals = work_range[axis]

        #try:
            #start = eval(vals[0])
        #except:
            #start = eval(str(main_data[vals[0]]))
        start = eval(str(vals[0]))
        
        #try:
            #end = eval(vals[1])
        #except:
            #locals()[vals[1]] = main_data[vals[1]]
        end = eval(str(vals[1]))
        

        new_work_range[axis] = [start, end]

    return new_work_range

def evaluate_output(output,main_data={}):
    
    for elem in main_data:
        if elem not in locals():
            locals()[elem] = main_data[elem]

    new_output = []
    new_output.append(output[0])
    new_output.append(eval(str(output[1])))

    return new_output

########################################            
# Vispark transformation 


def execGL(data, function_name, func_args= [], etc_args={}, uniforms={},  work_range={}, halo=0, comm_type='full', code=None, main_data={}, extern_code={}, output=[], profiler=Noprof):
    #import time
    profiler.start("execGL")
   
      
    data_id,InGPU =  id_check(data)
    

    if InGPU is False:
        _ ,data = send_data_gl(data_id,data, port=int(4950))
    
        
    work_range = evaluate_work_range(work_range)
    output     = evaluate_output(output)
    

    total_args = vispark_gl_workrange(function_name, func_args, etc_args, uniforms, work_range, halo, comm_type, code, main_data, extern_code=extern_code, output=output, proto_type = False)
    
 
    _ , data = run_gl(data_id,data,total_args)
    
    
    profiler.stop("execGL")

    return data 



def execGPU(data, function_name, func_args= [], etc_args={}, work_range={}, halo=0, comm_type='full', code=None, main_data={}, numiter = 1, extern_code=None, output=[], profiler=Noprof):

    #import time
    profiler.start("execGPU")
   
      
    data_id,InGPU =  id_check(data)
    

    if InGPU is False:
        _ ,data = send_data(data_id,data)
    
        
    work_range = evaluate_work_range(work_range)
    output = evaluate_output(output)
    

    total_args, indata_meta = vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output, proto_type = False)
    
 
    _ , data = run_gpu(data_id,data,total_args,numiter) 
    
    
    profiler.stop("execGPU")

    return data 

def execGPUseq(data, function_name, func_args= [], etc_args={}, work_range={}, halo=0, comm_type='full', code=None, main_data={}, numiter = 1, extern_code=None, output=[],profiler=Noprof):
    
    profiler.start("execGPUseq")

    data_id,InGPU =  id_check(data)

    if InGPU is False:
        _ ,data = send_data_seq(data_id,data)
    
        
    work_range = evaluate_work_range(work_range)
    output = evaluate_output(output)
    

    total_args, indata_meta = vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output, proto_type = False)
    
 
    _ , data = run_gpu(data_id,data,total_args,numiter) 
    
    
    profiler.stop("execGPUseq")

    return data 

def execGPUseq2(data, function_name, func_args= [], etc_args={}, work_range={}, halo=0, comm_type='full', code=None, main_data={}, numiter = 1, extern_code=None, output=[],profiler=Noprof):
    
    profiler.start("execGPUseq")

    data_id,InGPU =  id_check('')

    if InGPU is False:
        _ ,data = send_data_seq2(data_id,data)
    
        
    work_range = evaluate_work_range(work_range)
    output = evaluate_output(output)
    

    total_args, indata_meta = vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output, proto_type = False)
    
 
    _ , data = run_gpu(data_id,data,total_args,numiter) 
    
    
    profiler.stop("execGPUseq")

    return data 

def actionGPU(data,profiler=Noprof):
    
    profiler.start("recvGPU")

    data = action_data(data)

    profiler.stop("recvGPU")

    return data


def recvGPU(data,profiler=Noprof):
    
    profiler.start("recvGPU")

    data = recv_data_new(data)

    profiler.stop("recvGPU")

    return data

def cacheGPU(data,profiler=Noprof):
    
    profiler.start("cacheGPU")

    data_id,InGPU =  id_check(data)

    print data_id,InGPU

    if InGPU is False:
        _ ,data = send_data(data_id,data)
 
    gpu_persist(data_id, data)
    
    
    _, data_str = get_cache(data_id)

    profiler.stop("cacheGPU")

    return data_str

def shuffleCombine(key,values):

    tag = id_generator("TAG_")

    #signal="ready"
    address=gethostname()
    send_request(tag,values)
  

    signal="check" 
  
    #reply = "none"
    reply = send_signal(signal,tag)
    data_id = None
    
    cnt = 0
    while reply == "none":
        if cnt > 5:
            send_request(tag,values)
            cnt = 0

        time.sleep(0.05)
        reply = send_signal(signal,tag)
        cnt += 1
        print "Receive ", reply  

    return key


def shuffleReady(tag=None):

    if tag == None:
        tag = id_generator("TAG_")

    signal="wake"
    address=gethostname()

    #send_signal_socket(signal,tag,address,address,reply=True)   
    send_signal_socket(signal,tag,"ib1",address,reply=True)   

    """
    signal="ready"
    address=gethostname()
    send_signal(signal,tag,[address])
    """
    #address = "/tmp/shuffle_ack"
    #clisock = socket(AF_UNIX, SOCK_STREAM)
    #clisock.connect(address)

    #msg = clisock.recv(msg_size)
    #msg = msg[:msg.find('**')]

    #if msg != tag:
    #    print "Something Wrong" 

    return tag
    
def extractHalo(key,value,dic):
    
    data_id,InGPU =  id_check(value)

    print "extract" ,data_id
    block_idx = dic["nameToidx"][key]

    import cPickle as pickle
    args_str = pickle.dumps((block_idx,dic),-1)

    args_len=len(args_str)
    args_str += '0'*msg_size
    lenn1 = len(args_str)/msg_size

    msg_tag='extract_new'

    
    sending_str = "%s**%s**%s**%s**"%(str(data_id),msg_tag,str(args_len),str(lenn1))
    sending_str += '0'*msg_size

    send_str =''
    #send_str += sending_str[:msg_size]
    send_str += args_str[:lenn1*msg_size]

    address = "/tmp/gpu_manager"
    clisock = socket(AF_UNIX, SOCK_STREAM)
    clisock.connect(address)


    clisock.send(sending_str[:msg_size])

    sent_num = 0
    while sent_num < lenn1:
        sent_flag = clisock.send(send_str[sent_num*msg_size:(sent_num+1)*msg_size])
        if sent_flag == 0:
            raise RuntimeError("Run connection broken")
        sent_num += 1



    #value += send_str

    return (key,value)

def appendHalo(key,value,dic):
    
    data_id,InGPU =  id_check(value)
    print "append" ,data_id

    block_idx = dic["nameToidx"][key]

    import cPickle as pickle
    args_str = pickle.dumps((block_idx,dic),-1)

    args_len=len(args_str)
    args_str += '0'*msg_size
    lenn1 = len(args_str)/msg_size

    msg_tag='append_new'

    sending_str = "%s**%s**%s**%s**"%(str(data_id),msg_tag,str(args_len),str(lenn1))
    sending_str += '0'*msg_size

    send_str =''
    send_str += sending_str[:msg_size]
    send_str += args_str[:lenn1*msg_size]

    value += send_str

    return (key,value)


class VisparkRDD(object):
    
    def __init__(self, ctx=None, numSlices = None, target_rdd=None, path=None, name=None, halo=0, raw_to_array = False, gpu_flag=False, proflie_flag=True):
        self._InGPU = gpu_flag
        #self._InGPU = True
        self._path = path
        self._name = name
        self._rdd = target_rdd
        
        if raw_to_array == True:
 
            ImageName = path[path.rfind('/')+1:]
            client = InsecureClient('http://emerald1:50070',user=getpass.getuser())

            ImgDim = {}
            ImgSplit = {}
            ImgDtype = 'uchar'
            with client.read(ImageName +'/.meta', encoding='utf-8') as reader:
                content = reader.read().split('\n')
                for elem in content:
                    if elem.startswith('X : '):
                        ImgDim['x'] = int(elem[4:])
                    if elem.startswith('Y : '):
                        ImgDim['y'] = int(elem[4:])
                    if elem.startswith('Z : '):
                        ImgDim['z'] = int(elem[4:])
                    if elem.startswith('X split : '):
                        ImgSplit['x'] = int(elem[10:])
                    if elem.startswith('Y split : '):
                        ImgSplit['y'] = int(elem[10:])
                    if elem.startswith('Z split : '):
                        ImgSplit['z'] = int(elem[10:])
                    if elem.startswith('Dtype : '):
                        ImgDtype = elem[8:]


            def init_array (name,raw,ImgDim,ImgSplit,ImgDtype,halo):
            
                import numpy

                if ImgDtype == 'uchar':
                    d_type = numpy.uint8
                elif ImgDtype == 'float':
                    d_type = numpy.float32

                if 'z' not in ImgDim:
                    if 'y' not in ImgDim:
                        ## 1D case
                        dimX = ImgDim['x']/ImgSplit['x'] 
                        array = numpy.fromstring(raw,dtype=d_type).reshape((dimX))
                    ## 2D case
                    else:
                        dimX = ImgDim['x']/ImgSplit['x'] 
                        dimY = ImgDim['y']/ImgSplit['y'] 
                        array = numpy.fromstring(raw,dtype=d_type).reshape((dimY,dimX))
                ## 3D case
                else:   
                    dimX = ImgDim['x']/ImgSplit['x'] 
                    dimY = ImgDim['y']/ImgSplit['y'] 
                    dimZ = ImgDim['z']/ImgSplit['z'] 
                    array = numpy.fromstring(raw,dtype=d_type).reshape((dimZ,dimY,dimX))
 
                return name,array
        
            self._rdd = self._rdd.map(lambda(name,raw):init_array(name,raw,ImgDim,ImgSplit,ImgDtype,halo))

            
        

    def vmap(self, function_name, func_args= [], etc_args={}, work_range={}, halo=0, comm_type='full', code=None, main_data={}, numiter = 1, extern_code=None, output=[]):
        work_range = evaluate_work_range(work_range,main_data)
        output = evaluate_output(output,main_data)

        total_args, indata_meta = vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output, path=self._path)
 
        if self._InGPU == False:
            self._rdd = self._rdd.map(lambda (key, data):send_data(key, data, halo))
            self._InGPU = True 

        if halo != 0:
            self._rdd = self._rdd.map(lambda (key, data):send_halo(key,data,indata_meta))
            
            #self._rdd.foreach(lambda (key, data):send_halo(key,data,indata_meta))
            #self._rdd = self._rdd.map(lambda (key, data):(key,'')).persist()
            self._rdd.foreach(lambda (key, data):send_count(key))
            self._rdd.foreach(lambda (key, data):shuffle_halo(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_halo(key,data))
                
        _rdd = self._rdd.map(lambda (key, data):run_gpu(key,data,total_args,numiter))
                
        global result_cnt 
        self._result_name = 'result%d'%(result_cnt)
        result_cnt += 1
            
        #return self
        return VisparkRDD(target_rdd = _rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)


    ########################################            
    # Vispark transformation 

    def vmapSeq(self, function_name, func_args= [], etc_args={}, work_range={}, halo=0, comm_type='full', code=None, main_data={}, numiter = 1, extern_code=None, output=[]):
        work_range = evaluate_work_range(work_range,main_data)
        output = evaluate_output(output,main_data)


        total_args, indata_meta = vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output)
 
        if self._InGPU == False:
            self._rdd = self._rdd.map(lambda (key, data):send_data_seq(key, data, halo))
            self._InGPU = True 

        if halo != 0:
            self._rdd = self._rdd.map(lambda (key, data):send_halo(key,data,indata_meta))
            
            #self._rdd.foreach(lambda (key, data):send_halo(key,data,indata_meta))
            #self._rdd = self._rdd.map(lambda (key, data):(key,'')).persist()
            self._rdd.foreach(lambda (key, data):send_count(key))
            self._rdd.foreach(lambda (key, data):shuffle_halo(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_halo(key,data))
                
        _rdd = self._rdd.map(lambda (key, data):run_gpu(key,data,total_args,numiter))
                
        global result_cnt 
        self._result_name = 'result%d'%(result_cnt)
        result_cnt += 1
            
        #return self
        return VisparkRDD(target_rdd = _rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)




    def clear(self):
        self._rdd.foreach(lambda (key, data):clear_devicemem(key))


    def halo(self, halo=0):
        
        #total_args, indata_meta = vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output)
         
        if self._InGPU == False:
            self._rdd = self._rdd.map(lambda (key, data):send_data(key, data, halo))
            self._InGPU = True 


        indata_meta = {}
        indata_meta['full_data_shape'] = None
        indata_meta['split']           = None
        indata_meta['halo']            = halo
    
        # Necessary meta data
        if halo > 0: 
            ImgDim, ImgSplit = self.read_meta(self._path,halo)
        
            indata_meta['full_data_shape'] = ImgDim
            indata_meta['split']           = ImgSplit


        if halo != 0:
            self._rdd = self._rdd.map(lambda (key, data):send_halo(key,data,indata_meta))
            
            #self._rdd.foreach(lambda (key, data):send_halo(key,data,indata_meta))
            #self._rdd = self._rdd.map(lambda (key, data):(key,'')).persist()
            #self._rdd.foreach(lambda (key, data):send_count(key))
            #self._rdd.foreach(lambda (key, data):shuffle_halo(key))

            self._rdd.foreach(lambda (key, data):send_count(key))
            self._rdd.foreach(lambda (key, data):shuffle_halo(key))


            self._rdd = self._rdd.map(lambda (key, data):recv_halo(key,data))


        global result_cnt 
        self._result_name = 'result%d'%(result_cnt)
        result_cnt += 1
            
        #return self
        return VisparkRDD(target_rdd = self._rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)


    ########################################            
    # Spark compatible action

    def saveAsPNG(self, name, data_shape, hdfs_flag = False):

        def saveInHDFS(key, data, name, data_shape, flag):

            FileName = key[key.rfind('/')+1:]
            FileIdx  = int(key[key.rfind('_')+1:])
            #print FileName, FileIdx

            from PIL import Image
            Image.fromstring('L',(data_shape[0],data_shape[1]),data).save("/tmp/%s_%04d.png"%(name,FileIdx))
            if flag == True :

                #BaseName = path[path.rfind('/')+1:]
                client = InsecureClient('http://diamond:50070', user=getpass.getuser())
                #client.delete(BaseName, True)
                #client.makedirs(BaseName)
                #client.write(BaseName+'/'+FileName, data)
                #client.upload('%s/output/result_%04d.png'%(path,int(key)),'/tmp/result_%04d.png'%(int(key)))  

        self._rdd.foreach(lambda (key, data): saveInHDFS(key, data, name, data_shape, hdfs_flag ))


    def saveAsRaw(self, name, hdfs_flag = True):

        def saveInHDFS(key, data, name, flag):

            #FileName = key[key.rfind('/')+1:]

            if flag == True :
                FileIdx  = int(key)
                #pass
                BaseName = name
                name='nast'
                #client = InsecureClient('http://emerald1:50070', user=getpass.getuser())

                #client.delete(BaseName, True)
                #client.makedirs(BaseName)
                #if len(data)==0:

                #client.write(BaseName+'/'+FileName, data)
                
                f = open('/tmp/%s_%04d'%(name,FileIdx),"w")
                t_data = numpy.fromstring(data, dtype=numpy.float32).reshape(130, 258, 1026)[1:-1,1:-1,1:-1]
                f.write(t_data.tostring())
                f.close() 
                import os

                os.system('hdfs dfs -put /tmp/%s_%04d %s'%(name, FileIdx, BaseName))
                #print 'hdfs dfs -put /tmp/%s_%04d %s'%(name, FileIdx, BaseName)
                #os.system('rm -f /tmp/%s_%04d'%(name, FileIdx))

                # for Navier-Stokes
                #try:
                #t_data_U = numpy.fromstring(data, dtype=numpy.float32).reshape(2050, 8194, 6)[1:-1,1:-1,0].astype(numpy.uint8)*180
                #t_data_V = numpy.fromstring(data, dtype=numpy.float32).reshape(2050, 8194, 6)[1:-1,1:-1,1].astype(numpy.uint8)*180
                #t_data = numpy.fromstring(data, dtype=numpy.float32).reshape(2050, 8194, 6)[1:-1,1:-1,5].astype(numpy.uint8)*30
                #import Image

                #except:
                #print "data saved"
            else :
                f = open('/tmp/%s.raw'%(name),"wb")
                f.write(data)
                f.close() 
                #print "data saved"
                pass
            

        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False

        self._rdd.foreach(lambda (key, data): saveInHDFS(key, data, name, hdfs_flag ))


    def foreach(self, func):

        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False


        #self._rdd.persist()
        #self._rdd.foreach(lambda (key, data):drop_cuda(key))
        self._rdd.foreach(func)
        return 0


    def collect_old(self):
        if self._InGPU == True:
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._InGPU = False


        return self._rdd.collect()



    ########################################            
    # Spark compatible transformation 

    def rdd(self):
        if self._InGPU == True:
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._InGPU = False
        self._rdd = self._rdd.map(lambda (key, data):(key,recv_data_new(data)))

        return self._rdd

    def union(self ,other):
         
        if self._InGPU == True:
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._InGPU = False
        self._rdd = self._rdd.map(lambda (key, data):(key,recv_data_new(data)))

        return self._rdd.union(other)


    def map(self, f, preservesPartitioning=False):
    
        new_rdd = self._rdd       
 
        if self._InGPU == True:
            new_rdd = new_rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._InGPU = False

        #new_rdd = new_rdd.map(lambda (key, data):(key,recv_data_new(data)))

        def func(_, iterator):
            return imap(f, iterator)
        
        new_rdd = new_rdd.mapPartitionsWithIndex(func, preservesPartitioning)
        
        #return self
        return VisparkRDD(target_rdd = new_rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)

    def mapgpu(self, f, preservesPartitioning=False):
    
        new_rdd = self._rdd       
 
        #if self._InGPU == True:
        #    new_rdd = new_rdd.map(lambda (key, data):recv_data(key,data))
        #    #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
        #    self._InGPU = False

        #new_rdd = new_rdd.map(lambda (key, data):(key,recv_data_new(data)))

        def func(_, iterator):
            return imap(f, iterator)
        
        new_rdd = new_rdd.mapPartitionsWithIndex(func, preservesPartitioning)
        
        #return self
        return VisparkRDD(target_rdd = new_rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)


    def collect_debug(self):
       
        #if self._InGPU == True:
        #    self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
        #    self._InGPU = False
       
        #self._rdd = self._rdd.map(lambda (key, data):(key,recv_data_new(data)))


        return self._rdd.collect()



    def collect(self):
    
        try :       
            if self._InGPU == True:
                self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
                #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
                self._InGPU = False
       
            return self._rdd.map(lambda (key, data):(key,recv_data_new(data))).collect()
        except:
            pass

        return self._rdd.collect()

    def collect_gl(self):
    
        try :       
            if self._InGPU == True:
                self._rdd = self._rdd.map(lambda (key, data):recv_data_gl(key,data))
                self._InGPU = False
       
            return self._rdd.map(lambda (key, data):(key,recv_data_gl(data))).collect()
        except:
            pass

        return self._rdd.collect()
    




    def get_cache(self):
        self._rdd = self._rdd.map(lambda (key, data): get_cache(key))
        self._InGPU = True        
 
        return VisparkRDD(target_rdd = self._rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)

    def newRDD(self):
        return VisparkRDD(target_rdd = self._rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)
        

    def persist(self, tag=None):
        #self._rdd.persist()
  

        if tag == "GPU":
            #print "GPU Persist"
            if self._InGPU == False:

                self._rdd = self._rdd.map(lambda (key, data):send_data(key, data))
                self._InGPU = True 

            self._rdd = self._rdd.map(lambda (key, data):gpu_persist(key, data))
            self._rdd = self._rdd.map(lambda (key, data): get_cache(key))
        else :
            if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
                self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
                self._InGPU = False
 
        return VisparkRDD(target_rdd = self._rdd.persist(), path= self._path, name= self._name, gpu_flag=self._InGPU)
        #if tag == None:
        #    return VisparkRDD(target_rdd = self._rdd.persist(), path= self._path, name= self._name, gpu_flag=self._InGPU)
        #else 
        #    return VisparkRDD(target_rdd = self._rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)


    #def ta
    def takeSample(self, withReplacement, num, seed=None):
        return self._rdd.takeSample(withReplacement,num,seed)


    def reduceByKey(self, func, numPartitions=None):
    
        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False
        
        #return self._rdd.combineByKey(lambda x: x, func, func, numPartitions)
        new_rdd = self._rdd.combineByKey(lambda x: x, func, func, numPartitions)
        
        return VisparkRDD(target_rdd = new_rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)

    def reduceByKeyGPU(self, func, numPartitions=None):
    
        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False
        
        #return self._rdd.combineByKey(lambda x: x, func, func, numPartitions)
        new_rdd = self._rdd.combineByKey(lambda x: x, func, func, numPartitions)
        
        #new_rdd =new_rdd.map(lambda (key,data): (shuffleCombine(key,data)))
        #new_rdd = new_rdd.map(lambda (key,data): (shuffleReady(key),data))
        return VisparkRDD(target_rdd = new_rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)

    def fillKeyGPU(self):
        new_rdd =self._rdd.map(lambda (key,data): (shuffleCombine(key,data),data))
        return VisparkRDD(target_rdd = new_rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)


    def groupByKey(self, numPartitions=None):
 
        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False
    
        self._rdd = self._rdd.map(lambda (key, data):(key,recv_data_new(data)))
        #if self._InGPU == True:
        #    #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
        #    self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
        #    self._InGPU = False
        
        #return self._rdd.combineByKey(lambda x: x, func, func, numPartitions)
        #self._rdd = self._rdd.groupByKey()
        
        return VisparkRDD(target_rdd = self._rdd.groupByKey(), path= self._path, name= self._name, gpu_flag=self._InGPU)

    def shuffleByKeyGPU(self, numPartitions=None):
 
        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False
    
        #self._rdd = self._rdd.map(lambda (key, data):(key,recv_data_new(data)))
        #if self._InGPU == True:
        #    #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
        #    self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
        #    self._InGPU = False
        
        #return self._rdd.combineByKey(lambda x: x, func, func, numPartitions)
        #self._rdd = self._rdd.groupByKey()
        #new_rdd = self._rdd.map(lambda (key,data): (shuffleReady(key),data))
        #new_rdd =self._rdd.map(lambda (key,data): (shuffleCombine(key),data))
        new_rdd =self._rdd.map(lambda (key,data): (shuffleCombine(key,data)))
        
        return VisparkRDD(target_rdd = new_rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)




    def reduce(self, func):
    
        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False
        
        return self._rdd.reduce(func)

    def mapValues(self, func):
    
        #self._rdd = self._rdd.map(lambda (key, data):(key,recv_data_new(data)))
        #if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
        #    self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
        #    self._InGPU = False
        
        #self._rdd = self._rdd.mapValues(func)
        
        return VisparkRDD(target_rdd = self._rdd.mapValues(func), path= self._path, name= self._name, gpu_flag=self._InGPU)



    def drop(self):
        self._rdd.map(drop_cuda())
    
