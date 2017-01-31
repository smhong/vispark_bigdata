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

from hdfs import InsecureClient
import getpass
from socket import *

from pyspark.vislib.gpu_worker import send_data, send_args, send_halo, recv_data, run_gpu, shuffle_halo, drop_cuda, send_count, recv_halo, gpu_persist, get_cache, clear_devicemem
from pyspark.vislib.worker_assist import generate_data_shape

__all__ = ["VisparkRDD"]
result_cnt = 0

# It handles vispark ( binded RDDs )

class VisparkRDD(object):
    
    def __init__(self, ctx=None, numSlices = None, target_rdd=None, path=None, name=None, halo=0, raw_to_array = False, gpu_flag=False, proflie_flag=True):
        self._InGPU = gpu_flag
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

            
        


    def read_meta(self, path, halo=0):
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
        """
        # for 2D
        self._data_shape = [ImgDim[elem] for elem in ['z','y','x'] if elem in ImgDim]
        #self._data_range = {}
        self._data_range = ImgDim
        self._data_dtype = ImgDtype
        self._input_split = numpy.array([ImgSplit[elem] for elem in ['z','y','x'] if elem in ImgSplit])


        self._output_split = None
        #self._halo = halo 


        #self._type = "Image"
        self._ImgDim = ImgDim
        self._ImgSplit = ImgSplit

        #self._input_data_shape_list = []
        #self._input_split_shape = []
        #self._input_name = None
        """
 
        #if 'z' not in ImgDim:
            #if 'y' not in ImgDim:
                ## 1D case
                #for _x in range(ImgSplit['x']):
                    #ds, ss = generate_data_shape(self._data_shape, self._input_split, _x, -1, -1, self._halo)
                    #self._input_data_shape_list.append(ds)
                    #self._input_split_shape.append(ss)
            ## 2D case
            #else:
                #for _y in range(ImgSplit['y']):
                    #for _x in range(ImgSplit['x']):
                        #ds, ss = generate_data_shape(self._data_shape, self._input_split, _x, _y, -1, self._halo)
                        #self._input_data_shape_list.append(ds)
                        #self._input_split_shape.append(ss)
            #
        ## 3D case
        #else:   
            #for _z in range(ImgSplit['z']):
                #for _y in range(ImgSplit['y']):
                    #for _x in range(ImgSplit['x']):
                        #ds, ss = generate_data_shape(self._data_shape, self._input_split, _x, _y, _z, self._halo)
                        #self._input_data_shape_list.append(ds)
                        #self._input_split_shape.append(ss)


        #self._vm_in_list = []
        #for idx in range(len(self._input_split_shape)):
            #VM = VisparkMeta()
            #if self._input_name==None:
                #VM.name = 'origin'
            #else:
                #VM.name = self._input_name
            #VM.data_split      = self._input_split_shape[idx]
            #VM.data_shape      = self._input_data_shape_list[idx]
            #VM.buffer_shape    = self._input_data_shape_list[idx]
            #VM.full_data_shape = self._data_range
            #VM.data_type       = self._data_dtype
                    ##VM.data_kind       = iter
                ##VM.data_halo       = self._halo
                    ##VM.comm_type       = self._comm_type
                    ##VM.halo_updated    = False
#
            #self._vm_in_list.append(VM)
                #VM._print()


    def vispark_workrange(self, function_name, func_args= [], etc_args={}, work_range=[], halo=0, comm_type='full', code=None, main_data={}, num_iter=1, extern_code=None, output=[]):


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

        # create data_packages for vars func_args (Without Input Data and Output Data)
        func_args_name = list(func_args)
        _func_args = []
        for elem in func_args_name:
            if elem == func_args[0]:
                continue
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
            try:
                _data = eval(elem)
            except:
                if elem not in locals():
                    locals()[elem] = main_data[elem]
                _data = eval(elem)

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
            ImgDim, ImgSplit = self.read_meta(self._path,halo)
        
            indata_meta['full_data_shape'] = ImgDim
            indata_meta['split']           = ImgSplit

        target_func_args['meta']       = indata_meta

    
        return {'function_name':function_name, 'func_args':target_func_args, 'etc_args': etc_args, 'code':code, 'num_iter':num_iter, 'extern_code':extern_code, 'output':output, 'work_range':work_range}, indata_meta
 


    ########################################            
    # Vispark transformation 

    def vmap(self, function_name, func_args= [], etc_args={}, work_range={}, halo=0, comm_type='full', code=None, main_data={}, numiter = 1, extern_code=None, output=[]):
        #print "Hello Vmap"
        def evaluate_work_range(wr):
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

        work_range = evaluate_work_range(work_range)

        def evaluate_output(output):
            for elem in main_data:
                if elem not in locals():
                    locals()[elem] = main_data[elem]

            new_output = []
            new_output.append(output[0])
            new_output.append(eval(str(output[1])))
       
            return new_output

        output = evaluate_output(output)
        #print work_range, output 


        total_args, indata_meta = self.vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output)
 
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
    def clear(self):
        self._rdd.foreach(lambda (key, data):clear_devicemem(key))


    def halo(self, halo=0):
        
        #total_args, indata_meta = self.vispark_workrange(function_name, func_args, etc_args, work_range, halo, comm_type, code, main_data, num_iter = numiter, extern_code=extern_code, output=output)
         
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
            FileIdx  = int(key)

            if flag == True :
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
                #f = open('/tmp/%s_%04d.raw'%(name,FileIdx),"wb")
                #f.write(data)
                #f.close() 
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


    def collect(self):
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

        return self._rdd

    def union(self ,other):
         
        if self._InGPU == True:
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._InGPU = False

        return self._rdd.union(other)

    def map(self, f, preservesPartitioning=False):
        
        if self._InGPU == True:
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._InGPU = False

        def func(_, iterator):
            return imap(f, iterator)
        
        self._rdd = self._rdd.mapPartitionsWithIndex(func, preservesPartitioning)
        
        #return self
        return VisparkRDD(target_rdd = self._rdd, path= self._path, name= self._name, gpu_flag=self._InGPU)

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
        
        return self._rdd.combineByKey(lambda x: x, func, func, numPartitions)


    def reduce(self, func):
    
        if self._InGPU == True:
            #self._rdd = self._rdd.map(lambda (key, data):recv_data(key))
            self._rdd = self._rdd.map(lambda (key, data):recv_data(key,data))
            self._InGPU = False
        
        return self._rdd.reduce(func)


    def drop(self):
        self._rdd.map(drop_cuda())
    
