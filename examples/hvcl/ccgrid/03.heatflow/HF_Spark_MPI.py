from pyspark import SparkContext
from hdfs import InsecureClient
import numpy
from PIL import Image
import getpass
import time

from socket import *

def send_count(name):
    msg_size = 4*1024

    address = "/tmp/gpu_manager"
    clisock = socket(AF_UNIX, SOCK_STREAM)
    clisock.connect(address)

    sending_str = "%s**%s**"%("count",str(name))
    sending_str += '0'*msg_size
    clisock.send(sending_str[:msg_size])

    msg = clisock.recv(msg_size)
    reply = msg[:msg.find('**')]

    return name

def shuffle_halo(name):
    msg_size = 4*1024

    address = "/tmp/gpu_manager"
    clisock = socket(AF_UNIX, SOCK_STREAM)
    clisock.connect(address)

    sending_str = "%s**%s**"%("shuffle",str(name))
    sending_str += '0'*msg_size
    clisock.send(sending_str[:msg_size])

    msg = clisock.recv(msg_size)
    reply = msg[:msg.find('**')]

    return name



def genhalo3d(name,data,InfoPack):
    
    patchsize = InfoPack['PatchSize']
    patchnums = InfoPack['PatchNums']
    halo      = InfoPack['ImgHalo']

    t0 = time.time()

  
    def getNeighborLocation(source,xSplit,ySplit,zSplit):
        
        z = (source / xSplit) / ySplit
        y = (source / xSplit) % ySplit
        x = source % xSplit

        def checkinside(x,y,z,X,Y,Z):
            return x >= 0 and y >= 0 and z >= 0 and x < X and y < Y and z < Z

        target = []
            
        if checkinside(x-1,y,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + y*xSplit + x-1,'xl'))
        if checkinside(x+1,y,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + y*xSplit + x+1,'xr'))
        if checkinside(x,y-1,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + (y-1)*xSplit + x,'yl'))
        if checkinside(x,y+1,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + (y+1)*xSplit + x,'yr'))
        if checkinside(x,y,z-1,xSplit,ySplit,zSplit) == True:
            target.append(((z-1)*xSplit*ySplit + y*xSplit + x,'zl'))
        if checkinside(x,y,z+1,xSplit,ySplit,zSplit) == True:
            target.append(((z+1)*xSplit*ySplit + y*xSplit + x,'zr'))
        
        return target 

    
    target_id = getNeighborLocation(int(name),patchnums[0],patchnums[1],patchnums[2])
    
    halo_data = []

    ImgXstart = halo 
    ImgYstart = halo 
    ImgZstart = halo 
   
    ImgXend   = halo + patchsize[0]
    ImgYend   = halo + patchsize[1]
    ImgZend   = halo + patchsize[2]

    ImgHalo = halo

    #comm_flag = []
    
    #elem[0] : split, elem[1] : loc
    for elem in target_id:

        def send_data(target_data, target_split, target_key, target_loc):
            msg_size = 4*1024
            data_str = target_data.tostring()
            data_len = len(data_str)
            data_str += '0'*msg_size
            lenn    = len(data_str)/msg_size

            send_str ="halo_add**%s**%s**%s**%s**%s**%s**"%(str(name),str(target_split),str(target_key),str(target_loc),str(data_len),str(lenn))
            send_str += '0'*msg_size
   
            address = "/tmp/gpu_manager"
            clisock = socket(AF_UNIX, SOCK_STREAM)
            clisock.connect(address)
            
            clisock.send(send_str[:msg_size])

            for i in range(lenn):
                flag = clisock.send(data_str[i*msg_size:(i+1)*msg_size])
                if flag == 0:
                    raise RuntimeError("Connection broken")
        
        if elem[1] == 'zr':
            target_data=data[ImgZend-ImgHalo:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend]
            #comm_flag.append((elem[1], target_data.shape))
            send_data(target_data, elem[0], 'origin', elem[1])
        if elem[1] == 'zl':
            target_data=data[ImgZstart:ImgZstart+ImgHalo,ImgYstart:ImgYend,ImgXstart:ImgXend]
            #comm_flag.append((elem[1], target_data.shape))
            send_data(target_data, elem[0], 'origin', elem[1])
        if elem[1] == 'yr':
            target_data=data[ImgZstart:ImgZend,ImgYend-ImgHalo:ImgYend,ImgXstart:ImgXend]
            #comm_flag.append((elem[1], target_data.shape))
            send_data(target_data, elem[0], 'origin', elem[1])
        if elem[1] == 'yl':
            target_data=data[ImgZstart:ImgZend,ImgYstart:ImgYstart+ImgHalo,ImgXstart:ImgXend]
            #comm_flag.append((elem[1], target_data.shape))
            send_data(target_data, elem[0], 'origin', elem[1])
        if elem[1] == 'xr':
            target_data=data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXend-ImgHalo:ImgXend]
            #comm_flag.append((elem[1], target_data.shape))
            send_data(target_data, elem[0], 'origin', elem[1])
        if elem[1] == 'xl':
            target_data=data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXstart+ImgHalo]
            #comm_flag.append((elem[1], target_data.shape))
            send_data(target_data, elem[0], 'origin', elem[1])
    
    print time.time() - t0, "Halo generate"

    #target_data = data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend]

    #return [(name, (target_data, comm_flag))]
    return name




def attachHalo3d(name,data,InfoPack):

    patchsize = InfoPack['PatchSize']
    datasize  = InfoPack['DataSize']
    patchnums = InfoPack['PatchNums']
    halo      = InfoPack['ImgHalo']

    a = numpy.zeros((datasize[2],datasize[1],datasize[0]),dtype=numpy.float32)
    a.fill(255)   
 
    t0 = time.time() 

    ImgXstart = halo 
    ImgYstart = halo 
    ImgZstart = halo  
 
    ImgXend   = halo + patchsize[0]
    ImgYend   = halo + patchsize[1]
    ImgZend   = halo + patchsize[2]

    ImgHalo = halo

    a[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend] = data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend]
  
    def getNeighborLocation(source,xSplit,ySplit,zSplit):
        
        z = (source / xSplit) / ySplit
        y = (source / xSplit) % ySplit
        x = source % xSplit

        def checkinside(x,y,z,X,Y,Z):
            return x >= 0 and y >= 0 and z >= 0 and x < X and y < Y and z < Z

        target = []
            
        if checkinside(x-1,y,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + y*xSplit + x-1,'xl'))
        if checkinside(x+1,y,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + y*xSplit + x+1,'xr'))
        if checkinside(x,y-1,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + (y-1)*xSplit + x,'yl'))
        if checkinside(x,y+1,z,xSplit,ySplit,zSplit) == True:
            target.append((z*xSplit*ySplit + (y+1)*xSplit + x,'yr'))
        if checkinside(x,y,z-1,xSplit,ySplit,zSplit) == True:
            target.append(((z-1)*xSplit*ySplit + y*xSplit + x,'zl'))
        if checkinside(x,y,z+1,xSplit,ySplit,zSplit) == True:
            target.append(((z+1)*xSplit*ySplit + y*xSplit + x,'zr'))
        
        return target 


    target_id = getNeighborLocation(int(name),patchnums[0],patchnums[1],patchnums[2])

    comm_flag = []

    ImgZdiff = ImgZend-ImgZstart
    ImgYdiff = ImgYend-ImgYstart
    ImgXdiff = ImgXend-ImgXstart
   

    def recv_data(target_loc, target_key, target_split,shape):
        msg_size = 4*1024
        send_str ="halo_recv**%s**%s**%s**%s**"%(name,target_split,target_key,target_loc)
        send_str += '0'*msg_size
   
        address = "/tmp/gpu_manager"
        clisock = socket(AF_UNIX, SOCK_STREAM)
        clisock.connect(address)
        
        #Send Location data    
        clisock.send(send_str[:msg_size])

        #Recv data parameter
        msg = clisock.recv(msg_size)

        data_len = int(msg[:msg.find('**')])
        msg = msg[msg.find('**')+2:]
        lenn = int(msg[:msg.find('**')])

        #Recv actual data
        target_data = ''
        for i in range(lenn):
           target_data += clisock.recv(msg_size)

        target_data = target_data[:data_len]

        print target_split, target_loc, data_len 

        return numpy.fromstring(target_data,dtype=numpy.float32).reshape(shape)
    

    #for elem in comm_flag:
    for elem in target_id:

        if elem[1] == 'zl':
            shape = (ImgHalo, ImgYdiff, ImgXdiff)
            a[:ImgZstart,ImgYstart:ImgYend,ImgXstart:ImgXend] = recv_data('zr', 'origin', int(name),shape)
        if elem[1] == 'zr':
            shape = (ImgHalo, ImgYdiff, ImgXdiff)
            a[ImgZend:,ImgYstart:ImgYend,ImgXstart:ImgXend]   = recv_data('zl', 'origin', int(name),shape)
        if elem[1] == 'yl':
            shape  = (ImgZdiff, ImgHalo, ImgXdiff)
            a[ImgZstart:ImgZend,:ImgYstart,ImgXstart:ImgXend] = recv_data('yr', 'origin', int(name),shape)
        if elem[1] == 'yr':
            shape  = (ImgZdiff, ImgHalo, ImgXdiff)
            a[ImgZstart:ImgZend,ImgYend:,ImgXstart:ImgXend]   = recv_data('yl', 'origin', int(name),shape)
        if elem[1] == 'xl':
            shape  = (ImgZdiff, ImgYdiff, ImgHalo)
            a[ImgZstart:ImgZend,ImgYstart:ImgYend,:ImgXstart] = recv_data('xr', 'origin', int(name),shape)
        if elem[1] == 'xr':
            shape  = (ImgZdiff, ImgYdiff, ImgHalo)
            a[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXend:]   = recv_data('xl', 'origin', int(name),shape)
    
 
    print time.time() - t0, "Halo attach"

    return name,a

def keysort(x,y):
    
    a = []

    for elem in x:
        a.append(elem) 
 
    for elem in y:
        a.append(elem) 

    return a

def writeBytes(key,value,client,path,ImgDim):
    
    client.write('%s/output/result_%04d.raw'%(path,int(key)),value.tostring())

def writeRaw(key,value,InfoPack,haloprint=False):
   
    patchsize = InfoPack['PatchSize']
    datasize  = InfoPack['DataSize']
    patchnums = InfoPack['PatchNums']
    halo      = InfoPack['ImgHalo']

    path      = InfoPack['Path']
 
    if haloprint == True:
        print "Try To Write"
        t0 = time.time()
        
        File = open("/tmp/result_%04d.raw"%(int(key)),"wb")
        File.write(value)
        #Image.fromstring('L',(datasize[0],datasize[1]),value).save("/tmp/result_%04d.png"%(int(key)))
        #Image.fromstring('L',datasize,value).save("/tmp/result_%04d.png"%(int(key)))
    
        t1 = time.time()

        #client.upload('%s/output/result_%04d.raw'%(path,int(key)),'/tmp/result_%04d.raw'%(int(key)),True)

        t2 = time.time()
        print t1 - t0, "Write Raw"
        print t2 - t1, "Upload Raw"


def writePNG(key,value,client,InfoPack,haloprint=False):
   
    patchsize = InfoPack['PatchSize']
    datasize  = InfoPack['DataSize']
    patchnums = InfoPack['PatchNums']
    halo      = InfoPack['ImgHalo']

    path      = InfoPack['Path']
 
    if haloprint == True:
        t0 = time.time()
        
        Image.fromstring('L',(datasize[0],datasize[1]),value).save("/tmp/result_%04d.png"%(int(key)))
        #Image.fromstring('L',datasize,value).save("/tmp/result_%04d.png"%(int(key)))
    
        t1 = time.time()

        #client.upload('%s/output/result_%04d.png'%(path,int(key)),'/tmp/result_%04d.png'%(int(key)))

        t2 = time.time()
        print t1 - t0, "Write PNG"
        print t2 - t1, "Upload PNG"

#def writePNG(key,value,path,ImgDim):

 #   Image.fromstring('L',ImgDim,value).save("%s/result_%04d.png"%(path,int(key)))


def heatflow3d(name,data,InfoPack):

    patchsize = InfoPack['PatchSize']
    datasize  = InfoPack['DataSize']
    halo      = InfoPack['ImgHalo']

    t0 = time.time()

    g = numpy.array(data).astype(numpy.float32)

    a = numpy.roll(g, 1,axis=0)
    b = numpy.roll(g,-1,axis=0)
    c = numpy.roll(g, 1,axis=1)
    d = numpy.roll(g,-1,axis=1)
    e = numpy.roll(g, 1,axis=2)
    f = numpy.roll(g,-1,axis=2)

    data = g + (a+b+c+d+e+f-6.0*g)/4.0
   
    t1 = time.time()
    print t1 - t0, "Heat Flow"

    return (name,data.astype(numpy.float32))


def init_raw(name,data,InfoPack):
 
    patchsize = InfoPack['PatchSize']
    datasize  = InfoPack['DataSize']
    halo      = InfoPack['ImgHalo']

    name = '%04d'%(int(name[name.rfind('_')+1:]))
    ori_data = numpy.fromstring(data,dtype=numpy.float32).reshape((patchsize[2],patchsize[1],patchsize[0]))
        
    data = numpy.zeros((datasize[2],datasize[1],datasize[0]),dtype=numpy.float32)
    data.fill(255)

    data[halo:halo+patchsize[2],halo:halo+patchsize[1],halo:halo+patchsize[0]] = ori_data

    return (name,data) 


if __name__ == "__main__":
    
    #if len(sys.argv) < 5:
    #    print "Usage : %s input_image kernel_size parallel_factor"%(sys.argv[0])
    #    quit()
    import sys
 
    username = getpass.getuser()
    #ImageName = 'castle'
    #ImageName = 'SolData_03_256_0'
    #ImageName = 'SolData_03_1024'
    #ImageName = 'SolData_03_2048'
    #ImageName = 'SolData_03_4096'
    #ImageName = 'lena_gray'
    #ImageName = 'number'
    #ImageName  = 'heic1502a'
    #ImageName  = 'heic1502a_o'
    #ImageName = 'HK_75M'
    #ImageName = 'Heatflow_16G'
    ImageName = 'Heatflow_8G'
    #ImageName = 'Heatflow_4G'
    ImagePath = 'hdfs://emerald1:9000/user/' + username + '/' + ImageName + '/'
    client = InsecureClient('http://emerald1:50070',user=username)

    #ImgHalo = 20
    #kernel = 1 if len(sys.argv) < 2 else int(sys.argv[1])

    ImgHalo = 1

    sc = SparkContext(appName="HF_Spark_MPI_"+str(ImageName))
    print "SPARK : ", sc.version

    ImgDim = [-1,-1,-1]
    ImgSplit = [1,1,1]

    client.delete(ImageName + '/output', True)
    client.delete(ImageName + '/tmp', True)

    with client.read(ImageName +'/.meta', encoding='utf-8') as reader:
        content = reader.read().split('\n')
        
        for elem in content:
            if elem.startswith('X : '):
                ImgDim[0] = int(elem[4:])
            if elem.startswith('Y : '):
                ImgDim[1] = int(elem[4:])
            if elem.startswith('Z : '):
                ImgDim[2] = int(elem[4:])
            if elem.startswith('X split : '):
                ImgSplit[0] = int(elem[10:])
            if elem.startswith('Y split : '):
                ImgSplit[1] = int(elem[10:])
            if elem.startswith('Z split : '):
                ImgSplit[2] = int(elem[10:])

    print ImgDim, ImgSplit
    PatchDim = map(lambda x,y:x/y,ImgDim,ImgSplit)
    
    import copy

    DataDim = copy.deepcopy(PatchDim)
    DataDim[0] = DataDim[0] + 2*ImgHalo
    DataDim[1] = DataDim[1] + 2*ImgHalo
    DataDim[2] = DataDim[2] + 2*ImgHalo

    ImagePatch = sc.binaryFiles(ImagePath)
  
    # Image  
    if ImgDim[2] == -1:
        pass

    else :
        print "Raw Case" 
    # Raw  
        InfoPack = {}
        InfoPack['PatchSize'] = PatchDim
        InfoPack['PatchNums'] = ImgSplit
        InfoPack['ImgHalo']   = ImgHalo
        InfoPack['DataSize']  = DataDim
        InfoPack['Path'] = ImagePath

        print InfoPack

        ImagePatch = ImagePatch.map(lambda (name,data): init_raw(name,data,InfoPack))
  
        for ii in range(5):
            # Serialzation
            ImagePatch = ImagePatch.persist()

            # Halo gen, Deserialization
            ImageHalo = ImagePatch.map(lambda (name,data): genhalo3d(name,data,InfoPack))
            ImageHalo.foreach (lambda (key):send_count(key)) 

            # Deserialization 
            ImagePatch.foreach(lambda (key,data):shuffle_halo(key)) 

            # Deserialization 
            ImagePatch = ImagePatch.map(lambda (key,data): attachHalo3d(key,data,InfoPack))
            ImagePatch = ImagePatch.map(lambda (key,data): heatflow3d(key,data,InfoPack))

        ImagePatch.foreach(lambda (key,data): writeRaw(key,data,InfoPack,True))

