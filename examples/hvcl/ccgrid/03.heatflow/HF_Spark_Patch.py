from pyspark import SparkContext
from hdfs import InsecureClient
import numpy
from PIL import Image
import getpass
import time
import os


def genhalo(name,data,InfoPack,corner=False):
    
    patchsize = InfoPack['PatchSize']
    patchnums = InfoPack['PatchNums']
    halo      = InfoPack['ImgHalo']

    t0 = time.time()

  
    def getNeighborLocation(source,xSplit,ySplit):
        
        y = source / xSplit
        x = source % xSplit

        def checkinside(x,y,X,Y):
            return x >= 0 and y >= 0 and x < X and y < Y

        target = []
            
        target.append((int(name),'c'))
        
        if checkinside(x-1,y,xSplit,ySplit) == True:
            target.append((y*xSplit + x-1,'xl'))
        if checkinside(x+1,y,xSplit,ySplit) == True:
            target.append((y*xSplit + x+1,'xr'))
        if checkinside(x,y-1,xSplit,ySplit) == True:
            target.append(((y-1)*xSplit + x,'yl'))
        if checkinside(x,y+1,xSplit,ySplit) == True:
            target.append(((y+1)*xSplit + x,'yr'))
        
        if corner == True:
            if checkinside(x-1,y-1,xSplit,ySplit) == True:
                target.append(((y-1)*xSplit + x-1,'c0'))
            if checkinside(x-1,y+1,xSplit,ySplit) == True:
                target.append(((y+1)*xSplit + x-1,'c1'))
            if checkinside(x+1,y-1,xSplit,ySplit) == True:
                target.append(((y-1)*xSplit + x+1,'c2'))
            if checkinside(x+1,y+1,xSplit,ySplit) == True:
                target.append(((y+1)*xSplit + x+1,'c3'))
 
        return target 

    target_id = getNeighborLocation(int(name),patchnums[0],patchnums[1])
   
 
    halo_data = []

    ImgXstart = halo 
    ImgYstart = halo 
   
    ImgXend   = halo + patchsize[0]
    ImgYend   = halo + patchsize[1]

    ImgHalo = halo

    for elem in target_id:
        if elem[1] == 'c':
            halo_data.append(('%04d'%elem[0],('c',data[ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'yr':
            halo_data.append(('%04d'%elem[0],('yl',data[ImgYend-ImgHalo:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'yl':
            halo_data.append(('%04d'%elem[0],('yr',data[ImgYstart:ImgYstart+ImgHalo,ImgXstart:ImgXend])))
        if elem[1] == 'xr':
            halo_data.append(('%04d'%elem[0],('xl',data[ImgYstart:ImgYend,ImgXend-ImgHalo:ImgXend])))
        if elem[1] == 'xl':
            halo_data.append(('%04d'%elem[0],('xr',data[ImgYstart:ImgYend,ImgXstart:ImgXstart+ImgHalo])))
        if elem[1] == 'c0':
            halo_data.append(('%04d'%elem[0],('c3',data[ImgYstart:ImgYstart+ImgHalo,ImgXstart:ImgXstart+ImgHalo])))
        if elem[1] == 'c1':
            halo_data.append(('%04d'%elem[0],('c2',data[ImgYend-ImgHalo:ImgYend,ImgXstart:ImgXstart+ImgHalo])))
        if elem[1] == 'c2':
            halo_data.append(('%04d'%elem[0],('c1',data[ImgYstart:ImgYstart+ImgHalo,ImgXend-ImgHalo:ImgXend])))
        if elem[1] == 'c3':
            halo_data.append(('%04d'%elem[0],('c0',data[ImgYend-ImgHalo:ImgYend,ImgXend-ImgHalo:ImgXend])))
    #print time.time() - t0, "Halo generate"

    return halo_data

def fakegenhalo3d(name,data,InfoPack):
    
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
            
        target.append((int(name),'c'))
        
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

 
    for elem in target_id:
        if elem[1] == 'c':
            halo_data.append(('%04d'%elem[0],('c',data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'zr':
            halo_data.append(('%04d'%elem[0],('zl',data[ImgZend-ImgHalo:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'zl':
            halo_data.append(('%04d'%elem[0],('zr',data[ImgZstart:ImgZstart+ImgHalo,ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'yr':
            halo_data.append(('%04d'%elem[0],('yl',data[ImgZstart:ImgZend,ImgYend-ImgHalo:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'yl':
            halo_data.append(('%04d'%elem[0],('yr',data[ImgZstart:ImgZend,ImgYstart:ImgYstart+ImgHalo,ImgXstart:ImgXend])))
        if elem[1] == 'xr':
            halo_data.append(('%04d'%elem[0],('xl',data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXend-ImgHalo:ImgXend])))
        if elem[1] == 'xl':
            halo_data.append(('%04d'%elem[0],('xr',data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXstart+ImgHalo])))
    
    #print time.time() - t0, "Halo generate"

    return halo_data



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
            
        target.append((int(name),'c'))
        
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

 
    for elem in target_id:
        if elem[1] == 'c':
            halo_data.append(('%04d'%elem[0],('c',data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'zr':
            halo_data.append(('%04d'%elem[0],('zl',data[ImgZend-ImgHalo:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'zl':
            halo_data.append(('%04d'%elem[0],('zr',data[ImgZstart:ImgZstart+ImgHalo,ImgYstart:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'yr':
            halo_data.append(('%04d'%elem[0],('yl',data[ImgZstart:ImgZend,ImgYend-ImgHalo:ImgYend,ImgXstart:ImgXend])))
        if elem[1] == 'yl':
            halo_data.append(('%04d'%elem[0],('yr',data[ImgZstart:ImgZend,ImgYstart:ImgYstart+ImgHalo,ImgXstart:ImgXend])))
        if elem[1] == 'xr':
            halo_data.append(('%04d'%elem[0],('xl',data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXend-ImgHalo:ImgXend])))
        if elem[1] == 'xl':
            halo_data.append(('%04d'%elem[0],('xr',data[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXstart+ImgHalo])))
    
    #print time.time() - t0, "Halo generate"

    return halo_data


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
 
    for i in range(0,len(data),2):
        if data[i] == 'c':
            a[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXstart:ImgXend] = data[i+1]
        if data[i] == 'zl':
            a[:ImgZstart,ImgYstart:ImgYend,ImgXstart:ImgXend] = data[i+1]
        if data[i] == 'zr':
            a[ImgZend:,ImgYstart:ImgYend,ImgXstart:ImgXend] = data[i+1]
        if data[i] == 'yl':
            a[ImgZstart:ImgZend,:ImgYstart,ImgXstart:ImgXend] = data[i+1]
        if data[i] == 'yr':
            a[ImgZstart:ImgZend,ImgYend:,ImgXstart:ImgXend] = data[i+1]
        if data[i] == 'xl':
            a[ImgZstart:ImgZend,ImgYstart:ImgYend,:ImgXstart] = data[i+1]
        if data[i] == 'xr':
            a[ImgZstart:ImgZend,ImgYstart:ImgYend,ImgXend:] = data[i+1]
    
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

def writeRaw(key,value,client,InfoPack,haloprint=False):
   
    patchsize = InfoPack['PatchSize']
    datasize  = InfoPack['DataSize']
    patchnums = InfoPack['PatchNums']
    halo      = InfoPack['ImgHalo']

    path      = InfoPack['Path']
 

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

    blocknum = 4

    blocksize = numpy.array(patchsize)/blocknum 

    origin = numpy.array(data).astype(numpy.float32)


    for z in xrange(blocknum):
        for y in xrange(blocknum):
            for x in xrange(blocknum):
                z_str = z*blocksize[2]
                z_end = z_str + blocksize[2]+2
                y_str = y*blocksize[1]
                y_end = y_str + blocksize[1]+2
                x_str = x*blocksize[0]
                x_end = x_str + blocksize[0]+2
                 
                g = origin[z_str:z_end,y_str:y_end,x_str:x_end]

                a = numpy.roll(g, 1,axis=0)
                b = numpy.roll(g,-1,axis=0)
                c = numpy.roll(g, 1,axis=1)
                d = numpy.roll(g,-1,axis=1)
                e = numpy.roll(g, 1,axis=2)
                f = numpy.roll(g,-1,axis=2)

                data[z_str+1:z_end-1,y_str+1:y_end-1,x_str+1:x_end-1] = (g + (a+b+c+d+e+f-6.0*g)/4.0)[1:-1,1:-1,1:-1]
   
    t1 = time.time()
    print "%05d Heat Flow  %s"%(os.getpid(), t1-t0)

    #return (name,data.astype(numpy.float32))
    return (name,data)



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
    IterNum   = 5
    #ImageName = 'Heatflow_16G'
    ImageName = 'Heatflow_8G'
    #ImageName = 'Heatflow_16G_256'
    #ImageName = 'Heatflow_4G'
 
    #ImageName = 'Heatflow_16blk_4G'
    ImagePath = 'hdfs://emerald1:9000/user/' + username + '/' + ImageName + '/'
    client = InsecureClient('http://emerald1:50070',user=username)

    #ImgHalo = 20
    #kernel = 5 if len(sys.argv) < 2 else int(sys.argv[1])

    ImgHalo = 1
    #ImgHalo = 0

    sc = SparkContext(appName="Heatflow_Spark_Patch")
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

    #ImagePatch = sc.binaryFiles(ImagePath,serializer="msgpack")
    #ImagePatch = sc.binaryFiles(ImagePath,serializer="pickle")
    ImagePatch = sc.binaryFiles(ImagePath)
  
    # Image  
    InfoPack = {}
    InfoPack['PatchSize'] = PatchDim
    InfoPack['PatchNums'] = ImgSplit
    InfoPack['ImgHalo']   = ImgHalo
    InfoPack['DataSize']  = DataDim
    InfoPack['Path'] = ImagePath
    print InfoPack

    ImagePatch = ImagePatch.map(lambda (name,data): init_raw(name,data,InfoPack)).persist()
  
    for ii in range(IterNum):
        # halo comm
        ImageHalos = ImagePatch.flatMap(lambda (name,data): genhalo3d(name,data,InfoPack))

        # dummy halo comm
        #ImageHalos = ImagePatch.flatMap(lambda (name,data): fakegenhalo3d(name,data,InfoPack))

        Images = ImageHalos.reduceByKey(lambda x,y: keysort(x,y))
        ImagePatch = Images.map(lambda (key,data): attachHalo3d(key,data,InfoPack))
            
        # execution
        ImagePatch = ImagePatch.map(lambda (key,data): heatflow3d(key,data,InfoPack))

    ImagePatch.foreach(lambda (key,data): writeRaw(key,data,"test",InfoPack,True))

