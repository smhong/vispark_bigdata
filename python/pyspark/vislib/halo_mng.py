import numpy


def shape_tuple_or_list_to_dict(shape):
    AXIS = ['z', 'y', 'x']
    AXIS = AXIS[-len(shape):]
    ret_dict = {}

    tmp_shape = shape if shape[-1] > 4 else shape[:-1]
    
    for elem in AXIS:
        if elem == 'x':
            split_idx = -1
        elif elem == 'y':
            split_idx = -2
        elif elem == 'z':
            split_idx = -3
        ret_dict[elem] = [0, tmp_shape[split_idx]]

    return ret_dict

def shape_dict_to_tuple(shape):
    AXIS = ['z', 'y', 'x']

    ret = [] 
   
    for elem in AXIS:
        if elem in shape:
            ret.append(shape[elem])

    return tuple(ret)
    

def generate_data_split(target_id, data_split):
    if 'z' not in data_split:
        if 'y' not in data_split:
            ## 1D case
            z = -1
            y = -1
            x = target_id
        ## 2D case
        else:
            z = -1
            y = target_id / data_split['x']
            x = target_id % data_split['x']
    ## 3D case
    else:
        z = target_id  /(data_split['y'] *data_split['x'])
        y = (target_id / data_split['x'])%data_split['y']
        x = target_id  % data_split['x']

    split_shape = {'x':x+1, 'y':y+1, 'z':z+1}

    return split_shape


def check_in_area(full_data_shape, data_range, x, y, z, halo, itertype,comm_type='full', ):
    # test all center
    test_buffer = []
    if x != '-':
        test_buffer.append(x)
    if y != '-':
        test_buffer.append(y)
    if z != '-':
        test_buffer.append(z)
    test_buffer = numpy.array(test_buffer)
    if (test_buffer == 'c').all():return None
        
    # ignore corners
    if comm_type != 'full':
        c_count = 0 
        for elem in test_buffer:
            if elem == 'c':
                c_count += 1

        # corner has many non_c values
        if c_count != len(test_buffer)-1:
            return None
    ret_str = ''

    def local_iterative_code(full_data_shape, data_range, target, axis, halo, itertype):
        if target == '-':
            return None

        #print "DATA SHAPE", data_shape
    
        start = data_range[axis][0]
        end   = data_range[axis][1]
       
        if itertype == 'write': 
            if target == 'u' and end < full_data_shape[axis]:
                return '-%s:,'%(halo)
            
            if target == 'c':
                return ':,'

            if target == 'd' and start > 0:
                return ':%s,'%(halo)
        
            return "OUT"

        # GPU halo extraction
        # It extracts 'real' data which locate at inside of halo
        # h h h h h h
        # h * * * * h
        # h *  *  * h
        # h *     * h
        # h * * * * h
        # h h h h h h
        if itertype == "advanced_write":
            local_axis_shape = data_range[axis][1] - data_range[axis][0]
            
            if target == 'u':
                return '\'%s\': [%s,%s],'%(axis, local_axis_shape-2*halo, local_axis_shape-halo)
            
            if target == 'c':
                return '\'%s\': [%s,%s],'%(axis, halo, local_axis_shape-halo)

            if target == 'd':
                return '\'%s\': [%s,%s],'%(axis, halo, halo*2)
        
            return "OUT"

 
        if itertype == 'read': 
            if target == 'd' and end < full_data_shape[axis]:
                return "True"
            
            if target == 'u' and start > 0:
                return "True"
        
            if target == 'c':
                return "True"

            return "OUT"


        # GPU halo append
        # It extracts 'real' data which locate at inside of halo
        # * * * * * *
        # *         *
        # *         *
        # *         *
        # *         *
        # * * * * * *
        if itertype == "advanced_read":
            local_axis_shape = data_range[axis][1] - data_range[axis][0]
            
            if target == 'd':
                return '\'%s\':[ %s,%s ],'%(axis, local_axis_shape-halo, local_axis_shape)
            
            if target == 'c':
                return '\'%s\': [%s,%s],'%(axis, halo, local_axis_shape-halo)

            if target == 'u':
                return '\'%s\':[%s,%s],'%(axis, 0, halo)
        
            return "OUT"

        

    z_str = local_iterative_code(full_data_shape, data_range, z, 'z', halo, itertype)
    y_str = local_iterative_code(full_data_shape, data_range, y, 'y', halo, itertype)
    x_str = local_iterative_code(full_data_shape, data_range, x, 'x', halo, itertype)
 
    if z_str == "OUT" or y_str == "OUT" or x_str =="OUT":
        return None
   
    ret_str += z_str if z_str != None else ''
    ret_str += y_str if y_str != None else ''
    ret_str += x_str if x_str != None else ''
        #print data_shape, x, y, z


    return ret_str


class HaloManager(object):
    
    numBlocks = 0
    listBlocks = {}
    #ImgDim == Full_data_shape
    ImgDim ={}
    ImgSplit ={}
    
    nameToidx ={}
    idxToname ={}

    inBoundDic={}
    outBoundDic={}

    def __init__(self,blocknames,full_data_shape,split_shape):
        
        full_data_shape = list(full_data_shape)   
        split_shape     = list(split_shape)   
     
        self.numBlocks = reduce(lambda x,y:x*y,split_shape)

        assert len(full_data_shape) == len(split_shape)
        assert len(blocknames) == self.numBlocks
        

        for i in range(len(full_data_shape)):
            if   i == 0 :
                self.ImgDim['x'] = int(full_data_shape[-1])
            elif i == 1 :
                self.ImgDim['y'] = int(full_data_shape[-2])
            elif i == 2 :
                self.ImgDim['z'] = int(full_data_shape[-3])
 
        for i in range(len(split_shape)):
            if   i == 0 :
                self.ImgSplit['x'] = int(split_shape[-1])
            elif i == 1 :
                self.ImgSplit['y'] = int(split_shape[-2])
            elif i == 2 :
                self.ImgSplit['z'] = int(split_shape[-3])
    

        self.matching_dict(blocknames)
        
        print self.ImgDim         
        print self.ImgSplit
        print self.numBlocks
   
        print self.nameToidx 
        print self.idxToname

    def matching_dict(self,blocknames):

        import re

        renamed=[]

        for i in range(len(blocknames)):
            renamed.append(re.sub('[^a-zA-Z0-9]', ' ', blocknames[i]))

        #print renamed

        nameToidx = {}
        idxToname = {}
        inBoundDic = {}
        outBoundDic = {}
        for i in range(len(renamed)):
            number = [str(int(s)) for s in renamed[i].split(' ') if renamed[(i+1)%len(renamed)].rfind(s) != renamed[i].rfind(s)][0]

            idxToname[number] = blocknames[i]
            nameToidx[blocknames[i]] = number

            inBoundDic[number] = []
            outBoundDic[number] = []

        

        self.nameToidx = nameToidx
        self.idxToname = idxToname
        self.inBoundDic= inBoundDic
        self.outBoundDic = outBoundDic

        #print self.nameToidx
        #print self.idxToname

    def append_halo_info(self,data_halo=1,comm_type='full'):
        ######################################################
        #Prepare (CUDA function)
    
        numBlocks = self.numBlocks
        ImgSplit  = self.ImgSplit

        FullDataShape = shape_dict_to_tuple(self.ImgDim)
        LocalDataShape = map(lambda x,y:x/y,FullDataShape,shape_dict_to_tuple(ImgSplit))

        ImgDim    = shape_tuple_or_list_to_dict(LocalDataShape)

        #print ImgDim 

        for i in range(numBlocks):
            target_id = i

            input_data_split = generate_data_split(target_id, ImgSplit)
      
            #print input_data_split
 
            neighbor_indi_z = ['u','c','d'] if 'z' in ImgDim else ['-']
            neighbor_indi_y = ['u','c','d'] if 'y' in ImgDim else ['-']
            neighbor_indi_x = ['u','c','d'] if 'x' in ImgDim else ['-']
            split_pattern = {'u':-1, 'c':0, 'd':1}

            #print neighbor_indi_x
            #print neighbor_indi_y
            #print neighbor_indi_z

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
            
                            self.inBoundDic["%s"%i].append([target_split,target_loc,target_key]) 
                        
    def extract_halo_info(self,data_halo=1,comm_type='full'):

        numBlocks = self.numBlocks
        ImgSplit  = self.ImgSplit

        FullDataShape = shape_dict_to_tuple(self.ImgDim)
        LocalDataShape = map(lambda x,y:x/y,FullDataShape,shape_dict_to_tuple(ImgSplit))

        ImgDim    = shape_tuple_or_list_to_dict(LocalDataShape)


        #ImgDim      = vm_indata.data_shape
        #ImgSplit    = args_dict['split']
        #data_halo   = args_dict['halo']


        for i in range(numBlocks):
            target_id = i

            input_data_split = generate_data_split(target_id, ImgSplit)

            comm_type='full'

            neighbor_indi_z = ['u','c','d'] if 'z' in ImgDim else ['-']
            neighbor_indi_y = ['u','c','d'] if 'y' in ImgDim else ['-']
            neighbor_indi_x = ['u','c','d'] if 'x' in ImgDim else ['-']

  
            for _z in neighbor_indi_z:
                for _y in neighbor_indi_y:
                    for _x in neighbor_indi_x:
        
                        ret = check_in_area(ImgDim, ImgDim, _x, _y, _z, data_halo, 'advanced_write',comm_type)
    
                        if ret != None:

                            target_split = input_data_split
                            target_key = 'origin'
                            target_loc = 'z%sy%sx%s'%(_z,_y,_x)
                            self.outBoundDic["%s"%i].append([target_split,target_loc,target_key,ret]) 

        #print data        
        #print len(data)


    def inbound(self,key):
        return self.inBoundDic[self.nameToidx[key]]

    def outbound(self,key):
        return self.outBoundDic[self.nameToidx[key]]



if __name__ == "__main__":

    filename = ["hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_000.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_001.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_002.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_003.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_004.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_005.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_006.raw",
                "hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_007.raw"]

    ImgDim = (512,512,512)
    ImgSplit = (2,2,2)
    HaloSize = 1

    mng = HaloManager(filename,ImgDim,ImgSplit)

    mng.extract_halo_info(HaloSize)
    mng.append_halo_info(HaloSize)

    #print map(lambda key:mng.outbound(key),filename)
    print mng.inbound(filename[2])
    print mng.outbound(filename[2])

    #print mng.outbound("hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_006.raw")
    #print mng.inbound("hdfs://10.20.15.200:9000/whchoi/data_1024_1024_1024/block_006.raw")

 
