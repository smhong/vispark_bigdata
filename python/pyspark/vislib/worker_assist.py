"""
Worker that receives input from Piped RDD.
"""
import os
import sys
import time
import itertools
import numpy

import time 
from socket import *

    
# local functions
def get_function_dict(x):
    function_code_dict = {}
    def get_function_name_list(code=''):
        def get_head(code='', st=0):
            idx = code.find('def ', st)
            if idx == -1: return None, -1
            idx2 = code.find(':', idx+1)
            head = code[idx+4:idx2]
            head = head.strip()
            return head, idx2+1 
        def get_function_name(head=''):
            idx = head.find('(')
            return head[:idx]
                            
        function_name_list = []
        st = 0
        while True:
            head, st = get_head(code, st)
            if head == None: break
            function_name = get_function_name(head)
            function_name_list.append(function_name)
        return function_name_list   

    def get_code(function_name='', code=''):
        if function_name == '':
            print "No function name found"
            assert(False)
        # initialization
        ###################################################
        st = 'def '+function_name+'('
        output = ''
        s_idx = code.find(st)
        
        if s_idx == -1:
            print "Error"
            print "Cannot find the function"
            print "Function want to find:", function_name
            assert(False)
        n = len(code)
        
        def get_indent(line): 
            s_line = line.strip()
            i_idx = line.find(s_line)
            indent = line[:i_idx]

            return indent
            
        line = ''
        cnt = 1
        i = s_idx
        while i < n:
            w = code[i]
            
            line += w
            if w == '\n':
                indent = get_indent(line)       
                if indent == '' and (line.strip().startswith('def') or line.replace(' ','').strip().startswith('if__name__==\"__main__\"')):
                    # ex2 and ex3
                    if cnt == 0:break
                    cnt -= 1
            
                output += line
                line = ''
            i += 1
    
        return output
 
    function_name_list = get_function_name_list(x)
    for function_name in function_name_list:
        function_code = get_code(function_name=function_name, code=x)
        function_code_dict[function_name] = function_code
    return function_code_dict
    

def get_data_shape(target_id, data_info):
    data_shape = data_info['full_data_range']
    split      = data_info['split']
    halo       = data_info['halo']

    #
    # work_range is same with data_range
    # Implementation is required
    #
    #work_range = {}                                                                                           
    #for elem in data_range:                                                                             
        #work_range[elem] = [0, int(data_shape[elem][1])]                                                   

    
    split_position = id_to_split(target_id, split)

    x = split_position['x'] if 'x' in split else -1
    y = split_position['y'] if 'y' in split else -1
    z = split_position['z'] if 'z' in split else -1

    return generate_data_shape(data_shape, split, x, y, z, halo)[0]
        




def generate_vm(indata_info, args_dict):
    pass

def numpy_dtype_to_vispark_dtype(dtype, channel=''):
    if dtype == numpy.float32:
        return 'float%s'%channel
    if dtype == numpy.int32:
        return 'int%s'%channel
    if dtype == numpy.uint8:
        return 'uchar%s'%channel
    if dtype == numpy.uint16:
        return 'ushort%s'%channel

    return dtype
    
    

   
def create_local_dict(local_code, indata_meta, other_meta, target_id, iter_type):
    #first_args = local_code[local_code.find('(')+1:local_code.find(',')]
    local_args = local_code[local_code.find('(')+1:local_code.find(')')].replace(' ','')
    func_args_list = []
    while True:
        loc_comma = local_args.find(',')
        if loc_comma == -1:
            func_args_list.append(local_args)
            break;
        
        func_args_list.append(local_args[:loc_comma])
        local_args = local_args[loc_comma+1:]
    tmp_dict = {}

    
    indata_meta.data_kind = iter
    meta_list = [indata_meta] + other_meta
    for idx in range(len(meta_list)):
        #elem = args['func_args'][target_id][idx+1]
        elem = meta_list[idx]

        if elem.data_kind == iter:
            #tmp_dict = {'%s'%(func_args_list[idx]):'%s_volume'%(elem.data_type)}
            tmp_dict = {'%s'%(func_args_list[idx]):'%s_volume'%(iter_type)}
        elif elem.data_kind in [list, numpy.ndarray]:
            tmp_dict[func_args_list[idx]] = '%s*'%(elem.data_type)
        else:
            if elem.data_type is numpy.float32:
                tmp_dict[func_args_list[idx]] = 'float'
            elif elem.data_type is numpy.int32:
                tmp_dict[func_args_list[idx]] = 'int'

    #print tmp_dict

    lc = local_code[local_code.find('(')+1:]
    lc = lc[lc.find(',')+1:]
    flag = True
    while(flag):
        end = lc.find(',')
        if lc.find(',') == -1 or lc.find(')') < lc.find(','):
            end = lc.find(')')
            flag = False
        tmp_arg = lc[1:end]
        if tmp_arg not in tmp_dict:
            tmp_dict[tmp_arg] = 'float'
        lc = lc[lc.find(',')+1:]
    
    return tmp_dict

def make_cuda_list(rg, size=-1, order=False):
    AXIS = ['x','y','z','w']
    temp = []
    if size == -1: size = len(rg)
    if order == True: LT = AXIS[0:size]
    else: LT = AXIS[0:size][::-1]
    for axis in LT:
        if axis in rg:
            temp.append(numpy.int32(rg[axis][0]))
            temp.append(numpy.int32(rg[axis][1]))
        else:
            temp.append(numpy.int32(0))
            temp.append(numpy.int32(0))
    return temp

 
 
def data_range_to_cuda_in(data_range, full_data_range, buffer_range, data_halo=0, buffer_halo=0, stream=None, flag='input', cuda=None):
    #   global a
    a = numpy.empty((26), dtype=numpy.int32)

    cnt = 0
    for elem in ['x','y','z','w']:
        if elem in data_range:
            #a[cnt]   = numpy.int32(data_range[elem][0])
            #a[cnt+4] = numpy.int32(data_range[elem][1])
            if type(data_range[elem]) == list:
                a[cnt]   = numpy.int32(data_range[elem][0])
                a[cnt+4] = numpy.int32(data_range[elem][1])
            else:
                a[cnt]   = numpy.int32(0)
                a[cnt+4] = numpy.int32(data_range[elem])
        else:
            a[cnt]   = numpy.int32(0)
            a[cnt+4] = numpy.int32(0)
        cnt += 1

    cnt += 4
    for elem in ['x','y','z','w']:
        if elem in full_data_range:
            if type(full_data_range[elem]) == list:
                a[cnt]   = numpy.int32(full_data_range[elem][0])
                a[cnt+4] = numpy.int32(full_data_range[elem][1])
            else:
                a[cnt]   = numpy.int32(0)
                a[cnt+4] = numpy.int32(full_data_range[elem])
        else:
            a[cnt]   = numpy.int32(0)
            a[cnt+4] = numpy.int32(0)
        cnt += 1
    cnt += 4
    for elem in ['x','y','z','w']:
        if elem in buffer_range:
            #a[cnt] = numpy.int32(buffer_range[elem][0])
            #a[cnt+4] = numpy.int32(buffer_range[elem][1])
            if type(buffer_range[elem]) == list:
                a[cnt]   = numpy.int32(buffer_range[elem][0])
                a[cnt+4] = numpy.int32(buffer_range[elem][1])
            else:
                a[cnt]   = numpy.int32(0)
                a[cnt+4] = numpy.int32(buffer_range[elem])
        else:
            a[cnt] = numpy.int32(0)
            a[cnt+4] = numpy.int32(0)
        cnt += 1
    cnt += 4
    a[cnt] = numpy.int32(data_halo)
    a[cnt+1] = numpy.int32(data_halo)

    return cuda.In(a)              
                

def shape_tuple_or_list_to_dict(shape):
    AXIS = ['z', 'y', 'x']
    AXIS = AXIS[-len(shape):]
    ret_dict = {}

    tmp_shape = shape if shape[-1] > 4 else shape[:-1]
    
    print tmp_shape

    for elem in AXIS:
        if elem == 'x':
            split_idx = -1
        elif elem == 'y':
            split_idx = -2
        elif elem == 'z':
            split_idx = -3
        ret_dict[elem] = [0, tmp_shape[split_idx]]

    return ret_dict

 
def reshape_str(data, data_shape, loc, halo, full_data_shape):

    reshape_size = []
    AXIS = ['z','y','x']
    for axis in AXIS:
        if axis in loc:
            target = loc[loc.find(axis)+1] 
            if target in ['-']:
                pass
            elif target in ['u','d']:
                reshape_size.append(halo)
            else:
                start = data_shape[axis][0]
                end   = data_shape[axis][1]
                if start != 0:
                    if end < full_data_shape[axis]:
                        reshape_size.append(end-start-2*halo)
                    else:
                        reshape_size.append(end-start-halo)
                else:
                    reshape_size.append(end-halo)
    # FFLOAT case
    #print reshape_size

    #print "DATA_SHSDLKFJSDKLFJSLHAPE", data_shape
    #print "LEN : %s, reshape_size : %s"%(len(data), str(reshape_size))
    try:
        data = numpy.fromstring(data, dtype=numpy.uint8).reshape(reshape_size)
    except:
        data = numpy.fromstring(data, dtype=numpy.float32).reshape(reshape_size)
    return data

def reorganize(ori_data, shape, full_data_shape, data_halo):

    array_buffer_size = []
    for axis in ['z','y','x']:
        if axis in shape:
            start = shape[axis][0]
            end   = shape[axis][1]
            array_buffer_size.append(end-start)
   
    #print array_buffer_size, ori_data.shape
    if len(array_buffer_size) < len(list(ori_data.shape)):
        array_buffer_size.append(ori_data.shape[-1])


    if array_buffer_size == list(ori_data.shape):
        return ori_data

    new_array = numpy.zeros(array_buffer_size, dtype=ori_data.dtype)


    array_copy_str = 'new_array['
    for axis in ['z','y','x']:
        if axis in shape:
            start = shape[axis][0]
            end   = shape[axis][1]
 
            if end < full_data_shape[axis]:
                end = end - start - data_halo
            else:
                end = end - start

            if start is not 0:
                start = data_halo
            
            array_copy_str += '%d:%d,'%(start, end)
 
    array_copy_str += ']=ori_data'

    #print array_copy_str, ori_data.shape

    exec array_copy_str in globals(), locals()

    
    return new_array
 
def attach_halo(ori_data, halo_data, shape, loc, halo, full_data_shape):

    ori_data = reorganize(ori_data, shape, full_data_shape, halo)

    attach_str="ori_data["
    AXIS=['z','y','x']
    for local_elem in AXIS:
        if local_elem in loc:
            target = loc[loc.find(local_elem)+1] 
            if target in ['-']:
                pass
            elif target in ['u','d']:
                start = shape[local_elem][0]
                end   = shape[local_elem][1]
                end = end - start
                if start is not 0:
                    start = halo
    
                if target == 'd':
                    attach_str+="%d:,"%(end-halo)
                elif target == 'u':
                    attach_str+=":%d,"%halo
            else:
                start = shape[local_elem][0]
                end   = shape[local_elem][1]
    
                #print local_elem, start, end, halo
                if start != 0:
                    if end < full_data_shape[local_elem]:
                        attach_str += "%d:%d,"%(halo,end-start-halo)
                    else:
                        attach_str += "%d:%d,"%(halo,end-start)
                else:
                    attach_str += ":%d,"%(end-halo)

    attach_str += "] = halo_data"
    exec attach_str in globals(), locals()
    return ori_data


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

def dr_dict_to_list(in_dict):
    ret_list = []
    for axis in ['x', 'y', 'z']:
        if axis in in_dict:
            ret_list.append(in_dict[axis][1])
        else:
            ret_list.append(0);

    return numpy.array(ret_list, dtype=numpy.uint32)


def create_helper_textures():
    def create_2d_texture(a, module, variable, point_sampling=False):                                                                               
        a = numpy.ascontiguousarray(a)
        out_texref = module.get_texref(variable)
        cuda.matrix_to_texref(a, out_texref, order='C')
        if point_sampling: out_texref.set_filter_mode(cuda.filter_mode.POINT)
        else: out_texref.set_filter_mode(cuda.filter_mode.LINEAR)
        return out_texref

                
    def create_2d_rgba_texture(a, module, variable, point_sampling=False): 
        a = numpy.ascontiguousarray(a) 
        out_texref = module.get_texref(variable) 
        cuda.bind_array_to_texref( 
            cuda.make_multichannel_2d_array(a, order='C'), out_texref)     
        if point_sampling: out_texref.set_filter_mode(cuda.filter_mode.POINT) 
        else: out_texref.set_filter_mode(cuda.filter_mode.LINEAR) 
        return out_texref 
 
    def update_random_texture(random_texture):                                                      
        tmp = numpy.uint32(numpy.random.rand(256, 256) * (2 << 30))
        update_2d_texture(random_texture, tmp)


    def update_2d_texture(texref, newdata):
        arr = texref.get_array()
        newdata = numpy.ascontiguousarray(newdata)
        h, w = newdata.shape
                        
        desc = arr.get_descriptor()
        assert h == desc.height and w == desc.width
        assert desc.num_channels == 1
                        
        copy = cuda.Memcpy2D()
        copy.set_src_host(newdata)
        copy.set_dst_array(arr)
        copy.width_in_bytes = copy.src_pitch = newdata.strides[0]
        copy.src_height = copy.height = h
        copy(True)


    def hg(a):
        a2 = a * a
        a3 = a2 * a
        w0 = (-a3 + 3*a2 - 3*a + 1) / 6                                                                                            
        w1 = (3*a3 - 6*a2 + 4) / 6
        w2 = (-3*a3 + 3*a2 + 3*a + 1) / 6
        w3 = a3 / 6
        g = w2 + w3
        h0 = 1 - w1 / (w0 + w1) + a
        h1 = 1 + w3 / (w2 + w3) - a
        return h0, h1, g, 0
                
    def dhg(a):
        a2 = a * a
        w0 = (-a2 + 2*a - 1) / 2
        w1 = (3*a2 - 4*a) / 2
        w2 = (-3*a2 + 2*a + 1) / 2
        w3 = a2 / 2
        g = w2 + w3
        h0 = 1 - w1 / (w0 + w1) + a
        h1 = 1 + w3 / (w2 + w3) - a
        return h0, h1, g, 0
                
    tmp = numpy.zeros((256, 4), dtype=numpy.float32)
    for i, x in enumerate(numpy.linspace(0, 1, 256)):
        tmp[i, :] = numpy.array(hg(x))

    tmp = numpy.reshape(tmp, (1, 256, 4))
                
    hg_texture = create_2d_rgba_texture(tmp, mod, 'hgTexture')
    tmp = numpy.zeros((256, 4), dtype=numpy.float32)
    for i, x in enumerate(numpy.linspace(0, 1, 256)):
        tmp[i, :] = numpy.array(dhg(x))
    tmp = numpy.reshape(tmp, (1, 256, 4))
    dhg_texture = create_2d_rgba_texture(tmp, mod, 'dhgTexture')

    tmp = numpy.zeros((256, 256), dtype=numpy.uint32) # uint32(rand(256, 256) * (2 << 30))
    random_texture = create_2d_texture(tmp, mod, 'randomTexture', True)
    update_random_texture(random_texture)
            
    # to prevent GC from destroying the textures
    dummy = (hg_texture, dhg_texture)

def array_param_to_work_range(param):
    cnt = param.count(':')
    AXIS = ['x', 'y', 'z', 'w']
    AXIS = AXIS[:cnt][::-1]

    
    for elem in range(len(AXIS)):
        target = AXIS[elem]
        pass

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


def generate_data_shape(target_id, full_data_shape, data_split, Halo):

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


    ds = {}
    split_shape = {}
    step = 0

    #print "FROM GENERATE DATASHAPE", full_data_shape, data_split, x, y, z, Halo

    if z == -1:
        pass
    else:
        step = 'z'
        _z = z*full_data_shape[step]/data_split[step]
        z_start = _z - Halo if _z - Halo > 0 else 0
        z_end   = _z + full_data_shape[step]/data_split[step] + Halo if _z + full_data_shape[step]/data_split[step] + Halo < full_data_shape[step] else full_data_shape[step];

        ds['z'] = [z_start, z_end]
        split_shape['z'] = z+1
                    
    if y == -1:
        pass
    else:
        step = 'y'
        _y = y*full_data_shape[step]/data_split[step]
        y_start = _y - Halo if _y - Halo > 0 else 0
        y_end   = _y + full_data_shape[step]/data_split[step] + Halo if _y + full_data_shape[step]/data_split[step] + Halo < full_data_shape[step] else full_data_shape[step];

        ds['y'] = [y_start, y_end]
        split_shape['y'] = y+1
            

    if x == -1:
        pass
    else:
        step = 'x'
        _x = x*full_data_shape[step]/data_split[step]
        x_start = _x - Halo if _x - Halo > 0 else 0
        x_end   = _x + full_data_shape[step]/data_split[step] + Halo if _x + full_data_shape[step]/data_split[step] + Halo < full_data_shape[step] else full_data_shape[step];

        ds['x'] = [x_start, x_end]
        split_shape['x'] = x+1

    return ds, split_shape

# change target_id to local_split
def id_to_split(target_id, split):
    local_target_id = int(target_id)

    ret_split = {}
    div = {}
    if 'z' in split:
        div['z'] = split['y'] * split['x']
    if 'y' in split:
        div['y'] = split['x']
            

    AXIS = ['z', 'y', 'x']
    for elem in AXIS:
        if elem not in split:
            continue

        if elem not in div:
            ret_split[elem] = local_target_id
            continue
        ret_split[elem] = local_target_id / div[elem]
        local_target_id = local_target_id % div[elem]


    return ret_split


def get_block_grid(wr):
    block = [1,1,1]
    grid  = [1,1,1]

    block_unit = 8
    if 'z' in wr:
        block_unit = block_unit * 2
 
    if 'x' in wr:
        if (wr['x'][1] - wr['x'][0]) > block_unit:
            block[0] = block_unit
            grid[0] = ((wr['x'][1] - wr['x'][0])/block_unit+1)
        else :
            block[0] = (wr['x'][1] - wr['x'][0])
    if 'y' in wr:
        if (wr['y'][1] - wr['y'][0]) > block_unit:
            block[1] = block_unit
            grid[1] = ((wr['y'][1] - wr['y'][0])/block_unit+1)
        else :
            block[1] = (wr['y'][1] - wr['y'][0])
    if 'z' in wr:
        if (wr['z'][1] - wr['z'][0]) > block_unit:
            block[2] = block_unit
            grid[2] = ((wr['z'][1] - wr['z'][0])/block_unit+1)
        else :
            block[2] = (wr['z'][1] - wr['z'][0])
                
        #print block, grid
    return tuple(block), tuple(grid)


