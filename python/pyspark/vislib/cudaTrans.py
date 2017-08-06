
#from worker_assist import *
import numpy

from translator.vi2cu_translator.main import vi2cu_translator
from translator.common import load_GPU_attachment

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
    

def create_local_dict(local_code, iter_type, arg_type):
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

    print func_args_list

    print arg_type   
 
    #indata_meta.data_kind = iter
    #meta_list = [indata_meta] + other_meta
    #for idx in range(len(meta_list)):
    for idx in range(len(arg_type)):
        #elem = args['func_args'][target_id][idx+1]
        #elem = meta_list[idx]
        elem = arg_type[idx]

        """ 
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
        """ 

        if elem == iter:
            #tmp_dict = {'%s'%(func_args_list[idx]):'%s_volume'%(elem.data_type)}
            tmp_dict = {'%s'%(func_args_list[idx]):'%s_volume'%(iter_type)}
        elif elem in [list, numpy.ndarray]:
            tmp_dict[func_args_list[idx]] = '%s*'%(elem.data_type)
        else:
            if elem is float:
                tmp_dict[func_args_list[idx]] = 'float'
            elif elem is int:
                tmp_dict[func_args_list[idx]] = 'int'

    print tmp_dict

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


def translator(code="",function_name="",function_dtype='float',arg_type=[]):
    func_dict = get_function_dict(code)

    from translator.vi2cu_translator.main import vi2cu_translator
    from translator.common import load_GPU_attachment

    local_dict = {}
    
    #print code
    #print func_dict

    #print vm_indata
    #print vm_local
    #print target_id
    #print fuct
    
    if True:
        vm_indata={}
        vm_local={}
        target_id=0

        local_dict=create_local_dict(func_dict[function_name].strip(), function_dtype,arg_type)

    cuda_function_code, comma_cnt, result_class, lambda_func = vi2cu_translator(vivaldi_code=func_dict[function_name], local_dict=local_dict)
    
    attachment = load_GPU_attachment()

    kernel_code = attachment + result_class + 'extern "C"{\n'                                                                    
    kernel_code += cuda_function_code
    kernel_code += '\n}'

    function_name = str(function_name+function_dtype)

    return kernel_code , comma_cnt, result_class , lambda_func


if __name__ == "__main__":

    code ="""
    def heatflow(data, x, y, z, arg1, arg2):

    a = point_query_3d(data, x+1, y+0, z+0)
    b = point_query_3d(data, x-1, y+0, z+0)
    c = point_query_3d(data, x+0, y+1, z+0)
    d = point_query_3d(data, x+0, y-1, z+0)
    e = point_query_3d(data, x+0, y+0, z+1)
    f = point_query_3d(data, x+0, y+0, z-1)

    center =  point_query_3d(data, x+0, y+0, z+0)

    dt =  0.25
    result = center +  dt*(a+b+c+d+e+f - 6.0 * center)

    return result
    """
    
    function_name = "heatflow"
    data_type=[numpy.int32,iter,iter,float,float] 
   
    kernel_code , comma_cnt, result_class, lambda_func= translator(code,function_name,arg_type=data_type)

    print "CODE = ", kernel_code[135100:]
    print "Comma Count = ", comma_cnt
    print "Result Class = ", result_class
    print "lambda_func = ", lambda_func
