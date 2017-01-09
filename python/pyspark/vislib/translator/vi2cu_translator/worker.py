

code = """
def mip(volume, x, y):
    step = 1.0
    line_iter = orthogonal_iter(volume, x, y, step)

    max = 0
    for elem  in line_iter:
        val = point_query_3d(volume, elem)
        if max < val:
            max = val

    z_val = line_iter.get_depth()
    
    return ((x,y), (z_val, max))
"""
import numpy
import pycuda.driver as cuda
from pycuda.compiler import SourceModule


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

func_dict = get_function_dict(code)

from main import vi2cu_translator
#print func_dict['mip'][func_dict['mip'].strip().find('(')+1:func_dict['mip'].strip().find(',')]

cuda_function_code, _, __, ___ = vi2cu_translator(vivaldi_code=func_dict['mip'], local_dict={'volume': 'short_volume', 'x': 'int', 'y': 'int'})

#print _

from translator.common import load_GPU_attachment
attachment = load_GPU_attachment()


kernel_code = attachment + 'extern "C"{\n'                                                                    
kernel_code += cuda_function_code
kernel_code += '\n}'


#open("asdf.cu", 'w').write(kernel_code)

print cuda_function_code
