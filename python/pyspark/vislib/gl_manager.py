# NEW PROJECT
# GL RENDERING MANAGER WITH VISPARK

import time
import math
import sys
from socket import *
import subprocess

import os
import numpy

SPARK_HOME=os.environ["SPARK_HOME"]
PYSPARK_PATH = "%s/python/"%SPARK_HOME
PY4J_PATH ="%s/python/lib/py4j-0.10.4-src.zip"%SPARK_HOME

if PYSPARK_PATH not in sys.path:
    sys.path.append(PYSPARK_PATH)
if PY4J_PATH not in sys.path:
    sys.path.append(PY4J_PATH)

server_list={}
server_list_file=sys.argv[1]
server_size = 0
server_rank = -1
host_name =sys.argv[2]
#verify = int(sys.argv[3])
pid_path = sys.argv[3]
pid = os.getpid()
log_path = "/tmp/vispark_worker_%d"%(pid)



port = int(4950)
svrsock = socket(AF_INET, SOCK_STREAM)
svrsock.bind((host_name, port))
svrsock.listen(64)


# GLOBAL VALUES
loop  = True
alive = False
POLICY = 1
num_recv = 0
num_send =0

with open(server_list_file) as f:
    lines = f.readlines()

    for elem in lines:
        elem = elem.strip()

        if elem.find('#')==0:
            continue
        if elem.find('\n')!=-1: 
            elem = elem[:-2]
       
        if len(elem) > 0:
            #elem = elem.replace('ib','emerald')
            
            if elem == host_name:
                server_rank = server_size

            server_list[elem] = server_size
            server_size += 1


print server_list , host_name , server_rank


def get_proc_list():
    procs = []

    #command = "ps -eo pid,command | grep gpu_manager | grep mpirun"
    command = "ps -eo pid,command | grep gl_manager | grep python"
    output = subprocess.check_output(command,shell=True)
    #print output

    lists = [elem for elem in output.split('\n')][:-1]
   
    lists = [int(elem[:5]) for elem in lists if elem.find('ps') == -1 and elem.find('mpirun') == -1]

    return lists

#host_name = gethostname()
while False:
    cnt = 0
    
    isRunning = get_proc_list()
    
    for elem in isRunning:
        if pid != elem :
            cnt+=1

    if cnt > 0:
        print "[%s:%d] Another GL manager is working"%(host_name,pid), isRunning 
        time.sleep(3)
    else: 
        break



#from gpu_worker import gpu_run, gpu_htod, gpu_dtoh, \
from gl_worker import *


class bcolors:
    HEADER = '\033[95m'
    OKBLUE = '\033[94m'
    OKGREEN = '\033[92m'
    WARNING = '\033[93m'
    FAIL = '\033[91m'
    ENDC = '\033[0m'
    BOLD = '\033[1m'
    UNDERLINE = '\033[4m'


def print_green(source):
    if logging:
        with open(log_path,'a') as f:
            f.write(source)


def print_(source):
    if logging:
        print source, host_name
        with open(log_path,'a') as f:
            f.write(source)


def print_red(source):
    if logging:
        print bcolors.FAIL,source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


def print_blue(source):
    if logging:
        print bcolors.OKBLUE,source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


def print_bblue(source):
    if logging:
        print bcolors.BOLD, bcolors.OKBLUE,source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


def print_yellow(source):
    if logging:
        print bcolors.WARNING,source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


def print_purple(source):
    if logging:
        print bcolors.HEADER,source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


def print_bold(source):
    if logging:
        print bcolors.BOLD,source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


def print_bred(source):
    if logging:
        print bcolors.BOLD, bcolors.FAIL, source, host_name, bcolors.ENDC
        with open(log_path,'a') as f:
            f.write(source)


data_dict = {}
args_dict = {}

total_comm_time = 0.0

import os, sys


import OpenGL
OpenGL.ERROR_CHECKING = False
from OpenGL.GLUT import *
from OpenGL.GLU import *
from OpenGL.GL import *
from OpenGL.GLX import *
from OpenGL.arrays import ArrayDatatype as ADT
import OpenGL.arrays.vbo as glvbo
from OpenGL.GL.ARB.geometry_shader4 import *
from OpenGL.GL.EXT.geometry_shader4 import *

import time, math, numpy
from Buffer import*
from ShaderInitializer import *

import inspect
INS = inspect.currentframe()

# X Opengl
from Xlib import X, display
pd = display.Display()
pw = pd.screen().root.create_window(50, 50, 1, 1, 0, pd.screen().root_depth, X.InputOutput, X.CopyFromParent)

pw.map()
# ensure that the XID is valid on the server
pd.sync()

# get the window XID
xid = pw.__resource__()

# a separate ctypes Display object for OpenGL.GLX
xlib = cdll.LoadLibrary('libX11.so')
xlib.XOpenDisplay.argtypes = [c_char_p]
xlib.XOpenDisplay.restype = POINTER(struct__XDisplay)
d = xlib.XOpenDisplay(None)

from ctypes import *

count = 0


class Render:

    def __init__(self, name, windowSize):
        self.ScreenWidth = windowSize[0]
        self.ScreenHeight = windowSize[1]

        self.clisock = None
        self.addr    = None


        glutInit(sys.argv)
        glutSetOption(GLUT_MULTISAMPLE, 8);
        glutInitDisplayMode(GLUT_RGBA | GLUT_DOUBLE | GLUT_DEPTH | GLUT_MULTISAMPLE)
        glutInitWindowSize(1, 1)

        elements = c_int()
        configs = glXChooseFBConfig(d, 0, None, byref(elements))

        #glXCreateContext(disp, configs, None, False)
        w = glXCreateWindow(d, configs[0], c_ulong(xid), None)
        context = glXCreateNewContext(d, configs[0], GLX_RGBA_TYPE, None, True)
        glXMakeContextCurrent(d, w, w, context)
        glutCreateWindow(None)
        glutDisplayFunc(self.handlerIdle)
        glutReshapeFunc(self.handlerReshape)
        glutIdleFunc(self.handlerDisplay)
        self.initializeGL()

    def initializeGL(self):
        glShadeModel(GL_SMOOTH)
        glPixelStorei(GL_UNPACK_ALIGNMENT, 4)
        #glEnable(GL_ALPHA_TEST)
        #glAlphaFunc(GL_GREATER, 0.8)
        glEnable(GL_DEPTH_TEST)
        glEnable(GL_LINE_SMOOTH)
        glEnable(GL_POINT_SMOOTH)
        glEnable(GL_POLYGON_SMOOTH)

        glEnable(GL_LIGHTING)

        glEnable(GL_VERTEX_ARRAY)
        glEnable(GL_COLOR_ARRAY)
        glEnable(GL_NORMALIZE)

        glEnable(GL_TEXTURE_2D)
        glEnable(GL_CULL_FACE)
        glEnable(GL_BLEND)
        glBlendFunc(GL_SRC_ALPHA, GL_ONE_MINUS_SRC_ALPHA)
        #glBlendFunc(GL_ONE_MINUS_DST_ALPHA, GL_ONE);

        glEnable(GL_MULTISAMPLE)
        glEnable(GL_COLOR_MATERIAL)
        #glColorMaterial(GL_FRONT_AND_BACK, GL_AMBIENT_AND_DIFFUSE)

        glHint(GL_PERSPECTIVE_CORRECTION_HINT, GL_NICEST)
        glHint(GL_LINE_SMOOTH_HINT, GL_NICEST)
        glHint(GL_POLYGON_SMOOTH_HINT, GL_NICEST)
    
        

        glClearColor(0, 0, 0, 0)
        glClearStencil(0)      
        glClearDepth(1.0)     

        self.ModelViewMatrix = [1.,0.,0.,0.,0.,1.,0.,0.,0.,0.,1.,0.,0.,0.,0.,1.]
        self.ProjectionMatrix = [1.,0.,0.,0.,0.,1.,0.,0.,0.,0.,1.,0.,0.,0.,0.,1.]

        self.ModelViewMatrix = numpy.array(self.ModelViewMatrix, dtype=numpy.float32)
        self.ProjectionMatrix = numpy.array(self.ProjectionMatrix, dtype=numpy.float32)

        self.SphereRadius = 0
        

        fboColor = Create_Color_Buffer(self.ScreenWidth, self.ScreenHeight)
        fboDepth = Create_Depth_Buffer(self.ScreenWidth, self.ScreenHeight)
        self.fboID = Create_FBO(fboColor, fboDepth , 1)
        self.fboMSAA = Create_FBO_MSAA(self.ScreenWidth, self.ScreenHeight)
        self.GL_Data = None
        self.VBOIdList = []

    def setGLData(self, gl_data):
        self.GL_Data = gl_data
    
        for data in self.GL_Data:
            vboid = 0
            vbosize = 0
            if data['primitive'] == "triangle":
                vboid, vbosize = self.initializeBuffer(data['VBOData'], 3)
            else:
                vboid, vbosize = self.initializeBuffer(data['VBOData'], 2)
            #vboid_index, vbosize_index = self.initializeIndexBuffer(data['indices'])
            self.VBOIdList.append({'primitive':data['primitive'],
                                 'VBOData': [vboid, vbosize],                 
                                 'radius':data['radius']})

        self.CircleShader = initializeCircleShader()
        self.TubeShader = initializeTubeShader()


    def initializeBuffer(self, Vertices, primflag=2):
        Vertices = numpy.array(Vertices, dtype=numpy.float32)

        vboId = glvbo.VBO(Vertices)
        vboSize = len(Vertices)/primflag
        return vboId, vboSize
    


    def renderTubeScene(self, start, size):
#        glutSolidSphere(50, 12,12);
        glUseProgram(self.TubeShader)


        loc = glGetUniformLocation(self.TubeShader, "ModelViewMatrix");
        glUniformMatrix4fv(loc, 1, GL_FALSE, self.ModelViewMatrix);
        loc = glGetUniformLocation(self.TubeShader, "ProjectionMatrix");
        glUniformMatrix4fv(loc, 1, GL_FALSE, self.ProjectionMatrix);
        
        LightPos = [100000.0, 100000.0, 100000.0, 1.0]
        LightAmbient = [.2, .2, .2, 0.3]
        LigthDiffuse = [0.5,0.5,0.5,0.3]
        LightSpecular = [0.2,0.2,0.2,0.3]
        LightAtt = [0.2, 0.0, 0.0, 0.3]

        MaterialAmbient = [ 0.3, 0.3, 0.3, 0.3]
        MaterialDiffuse = [0.5,0.5,0.5,0.3]
        MaterialSpecular = [0.3,0.3,0.3,0.3]
        MaterialShineness = 1.0
        # light
        loc = glGetUniformLocation(self.TubeShader, "light_position");
        glUniform4fv(loc, 1, LightPos);
        loc = glGetUniformLocation(self.TubeShader, "light_ambient");
        glUniform4fv(loc, 1, LightAmbient);
        loc = glGetUniformLocation(self.TubeShader, "light_diffuse");
        glUniform4fv(loc, 1, LigthDiffuse);
        loc = glGetUniformLocation(self.TubeShader, "light_specular");
        glUniform4fv(loc, 1, LightSpecular);
        loc = glGetUniformLocation(self.TubeShader, "light_att");
        glUniform4fv(loc, 1, LightAtt);
        
        # material
        loc = glGetUniformLocation(self.TubeShader, "material_ambient");
        glUniform4fv(loc, 1, MaterialAmbient);
        loc = glGetUniformLocation(self.TubeShader, "material_diffuse");
        glUniform4fv(loc, 1, MaterialDiffuse);
        loc = glGetUniformLocation(self.TubeShader, "material_specular");
        glUniform4fv(loc, 1, MaterialSpecular);
        loc = glGetUniformLocation(self.TubeShader, "material_shineness");
        glUniform1f(loc, MaterialShineness);

        loc = glGetUniformLocation(self.TubeShader, "radius");
        glUniform1f(loc, self.SphereRadius);

        glDrawArrays(GL_LINE_STRIP, start, size)
        
        glUseProgram(0)

    def renderCircleScene(self,start, size, index):
        
        
        glUseProgram(self.CircleShader)

        loc = glGetUniformLocation(self.CircleShader, "ModelViewMatrix");
        glUniformMatrix4fv(loc, 1, GL_FALSE, self.ModelViewMatrix);
        loc = glGetUniformLocation(self.CircleShader, "ProjectionMatrix");
        glUniformMatrix4fv(loc, 1, GL_FALSE, self.ProjectionMatrix);

        LightPos = [100000.0, 100000.0, 100000.0, 1.0]
        LightAmbient = [.2, .2, .2, 0.3]
        LigthDiffuse = [0.5,0.5,0.5,0.3]
        LightSpecular = [0.2,0.2,0.2,0.3]
        LightAtt = [0.2, 0.0, 0.0, 0.3]

        MaterialAmbient = [ 0.3, 0.3, 0.3, 0.3]
        MaterialDiffuse = [0.5,0.5,0.5,0.3]
        MaterialSpecular = [0.3,0.3,0.3,0.3]
        MaterialShineness = 1.0
            
        # light
        loc = glGetUniformLocation(self.CircleShader, "light_position");
        glUniform4fv(loc, 1, LightPos);
        loc = glGetUniformLocation(self.CircleShader, "light_ambient");
        glUniform4fv(loc, 1, LightAmbient);
        loc = glGetUniformLocation(self.CircleShader, "light_diffuse");
        glUniform4fv(loc, 1, LigthDiffuse);
        loc = glGetUniformLocation(self.CircleShader, "light_specular");
        glUniform4fv(loc, 1, LightSpecular);
        loc = glGetUniformLocation(self.CircleShader, "light_att");
        glUniform4fv(loc, 1, LightAtt);
        
        # material
        loc = glGetUniformLocation(self.CircleShader, "material_ambient");
        glUniform4fv(loc, 1, MaterialAmbient);
        loc = glGetUniformLocation(self.CircleShader, "material_diffuse");
        glUniform4fv(loc, 1, MaterialDiffuse);
        loc = glGetUniformLocation(self.CircleShader, "material_specular");
        glUniform4fv(loc, 1, MaterialSpecular);
        loc = glGetUniformLocation(self.CircleShader, "material_shineness");
        glUniform1f(loc, MaterialShineness);

        loc = glGetUniformLocation(self.CircleShader, "radius");
        glUniform1f(loc, self.SphereRadius);
        

        loc = glGetUniformLocation(self.CircleShader, "index");
        glUniform1i(loc, index);
        
        glDrawArrays(GL_POINTS, start, size) 

        glUseProgram(0)

    def SendBufferToVivaldi(self, source):
         
        glBindFramebuffer(GL_FRAMEBUFFER, self.fboID)

        Color_Mat = ( GLubyte * (4*self.ScreenWidth*self.ScreenHeight) )(0)
        glReadPixels(0, 0, self.ScreenWidth, self.ScreenHeight, GL_RGBA, GL_UNSIGNED_BYTE, Color_Mat)
        Color_Mat = numpy.fromstring(Color_Mat, dtype=numpy.uint8)#.astype(numpy.float32)
        #open("colorMat", "wb").write(Color_Mat)

        Depth_Mat = ( GLubyte * (4 * self.ScreenWidth*self.ScreenHeight) )(0)
        glReadPixels(0, 0, self.ScreenWidth, self.ScreenHeight, GL_DEPTH_COMPONENT, GL_FLOAT, Depth_Mat)

        Depth_Mat = numpy.fromstring(Depth_Mat, dtype=numpy.float32)
        
        index = 0
        for d in Depth_Mat:
            if int(d) == 1:
                Color_Mat[4*index] = 0
                Color_Mat[4*index+1] = 0
                Color_Mat[4*index+2] = 0
                Color_Mat[4*index+3] = 0
            index += 1
        Depth_Mat = Depth_Mat.reshape(self.ScreenHeight, self.ScreenWidth)
        Color_Mat = Color_Mat.reshape(self.ScreenHeight, self.ScreenWidth, 4)    
        #open("depthMat", "wb").write(Depth_Mat)

        glBindFramebuffer(GL_FRAMEBUFFER, 0)

        Depth_Mat = (Depth_Mat * 2 * 4096 - 4096)

        #Depth_Mat[Depth_Mat > 4095] = -4096

        #Depth_Mat = -Depth_Mat
        global count

        if count > 10:
            exit()

        else:
            #Image.open('result/test_%03d.tif'
            import Image
            Image.fromarray(Color_Mat).save('result/test_%03d.tif'%count)
            count += 1
        


    def handlerDisplay(self):

        global alive, num_recv, num_send, data_dict, args_dict, total_comm_time

        if not alive:
            clisock, addr = svrsock.accept()
            self.clisock = clisock
            self.addr    = addr
            alive = True
    
        if alive:
       
            msg = self.clisock.recv(msg_size)
            #if msg == 'quit':
                #break
            num_recv += 1       
    
            temp_time = time.time()
            #if command == "GPUS":
            #elif command == "TRAIN":
            #print msg[:msg.rfind('**')]
            key = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]
            command = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]

            print command
    
            if command =='':
                pass
    
            if command == 'clear':
                data_dict = {}
                args_dict = {}
                pass 
    
            elif command == 'suicide':
                loop = False
                alive = False
                svrsock.close()
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
                exit()     
    
    
            if command == 'persist':
                print "NO PERSIST"
                exit()
    
            if command == 'hit':
                print "NO HIT"
                exit()
    
    
            if command == 'send':
                                    
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
    
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
               
                #print args_len,lenn1,data_len,lenn2
    
                temp_time = time.time()
    
                args_str = sock_recv(self.clisock,lenn1)
                data_str = sock_recv(self.clisock,lenn2)
        
    
                bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
                
                args_str = args_str[:args_len]
                data_str = data_str[:data_len]
          
    
                if key in args_dict : pass
                else : args_dict[key] = {}
    
                import cPickle as pickle
                target_id, shape_dict= pickle.loads(args_str)
              
                args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                args_dict[key]['indata_type']  = shape_dict['indata_type']
                args_dict[key]['data_halo']    = shape_dict['data_halo']
                args_dict[key]['target_id']    = target_id
                data_dict[key] = data_str
                
            if command == 'send_seq2':
                #print "[%s] send_seq 2 start : "%host_name, time.time()
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                num_elems = int(msg[:msg.find('**')])
    
                #data_str =''
                #for elem in range(lenn1):
                #    data_str += clisock.recv(msg_size)
                #    num_recv += 1       
    
                data_str = sock_recv(self.clisock,lenn1)
                if key in args_dict : pass
                else : args_dict[key] = {}
    
    
                key_list= []
    
                #Take 40ms 
                for i in range(num_elems): 
                    data_key= data_str[:data_str.find('**')]
                    data_str= data_str[data_str.find('**')+2:]
                   
                    key_list.append(data_key)
    
                    #if data_key not in data_dict_cache:
                    #    miss_list.append(data_key)
    
    
                data_buf = []
    
                i = 0
    
                for data_key in key_list:
    
                    array_info , array_str = data_dict_cache[data_key] 
    
                    data_buf.append(array_str)
                    
                    if i == 0:
    
                        import cPickle as pickle
                        shape_dict= pickle.loads(array_info)
                        args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                        args_dict[key]['indata_type']  = shape_dict['indata_type']
                        args_dict[key]['data_halo']    = 0
                        args_dict[key]['indata_num']   = num_elems
                        args_dict[key]['target_id']    = 0
                
                        i+=1
                
                stream[key] = cuda.Stream()
                data_string = "".join(data_buf)
                data_dict[key] = data_string
                
                for data_key in key_list:
                    del data_dict_cache[data_key]
                                
    
            if command == 'send_seq':
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
    
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
                
                temp_time = time.time()
                #args_str =''
                #for elem in range(lenn1):
                #    args_str += clisock.recv(msg_size)
                #    num_recv += 1       
    
                args_str = sock_recv(self.clisock,lenn1)
                data_str = sock_recv(self.clisock,lenn2)
    
                #data_str =''
                #for elem in range(lenn2):
                #    data_str += clisock.recv(msg_size)
                #    num_recv += 1       
                bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
                
                args_str = args_str[:args_len]
                data_str = data_str[:data_len]
          
    
    
                if key in args_dict : pass
                else : args_dict[key] = {}
    
                import cPickle as pickle
                target_id, shape_dict= pickle.loads(args_str)
                                
                args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                args_dict[key]['indata_type']  = shape_dict['indata_type']
                args_dict[key]['data_halo']    = shape_dict['data_halo']
                args_dict[key]['indata_num']   = shape_dict['indata_num']
                args_dict[key]['target_id']    = target_id
                
                stream[key] = cuda.Stream()
    
                data_dict[key] = data_str
    
                              
            elif command == 'recv':
                        
                # send split_position location send_cnt data
                #key = msg[:msg.find('**')]
    
                if key not in data_dict:
                    sending_str = "absent**"
                    sending_str += '0'*msg_size
                    self.clisock.send(sending_str[:msg_size])
                else :
    
                    #if key in devptr_dict:
                    #if in_process == True:
                    if True:
                        #data_dict[key]   = gpu_dtoh(args_dict[key],ctx)
                        data_dict[key]  = numpy.zeros([100, 100, 3], dtype=numpy.uint8)
                        
            
                    print "[%s] run end : "%host_name, time.time()
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
    
                    #print args_str,args_len,data_len,lenn1,lenn2
    
                    sending_str = "exist**"
                    sending_str += "%s**%s**%s**%s**"%(str(args_len), str(lenn1), str(data_len),str(lenn2))
                    sending_str += '0'*msg_size
                    self.clisock.send(sending_str[:msg_size])
    
                    temp_time = time.time()
                    for elem in range(lenn1):
                        self.clisock.send(args_str[elem*msg_size:(elem+1)*msg_size])
    
                    for elem in range(lenn2):
                        self.clisock.send(data_str[elem*msg_size:(elem+1)*msg_size])
                    #clisock.close()
                    #print_bred("NEVER CALL ME")
    
                    bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
    
                    self.clisock.close()
                    del args_dict[key]
                    del data_dict[key]
                    
                    print "[%s] recv end : "%host_name, time.time()
                    print "\n\n\n"
                    pass 
    
            elif command == 'action':
                        
                if key not in data_dict:
                    sending_str = "absent**"
                    sending_str += '0'*msg_size
                    self.clisock.send(sending_str[:msg_size])
                else :
    
                    if True:
                        #data_dict[key]   = gpu_dtoh(args_dict[key],ctx)
                        #data_dict[key]  = gpu_dtoh(data_dict[key],args_dict[key],ctx,stream[key])
                        pass
                    print "[%s] run end : "%host_name, time.time()
            
    
                    data_array = data_dict[key]
    
                    shape_dict={}
                    shape_dict['indata_shape'] = data_array.shape
                    shape_dict['indata_type'] = data_array.dtype
    
                    import cPickle as pickle
                    args_str = pickle.dumps(shape_dict,-1)
                    data_str = data_array.tostring()
    
                    new_data_key=id_generator()
                    
                    data_dict_cache[new_data_key]=[args_str,data_str]
        
                    
                    del args_dict[key]
                    del data_dict[key]
                count += 1
    
            elif command == 'run':
                print "[%s] run start : "%host_name, time.time()
    
                #key = msg[:msg.find('**')]
                #msg = msg[msg.find('**')+2:]
        
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
    
                args_str = sock_recv(self.clisock,lenn)
                args_str = args_str[:args_len]
    
                #reading_args(args_dict[key],args_str)
    
    
            elif command == 'request':
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                requester = msg[:msg.find('**')] 
    
                
    
                data_str = sock_recv(self.clisock,lenn)
    
                import cPickle as pickle
                recv_dict = pickle.loads(data_str)
    
                send_dict = []
                for elem in recv_dict:
                    if elem in data_dict_cache:
                        send_dict.append(elem) 
                
    
                if len(send_dict) > 0:
                    newsock = socket(AF_INET, SOCK_STREAM)
                    newsock.connect((requester,4950))
                    msg_tag = "transfer"
                    sending_str = "%s**%s**%s**%s**"%(str(key),msg_tag,str(len(send_dict)),host_name)
                    sending_str += '0'*msg_size
                    newsock.send(sending_str[:msg_size])
    
                    for elem in send_dict:
                        args_str = data_dict_cache[elem][0]
                        args_len = len(args_str)
                        args_str += '0'*msg_size
                        lenn1 = len(args_str)/msg_size
    
                        data_str = data_dict_cache[elem][1]
                        data_len = len(data_str)
                        data_str += '0'*msg_size
                        lenn2 = len(data_str)/msg_size
    
                        sending_str = "%s**%s**%s**%s**%s**"%(elem,str(args_len),str(lenn1),str(data_len),str(lenn2))
                        sending_str += '0'*msg_size
                    
                        newsock.send(sending_str[:msg_size])
                        newsock.send(args_str[:lenn1*msg_size])
                        newsock.send(data_str[:lenn2*msg_size])
                        
                        del data_dict_cache[elem]
    
    
                    newsock.close()
                else :
                    pass
    
                self.clisock.close()
                pass 
               
    
            elif command == 'count':
                block_num += 1 
                #pass
                sending_str = "done**"
                sending_str += '0'*msg_size
                self.clisock.send(sending_str[:msg_size])
    
                print_( "Process [%d/%d] has block %d now "%(mpi_rank,mpi_size,block_num))
    
        
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


        

    def render():

        self.ModelViewMatrix = numpy.eye(4)

        glMatrixMode(GL_MODELVIEW)
        glLoadMatrixf(self.ModelViewMatrix)
        glBindFramebuffer(GL_FRAMEBUFFER, self.fboMSAA)
        glClearColor(0.5, 0.5, 0.5, 0.0)
        glClear(GL_COLOR_BUFFER_BIT | GL_DEPTH_BUFFER_BIT)

        
        glPushMatrix()
        for vbo in self.VBOIdList:
            vbo['VBOData'][0].bind()
            
            self.SphereRadius = vbo['radius']
            glEnableClientState(GL_VERTEX_ARRAY)
            glEnableClientState(GL_COLOR_ARRAY)

            if vbo['primitive'] == "tube":
                self.SphereRadius = vbo['radius']
                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderCircleScene(0, vbo['VBOData'][1],1)
                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderCircleScene(0, vbo['VBOData'][1],2)
                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderCircleScene(0, vbo['VBOData'][1],3)
                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderCircleScene(0, vbo['VBOData'][1],4)
                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderCircleScene(0, vbo['VBOData'][1],5)
                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderCircleScene(0, vbo['VBOData'][1],6)

                glVertexPointer(4, GL_FLOAT, 32, vbo['VBOData'][0])
                glColorPointer(4, GL_FLOAT, 32, vbo['VBOData'][0]+16)
                self.renderTubeScene(0, vbo['VBOData'][1])

            glDisableClientState(GL_COLOR_ARRAY);
            glDisableClientState(GL_VERTEX_ARRAY);

            vbo['VBOData'][0].unbind()

        glPopMatrix()

        glutSwapBuffers()

        glBindFramebuffer(GL_FRAMEBUFFER, 0);
        glBindFramebuffer(GL_READ_FRAMEBUFFER, self.fboMSAA)
        glBindFramebuffer(GL_DRAW_FRAMEBUFFER, self.fboID)
        glBlitFramebuffer(0, 0, self.ScreenWidth, self.ScreenHeight, 0, 0, self.ScreenWidth, self.ScreenHeight, GL_COLOR_BUFFER_BIT, GL_NEAREST);      
        glBlitFramebuffer(0, 0, self.ScreenWidth, self.ScreenHeight, 0, 0, self.ScreenWidth, self.ScreenHeight, GL_DEPTH_BUFFER_BIT, GL_NEAREST);      
        glBindFramebuffer(GL_FRAMEBUFFER, 0);
        
        glutSwapBuffers()

        self.SendBufferToVivaldi(0)

        return

    def handlerReshape(self, width, height):
        glMatrixMode(GL_PROJECTION)
        glLoadIdentity()
        glViewport(0, 0, self.ScreenWidth, self.ScreenHeight)
        glOrtho(-1*self.ScreenWidth/2, self.ScreenWidth/2, -1*self.ScreenHeight/2, self.ScreenHeight/2, -4096, 4096)

        self.ProjectionMatrix = glGetFloatv(GL_PROJECTION_MATRIX, self.ProjectionMatrix);
        

        return

    def handlerIdle(self):
        glutPostRedisplay()

    def EnterMainLoop(self):
        glutMainLoop()
        return


GL_Rendering_Size = [1024, 1024]

mRender = Render("OpenGL_Render", GL_Rendering_Size)

def readColor(file_path):                                                                                                                                                                                                                    
    lines = open(file_path).readlines()

    colors = []
    for elem in lines:
        colors.append([int(color) for color in elem.split(' ')])

    colors = numpy.array(colors, dtype=numpy.float32) / 255.0

    return colors



def readSphereArray(Point_path, Color_path):
    points = []
    lines = open(Point_path).readlines()

    color = readColor(Color_path)

    for elem in lines:
        vals = elem.split(" ")
        vals = [(float(val)) for val in vals]
        vals[-1] = 0
        points.append(vals)
    print len(color), len(points)
    
    vertices = []
    for elem in range(len(color)):
        vertices.append(points[elem])
        col = list(color[elem])
        col.append(1.0)
        vertices.append(col)

    return vertices

data = readSphereArray('test/Somas_Set_5_coord','test/Somas_Set_5_color')
mGL_Data = []
mData = {'primitive':'sphere', 'VBOData':data, 'radius':3.25}
mGL_Data.append(mData)

mRender.setGLData(mGL_Data)

print("gl_renderer initialize done")
mRender.EnterMainLoop()



while False:
    #try:
    if True:
        clisock, addr = svrsock.accept()

        while alive:
       
            msg = clisock.recv(msg_size)
            #if msg == 'quit':
                #break
            num_recv += 1       
 
            temp_time = time.time()
            #if command == "GPUS":
            #elif command == "TRAIN":
            #print msg[:msg.rfind('**')]
            key = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]
            command = msg[:msg.find('**')]
            msg = msg[msg.find('**')+2:]

            print 'command : ', command

            if command =='':
                break

            if command == 'clear':
                data_dict = {}
                args_dict = {}
                continue

            elif command == 'suicide':
                loop = False
                alive = False
                svrsock.close()
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
                exit()     


            if command == 'persist':
                print "NO PERSIST"
                exit()

            if command == 'hit':
                print "NO HIT"
                exit()


            if command == 'send':
                                    
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
               
                #print args_len,lenn1,data_len,lenn2
 
                temp_time = time.time()

                args_str = sock_recv(clisock,lenn1)
                data_str = sock_recv(clisock,lenn2)
        

                bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
                
                args_str = args_str[:args_len]
                data_str = data_str[:data_len]
          

                if key in args_dict : pass
                else : args_dict[key] = {}

                import cPickle as pickle
                target_id, shape_dict= pickle.loads(args_str)
              
                args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                args_dict[key]['indata_type']  = shape_dict['indata_type']
                args_dict[key]['data_halo']    = shape_dict['data_halo']
                args_dict[key]['target_id']    = target_id
                data_dict[key] = data_str
                
            if command == 'send_seq2':
                #print "[%s] send_seq 2 start : "%host_name, time.time()
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                num_elems = int(msg[:msg.find('**')])

                #data_str =''
                #for elem in range(lenn1):
                #    data_str += clisock.recv(msg_size)
                #    num_recv += 1       

                data_str = sock_recv(clisock,lenn1)
                if key in args_dict : pass
                else : args_dict[key] = {}


                key_list= []

                #Take 40ms 
                for i in range(num_elems): 
                    data_key= data_str[:data_str.find('**')]
                    data_str= data_str[data_str.find('**')+2:]
                   
                    key_list.append(data_key)
 
                    #if data_key not in data_dict_cache:
                    #    miss_list.append(data_key)


                data_buf = []

                i = 0

                for data_key in key_list:
 
                    array_info , array_str = data_dict_cache[data_key] 

                    data_buf.append(array_str)
                    
                    if i == 0:

                        import cPickle as pickle
                        shape_dict= pickle.loads(array_info)
                        args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                        args_dict[key]['indata_type']  = shape_dict['indata_type']
                        args_dict[key]['data_halo']    = 0
                        args_dict[key]['indata_num']   = num_elems
                        args_dict[key]['target_id']    = 0
                
                        i+=1
                
                stream[key] = cuda.Stream()
                data_string = "".join(data_buf)
                data_dict[key] = data_string
                
                for data_key in key_list:
                    del data_dict_cache[data_key]
                                
 
            if command == 'send_seq':
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn1 = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn2 = int(msg[:msg.find('**')])
                
                temp_time = time.time()
                #args_str =''
                #for elem in range(lenn1):
                #    args_str += clisock.recv(msg_size)
                #    num_recv += 1       
 
                args_str = sock_recv(clisock,lenn1)
                data_str = sock_recv(clisock,lenn2)

                #data_str =''
                #for elem in range(lenn2):
                #    data_str += clisock.recv(msg_size)
                #    num_recv += 1       
                bandwidth = (lenn1 + lenn2)*msg_size / (time.time() - temp_time) / (1048576.0)
                
                args_str = args_str[:args_len]
                data_str = data_str[:data_len]
          

 
                if key in args_dict : pass
                else : args_dict[key] = {}

                import cPickle as pickle
                target_id, shape_dict= pickle.loads(args_str)
                                
                args_dict[key]['indata_shape'] = shape_dict['indata_shape']
                args_dict[key]['indata_type']  = shape_dict['indata_type']
                args_dict[key]['data_halo']    = shape_dict['data_halo']
                args_dict[key]['indata_num']   = shape_dict['indata_num']
                args_dict[key]['target_id']    = target_id
                
                stream[key] = cuda.Stream()

                data_dict[key] = data_str

                              
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
                        data_dict[key]  = numpy.zeros([100, 100, 3], dtype=numpy.uint8)
                        
            
                    print "[%s] run end : "%host_name, time.time()
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

                    #print args_str,args_len,data_len,lenn1,lenn2

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

                    clisock.close()
                    del args_dict[key]
                    del data_dict[key]
                    
                    print "[%s] recv end : "%host_name, time.time()
                    print "\n\n\n"
                    break
 
            elif command == 'action':
                        
                if key not in data_dict:
                    sending_str = "absent**"
                    sending_str += '0'*msg_size
                    clisock.send(sending_str[:msg_size])
                else :

                    if True:
                        #data_dict[key]   = gpu_dtoh(args_dict[key],ctx)
                        #data_dict[key]  = gpu_dtoh(data_dict[key],args_dict[key],ctx,stream[key])
                        pass
                    print "[%s] run end : "%host_name, time.time()
            

                    data_array = data_dict[key]

                    shape_dict={}
                    shape_dict['indata_shape'] = data_array.shape
                    shape_dict['indata_type'] = data_array.dtype

                    import cPickle as pickle
                    args_str = pickle.dumps(shape_dict,-1)
                    data_str = data_array.tostring()

                    new_data_key=id_generator()
                    
                    data_dict_cache[new_data_key]=[args_str,data_str]
        
                    
                    del args_dict[key]
                    del data_dict[key]
                count += 1

            elif command == 'run':
                print "[%s] run start : "%host_name, time.time()

                #key = msg[:msg.find('**')]
                #msg = msg[msg.find('**')+2:]
        
                args_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]

                args_str = sock_recv(clisock,lenn)
                args_str = args_str[:args_len]
 
                #reading_args(args_dict[key],args_str)
  

            elif command == 'request':
                data_len = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                lenn = int(msg[:msg.find('**')])
                msg = msg[msg.find('**')+2:]
                requester = msg[:msg.find('**')] 

                

                data_str = sock_recv(clisock,lenn)

                import cPickle as pickle
                recv_dict = pickle.loads(data_str)

                send_dict = []
                for elem in recv_dict:
                    if elem in data_dict_cache:
                        send_dict.append(elem) 
                

                if len(send_dict) > 0:
                    newsock = socket(AF_INET, SOCK_STREAM)
                    newsock.connect((requester,4950))
                    msg_tag = "transfer"
                    sending_str = "%s**%s**%s**%s**"%(str(key),msg_tag,str(len(send_dict)),host_name)
                    sending_str += '0'*msg_size
                    newsock.send(sending_str[:msg_size])

                    for elem in send_dict:
                        args_str = data_dict_cache[elem][0]
                        args_len = len(args_str)
                        args_str += '0'*msg_size
                        lenn1 = len(args_str)/msg_size
    
                        data_str = data_dict_cache[elem][1]
                        data_len = len(data_str)
                        data_str += '0'*msg_size
                        lenn2 = len(data_str)/msg_size

                        sending_str = "%s**%s**%s**%s**%s**"%(elem,str(args_len),str(lenn1),str(data_len),str(lenn2))
                        sending_str += '0'*msg_size
                    
                        newsock.send(sending_str[:msg_size])
                        newsock.send(args_str[:lenn1*msg_size])
                        newsock.send(data_str[:lenn2*msg_size])
                        
                        del data_dict_cache[elem]


                    newsock.close()
                else :
                    pass
 
                clisock.close()
                break
               

            elif command == 'count':
                block_num += 1 
                #pass
                sending_str = "done**"
                sending_str += '0'*msg_size
                clisock.send(sending_str[:msg_size])
 
                print_( "Process [%d/%d] has block %d now "%(mpi_rank,mpi_size,block_num))

        
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

