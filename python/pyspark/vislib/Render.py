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

		self.Shader = initializeShader()


	def initializeBuffer(self, Vertices, primflag=2):
		Vertices = numpy.array(Vertices, dtype=numpy.float32)

		vboId = glvbo.VBO(Vertices)
		vboSize = len(Vertices)/primflag
		return vboId, vboSize
	


	def renderTubeScene(self, start, size):
#		glutSolidSphere(50, 12,12);
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
