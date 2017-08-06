from OpenGL.GLUT import *
from OpenGL.GLU import *
from OpenGL.GL import *
from OpenGL.arrays import ArrayDatatype as ADT
import sys, numpy


def Create_Color_Texture(TEXTURE_WIDTH, TEXTURE_HEIGHT):
	color_tex = glGenTextures(1);
	glBindTexture(GL_TEXTURE_2D, color_tex);
	glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_LINEAR);
	glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_LINEAR_MIPMAP_LINEAR);
	glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_CLAMP_TO_EDGE);
	glTexParameterf(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_CLAMP_TO_EDGE);
	glTexParameteri(GL_TEXTURE_2D, GL_GENERATE_MIPMAP, GL_TRUE);
	glTexImage2D(GL_TEXTURE_2D, 0, GL_RGBA, TEXTURE_WIDTH, TEXTURE_HEIGHT, 0, GL_RGBA, GL_UNSIGNED_BYTE, None);
	glBindTexture(GL_TEXTURE_2D, 0)
	return color_tex

def Create_Depth_Texture(TEXTURE_WIDTH, TEXTURE_HEIGHT):
	depth_tex = glGenTextures(1);
	glBindTexture(GL_TEXTURE_2D, depth_tex);
	glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_S, GL_REPEAT);
	glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_WRAP_T, GL_REPEAT);
	glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MIN_FILTER, GL_NEAREST);
	glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_MAG_FILTER, GL_NEAREST);
	glTexParameteri(GL_TEXTURE_2D, GL_DEPTH_TEXTURE_MODE, GL_INTENSITY);
	glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_COMPARE_MODE, GL_COMPARE_R_TO_TEXTURE);
	glTexParameteri(GL_TEXTURE_2D, GL_TEXTURE_COMPARE_FUNC, GL_LEQUAL);
	glTexImage2D(GL_TEXTURE_2D, 0, GL_DEPTH_COMPONENT, TEXTURE_WIDTH, TEXTURE_HEIGHT, 0, GL_DEPTH_COMPONENT, GL_FLOAT, None);
	glBindTexture(GL_TEXTURE_2D, 0)
	return depth_tex

def Create_Depth_Buffer(TEXTURE_WIDTH, TEXTURE_HEIGHT):
	depthBuf = glGenRenderbuffers(1);
	glBindRenderbuffer(GL_RENDERBUFFER, depthBuf);
	glRenderbufferStorage(GL_RENDERBUFFER, GL_DEPTH_COMPONENT, TEXTURE_WIDTH, TEXTURE_HEIGHT);
	glBindRenderbuffer(GL_RENDERBUFFER, 0);
	return depthBuf

def Create_Color_Buffer(TEXTURE_WIDTH, TEXTURE_HEIGHT):
	colorBuf = glGenRenderbuffers(1);
	glBindRenderbuffer(GL_RENDERBUFFER, colorBuf);
	glRenderbufferStorage(GL_RENDERBUFFER, GL_RGBA, TEXTURE_WIDTH, TEXTURE_HEIGHT);
	glBindRenderbuffer(GL_RENDERBUFFER, 0);
	return colorBuf

def Create_FBO(colorId, depthId, option):
	fboId = glGenFramebuffers(1);
	glBindFramebuffer(GL_FRAMEBUFFER, fboId);    
	if option == 0:
		glFramebufferTexture2D(GL_FRAMEBUFFER, GL_COLOR_ATTACHMENT1, GL_TEXTURE_2D, colorId, 0)  
		glFramebufferTexture2D(GL_FRAMEBUFFER, GL_DEPTH_ATTACHMENT, GL_TEXTURE_2D, depthId, 0)
	elif option == 1:
		glFramebufferRenderbuffer(GL_FRAMEBUFFER, GL_COLOR_ATTACHMENT0, GL_RENDERBUFFER, colorId);
		glFramebufferRenderbuffer(GL_FRAMEBUFFER, GL_DEPTH_ATTACHMENT, GL_RENDERBUFFER, depthId);
	#glFramebufferRenderbuffer(GL_FRAMEBUFFER, GL_DEPTH_ATTACHMENT, GL_RENDERBUFFER, depthBuf);

	status = glCheckFramebufferStatus(GL_FRAMEBUFFER);
	if status == GL_FRAMEBUFFER_COMPLETE:
			pass
	glBindFramebuffer(GL_FRAMEBUFFER, 0);
	return fboId

def Create_FBO_MSAA(TEXTURE_WIDTH, TEXTURE_HEIGHT):
	fboMsaaId = glGenFramebuffers(1);
	glBindFramebuffer(GL_FRAMEBUFFER, fboMsaaId);

	# // create a MSAA renderbuffer object to store color info
	rboColorId = glGenRenderbuffers(1);
	glBindRenderbuffer(GL_RENDERBUFFER, rboColorId);
	glRenderbufferStorageMultisample(GL_RENDERBUFFER, 8, GL_RGBA, TEXTURE_WIDTH, TEXTURE_HEIGHT);
	glBindRenderbuffer(GL_RENDERBUFFER, 0);

	# // create a MSAA renderbuffer object to store depth info
	# // NOTE: A depth renderable image should be attached the FBO for depth test.
	# // If we don't attach a depth renderable image to the FBO, then
	# // the rendering output will be corrupted because of missing depth test.
	# // If you also need stencil test for your rendering, then you must
	# // attach additional image to the stencil attachement point, too.
	rboDepthId = glGenRenderbuffers(1);
	glBindRenderbuffer(GL_RENDERBUFFER, rboDepthId);
	glRenderbufferStorageMultisample(GL_RENDERBUFFER, 8, GL_DEPTH_COMPONENT, TEXTURE_WIDTH, TEXTURE_HEIGHT);
	glBindRenderbuffer(GL_RENDERBUFFER, 0);

	# // attach msaa RBOs to FBO attachment points
	glFramebufferRenderbuffer(GL_FRAMEBUFFER, GL_COLOR_ATTACHMENT0, GL_RENDERBUFFER, rboColorId);
	glFramebufferRenderbuffer(GL_FRAMEBUFFER, GL_DEPTH_ATTACHMENT, GL_RENDERBUFFER, rboDepthId);

	return fboMsaaId

def Create_PBO(TEXTURE_WIDTH, TEXTURE_HEIGHT):
	pboId = glGenBuffers(1);
	glBindBuffer(GL_PIXEL_PACK_BUFFER, pboId);
	glBufferData(GL_PIXEL_PACK_BUFFER, TEXTURE_WIDTH*TEXTURE_HEIGHT*4, None, GL_STREAM_READ);
	glBindBuffer(GL_PIXEL_PACK_BUFFER, 0);
	return pboId
