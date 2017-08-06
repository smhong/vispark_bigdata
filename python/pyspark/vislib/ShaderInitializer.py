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


def initializeMeshShader():
	global VIVALDI_PATH

	Mesh_Shader = glCreateProgram()
	Mesh_VertexShader = glCreateShader(GL_VERTEX_SHADER)
	# Mesh_GeometryShader = glCreateShader(GL_GEOMETRY_SHADER)
	Mesh_FragmentShader = glCreateShader(GL_FRAGMENT_SHADER)
	# glProgramParameteri(Mesh_Shader, GL_GEOMETRY_INPUT_TYPE_ARB, GL_TRIANGLES)
	# glProgramParameteri(Mesh_Shader, GL_GEOMETRY_OUTPUT_TYPE_ARB, GL_TRIANGLES)
	# glProgramParameteri(Mesh_Shader, GL_MAX_GEOMETRY_OUTPUT_VERTICES, 1024)

	glAttachShader(Mesh_Shader, Mesh_VertexShader)
	# glAttachShader(Mesh_Shader, Mesh_GeometryShader)
	glAttachShader(Mesh_Shader, Mesh_FragmentShader)

	glShaderSource(Mesh_VertexShader, open('Shader/Mesh.vert', "r"))
	glCompileShader(Mesh_VertexShader)
	#print_blue(glGetShaderInfoLog(Mesh_VertexShader))

	# glShaderSource(Mesh_GeometryShader, open(VIVALDI_PATH+'/src/interactive_mode/Vivaldi_gl_renderer/Shader/Mesh.geom', "r"))
	# glCompileShader(Mesh_GeometryShader)
	# print_blue(glGetShaderInfoLog(Mesh_GeometryShader))

	glShaderSource(Mesh_FragmentShader, open('Shader/Mesh.frag', "r"))
	glCompileShader(Mesh_FragmentShader)
	#print_blue(glGetShaderInfoLog(Mesh_FragmentShader))

	glLinkProgram(Mesh_Shader)
	#print_blue(glGetProgramInfoLog(Mesh_Shader))
	
	return Mesh_Shader


def initializeLineShader():
	global VIVALDI_PATH

	Line_Shader = glCreateProgram()
	Line_VertexShader = glCreateShader(GL_VERTEX_SHADER)
	Line_GeometryShader = glCreateShader(GL_GEOMETRY_SHADER)
	Line_FragmentShader = glCreateShader(GL_FRAGMENT_SHADER)
	glProgramParameteri(Line_Shader, GL_GEOMETRY_INPUT_TYPE_ARB, GL_LINES)
	glProgramParameteri(Line_Shader, GL_GEOMETRY_OUTPUT_TYPE_ARB, GL_TRIANGLES)
	glProgramParameteri(Line_Shader, GL_MAX_GEOMETRY_OUTPUT_VERTICES, 1024)

	glAttachShader(Line_Shader, Line_VertexShader)
	glAttachShader(Line_Shader, Line_GeometryShader)
	glAttachShader(Line_Shader, Line_FragmentShader)

	glShaderSource(Line_VertexShader, open('Shader/Line.vert', "r"))
	glCompileShader(Line_VertexShader)
	#print_blue(glGetShaderInfoLog(Line_VertexShader))

	glShaderSource(Line_GeometryShader, open('Shader/Line.geom', "r"))
	glCompileShader(Line_GeometryShader)
	#print_blue(glGetShaderInfoLog(Line_GeometryShader))

	glShaderSource(Line_FragmentShader, open('Shader/Line.frag', "r"))
	glCompileShader(Line_FragmentShader)
	#print_blue(glGetShaderInfoLog(Line_FragmentShader))

	glLinkProgram(Line_Shader)
	#print_blue(glGetProgramInfoLog(Line_Shader))

	return Line_Shader

def initializeTubeShader():
	global VIVALDI_PATH

	Tube_Shader = glCreateProgram()
	Tube_VertexShader = glCreateShader(GL_VERTEX_SHADER)
	Tube_GeometryShader = glCreateShader(GL_GEOMETRY_SHADER)
	Tube_FragmentShader = glCreateShader(GL_FRAGMENT_SHADER)
	glProgramParameteri(Tube_Shader, GL_GEOMETRY_INPUT_TYPE_ARB, GL_LINES)
	glProgramParameteri(Tube_Shader, GL_GEOMETRY_OUTPUT_TYPE_ARB, GL_TRIANGLES)
	glProgramParameteri(Tube_Shader, GL_MAX_GEOMETRY_OUTPUT_VERTICES, 1024)

	glAttachShader(Tube_Shader, Tube_VertexShader)
	glAttachShader(Tube_Shader, Tube_GeometryShader)
	glAttachShader(Tube_Shader, Tube_FragmentShader)

	glShaderSource(Tube_VertexShader, open('Shader/Tube.vert', "r"))
	glCompileShader(Tube_VertexShader)
	#print_blue(glGetShaderInfoLog(Tube_VertexShader))

	glShaderSource(Tube_GeometryShader, open('Shader/Tube.geom', "r"))
	glCompileShader(Tube_GeometryShader)
	#print_blue(glGetShaderInfoLog(Tube_GeometryShader))

	glShaderSource(Tube_FragmentShader, open('Shader/Tube.frag', "r"))
	glCompileShader(Tube_FragmentShader)
	#print_blue(glGetShaderInfoLog(Tube_FragmentShader))

	glLinkProgram(Tube_Shader)
	#print_blue(glGetProgramInfoLog(Tube_Shader))

	return Tube_Shader

def initializeCircleShader():
	global VIVALDI_PATH

	Circle_Shader = glCreateProgram()
	Circle_VertexShader = glCreateShader(GL_VERTEX_SHADER)
	Circle_GeometryShader = glCreateShader(GL_GEOMETRY_SHADER)
	Circle_FragmentShader = glCreateShader(GL_FRAGMENT_SHADER)
	glProgramParameteri(Circle_Shader, GL_GEOMETRY_INPUT_TYPE_ARB, GL_LINES)
	glProgramParameteri(Circle_Shader, GL_GEOMETRY_OUTPUT_TYPE_ARB, GL_TRIANGLES)
	glProgramParameteri(Circle_Shader, GL_MAX_GEOMETRY_OUTPUT_VERTICES, 1024)

	glAttachShader(Circle_Shader, Circle_VertexShader)
	glAttachShader(Circle_Shader, Circle_GeometryShader)
	glAttachShader(Circle_Shader, Circle_FragmentShader)

	glShaderSource(Circle_VertexShader, open('Shader/Circle.vert', "r"))
	glCompileShader(Circle_VertexShader)
	#print_blue(glGetShaderInfoLog(Circle_VertexShader))

	glShaderSource(Circle_GeometryShader, open('Shader/Circle.geom', "r"))
	glCompileShader(Circle_GeometryShader)
	#print_blue(glGetShaderInfoLog(Circle_GeometryShader))

	glShaderSource(Circle_FragmentShader, open('Shader/Circle.frag', "r"))
	glCompileShader(Circle_FragmentShader)
	#print_blue(glGetShaderInfoLog(Circle_FragmentShader))

	glLinkProgram(Circle_Shader)
	#print_blue(glGetProgramInfoLog(Circle_Shader))

	return Circle_Shader
