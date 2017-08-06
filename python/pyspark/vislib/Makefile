all:
	#g++ c_manager.cpp -std=c++1y
	nvcc -o new_manager c_manager.cu -std=c++11 -O3 -arch=sm_52 -lcuda -Xcompiler -fopenmp

