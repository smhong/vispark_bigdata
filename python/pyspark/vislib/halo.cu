
extern "C"{
__global__ void extract_halo(float* out, float* in, int* in_size, int *_x, int *_y, int *_z, int len_vec) {

	// Output coordinate
	int o_x = blockIdx.x * blockDim.x + threadIdx.x ;
	int o_y = blockIdx.y * blockDim.y + threadIdx.y ;
	int o_z = blockIdx.z * blockDim.z + threadIdx.z ;

	// Input coordinate
	int i_x = o_x + _x[0];
	int i_y = o_y + _y[0];
	int i_z = o_z + _z[0];

	if(i_x >= in_size[0]  || i_y >= in_size[1]  || i_z >= in_size[2]) 
		return;
	if(o_x >= _x[1]-_x[0] || o_y >= _y[1]-_y[0] || o_z >= _z[1]-_z[0]) 
		return;

	int idx     = (i_z * in_size[0]   * in_size[1]   + i_y * in_size[0]   + i_x);
	int out_idx = (o_z * (_x[1]-_x[0]) * (_y[1]-_y[0]) + o_y * (_x[1]-_x[0]) + o_x);

	for(int i=0; i<len_vec; i++) {
		out[out_idx*len_vec + i ] = in[idx*len_vec + i];
	}

}
}


extern "C"{
__global__ void append_halo(float* out, float* in, int* out_size, int *_x, int *_y, int *_z, int len_vec) {

	// Output coordinate
	int i_x = blockIdx.x * blockDim.x + threadIdx.x ;
	int i_y = blockIdx.y * blockDim.y + threadIdx.y ;
	int i_z = blockIdx.z * blockDim.z + threadIdx.z ;

	// Input coordinate
	int o_x = i_x + _x[0];
	int o_y = i_y + _y[0];
	int o_z = i_z + _z[0];

	if(o_x >= out_size[0]  || o_y >= out_size[1]  || o_z >= out_size[2]) 
		return;
	if(i_x >= _x[1]-_x[0] || i_y >= _y[1]-_y[0] || i_z >= _z[1]-_z[0]) 
		return;

	int idx    = (o_z * out_size[0]   * out_size[1]   + o_y * out_size[0]   + o_x);
	int in_idx = (i_z * (_x[1]-_x[0]) * (_y[1]-_y[0]) + i_y * (_x[1]-_x[0]) + i_x);

	for(int i=0; i<len_vec; i++) {
		out[idx*len_vec + i ] = in[in_idx*len_vec + i];
		//out[idx*len_vec + i ] = 10.0;
	}

}
}
