
extern "C"{

    __global__ void image_pass(float* result, float *data, int block_idx, int dimx, int dimy, int scr_idx)
    {
	    int x = blockIdx.x * blockDim.x + threadIdx.x;
	    int y = blockIdx.y * blockDim.y + threadIdx.y;


        if (x < dimx && y < dimy)
        {
            result[y*dimx+x] = data[y*dimx + x]+1;
            //result[y*dimx+x] = 1;
        }
    }

    __global__ void composit(float* result, float *data, int num_data, int dimx, int dimy)
    {
	    int x = blockIdx.x * blockDim.x + threadIdx.x;
	    int y = blockIdx.y * blockDim.y + threadIdx.y;


        if (x < dimx && y < dimy)
        {
            float max_value = -1;
            
            for (int i = 0 ; i < num_data ; i++)
            {
                if (data[i*dimx*dimy + y*dimx + x] > max_value)
                {
                    max_value = data[i*dimx*dimy + y*dimx + x];
                }
            }
           
            result[y*dimx + x] = max_value;   
        }
    
    }
}
