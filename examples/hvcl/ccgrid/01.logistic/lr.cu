extern "C"{


//__global__ void transpose(float* ret, float* data, float* w, int D, int N)
__global__ void transpose(float* ret, unsigned char* data, float* w, int D, int N)
{
	int thread_idx = blockIdx.x * blockDim.x + threadIdx.x;

    if(thread_idx >= N) return;

    float label = data[thread_idx*(D+1) + 0];
    
    float dot_result = 0.0;
    for (int i = 0 ; i < D ; i++)
        dot_result += data[thread_idx * (D+1) + i+ 1] * w[i];

    float scalar_result = 1.0/(1.0 + expf(-1.0 * label * dot_result)) - 1.0 * label;

    for (int i = 0 ; i < D; i++)
        ret[i*N + thread_idx] = scalar_result * data[thread_idx*(D+1) + 1 + i];
}

__global__ void local_reduce(float* ret, float* data, int D, int N)
{
	int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if(idx >= D) return;

    float result = 0.0;

    for (int i = 0 ; i < N ; i++)
        result += data[idx * N + i];

    ret[idx] = result;


}
}

