extern "C"{
    __global__ void adder(float* A, float* B, int N, float x)
    {
        int idx = blockIdx.x * blockDim.x + threadIdx.x;
        if(idx > N)
            return;
        A[idx] = B[idx] + x;
    }
}
