extern "C"{
__global__ void cloner(float* A, float* B, int *list, int N)
{
	int idx = blockIdx.x * blockDim.x + threadIdx.x;
    if(idx > 1000) 
        return;
	for (int i = 0 ; i < N ; i++) 
		A[N* idx + i] = B[idx] + list[i];
}

//__global__ void closest_with_reduce(float* ret, float* data, float* centers, int K, int dim, int N)
__global__ void closest_with_reduce_uchar(float* ret, unsigned char* data, float* centers, int K, int dim, int N)
{
	int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if(idx >= N) 
        return;

    float min_dist = 99999999;
    int   center_idx = 0 ;

    for (int j = 0 ; j < K ; j++)
    {
        float sum_dist = 0 ;
        float diff ;    

        for (int i = 0 ; i < dim ; i++)
        {
            diff =  centers[j*dim + i] - data[idx*dim + i];
            sum_dist += diff * diff;
        } 

        if (sum_dist < min_dist)
        {
            center_idx = j;
            min_dist = sum_dist;
        }
    }

    //for (int i = 0 ; i < dim + 2 ; i++)
    //    ret[idx * (2 + dim) + i] = i;
 
    //ret[idx * (2 + dim) + 0] = center_idx;
    //ret[idx * (2 + dim) + 1] = 1;
    //ret[idx * (2 + dim) + 2] = 5;
    
    atomicAdd(&(ret[center_idx*(1+dim) + 0]),1);

    for (int i = 0 ; i < dim ; i++)
        atomicAdd(&(ret[center_idx*(1+dim) + i + 1]),data[idx*dim + i]);
}

__global__ void closest_with_reduce(float* ret, float* data, float* centers, int K, int dim, int N)
{
	int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if(idx >= N) 
        return;

    float min_dist = 99999999;
    int   center_idx = 0 ;

    for (int j = 0 ; j < K ; j++)
    {
        float sum_dist = 0 ;
        float diff ;    

        for (int i = 0 ; i < dim ; i++)
        {
            diff =  centers[j*dim + i] - data[idx*dim + i];
            sum_dist += diff * diff;
        } 

        if (sum_dist < min_dist)
        {
            center_idx = j;
            min_dist = sum_dist;
        }
    }

    //for (int i = 0 ; i < dim + 2 ; i++)
    //    ret[idx * (2 + dim) + i] = i;
 
    //ret[idx * (2 + dim) + 0] = center_idx;
    //ret[idx * (2 + dim) + 1] = 1;
    //ret[idx * (2 + dim) + 2] = 5;
    
    atomicAdd(&(ret[center_idx*(1+dim) + 0]),1);

    for (int i = 0 ; i < dim ; i++)
        atomicAdd(&(ret[center_idx*(1+dim) + i + 1]),data[idx*dim + i]);
}



//__global__ void closest(float* ret, unsigned char* data, float* centers, int K, int dim, int N)
__global__ void closest(float* ret, float* data, float* centers, int K, int dim, int N)
{
	int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if(idx >= N) 
        return;

    float min_dist = 99999999;
    int   center_idx = 0 ;

    for (int j = 0 ; j < K ; j++)
    {
        float sum_dist = 0 ;
        float diff ;    

        for (int i = 0 ; i < dim ; i++)
        {
            diff =  centers[j*dim + i] - data[idx*dim + i];
            sum_dist += diff * diff;
        } 

        if (sum_dist < min_dist)
        {
            center_idx = j;
            min_dist = sum_dist;
        }
    }

    //for (int i = 0 ; i < dim + 2 ; i++)
    //    ret[idx * (2 + dim) + i] = i;
 
    ret[idx * (2 + dim) + 0] = center_idx;
    ret[idx * (2 + dim) + 1] = 1;
    //ret[idx * (2 + dim) + 2] = 5;
    
    for (int i = 0 ; i < dim ; i++)
        ret[idx * (2 + dim) + 2 + i] = data[idx*dim + i];
}

__global__ void local_reduce(float* ret, float* data, int K , int dim, int N)
{
	int idx = blockIdx.x * blockDim.x + threadIdx.x;

    if(idx >= N) 
        return;


    int target_idx = data[idx*(2+dim) + 0];

//    atomicAdd(&(ret[target_idx*(1+dim) + 0]),data[idx*(2+dim) + 1]);
    
    for (int i = 0 ; i < dim+1 ; i++)
        atomicAdd(&(ret[target_idx*(1+dim) + i]),data[idx*(2+dim) + 1 + i]);



}
}

