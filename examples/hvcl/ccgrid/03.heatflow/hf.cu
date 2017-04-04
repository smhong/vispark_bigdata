

extern "C" {

__global__ void heatf(float* out, float* in, int len_x, int len_y, int len_z, int nx, int ny) {
    int idx_x = blockIdx.x * blockDim.x + threadIdx.x;   
    int idx_y = blockIdx.y * blockDim.y + threadIdx.y;   
    int idx_z = blockIdx.z * blockDim.z + threadIdx.z;   

    if(idx_x >= len_x-1 || idx_y >= len_y-1 || idx_z >= len_z-1)
        return;
    if(idx_x < 1 || idx_y < 1 || idx_z < 1)
        return;

    int idx   = (idx_z+0) * ny * nx  + (idx_y+0) * nx + (idx_x+0);
    int idx_u = (idx_z+0) * ny * nx  + (idx_y+1) * nx + (idx_x+0);
    int idx_d = (idx_z+0) * ny * nx  + (idx_y-1) * nx + (idx_x+0);
    int idx_r = (idx_z+0) * ny * nx  + (idx_y+0) * nx + (idx_x+1);
    int idx_l = (idx_z+0) * ny * nx  + (idx_y+0) * nx + (idx_x-1);
    int idx_t = (idx_z+1) * ny * nx  + (idx_y+0) * nx + (idx_x+0);
    int idx_b = (idx_z-1) * ny * nx  + (idx_y+0) * nx + (idx_x+0);
    
    float u = in[idx_u];
    float d = in[idx_d];
    float r = in[idx_r];
    float l = in[idx_l];
    float t = in[idx_t];
    float b = in[idx_b];
    
    float c = in[idx];

    float val = c + 0.25 * (u+d+r+l+t+b - 6.0*c);

    out[idx] = val;
}


}
