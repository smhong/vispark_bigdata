#include<stdio.h>
#include<math.h>
#include<iostream>
#include<stdlib.h>


float random_between_two_int(float min, float max)    
{    
    return (max - min)*(-0.5) + (((float) rand()) / (float) RAND_MAX) * (max - (min));    
}


int main (int argc, char *argv[])
{
    int elems = atoi(argv[1]);
    int dim   = atoi(argv[2]);

    float sigma = 0.15; 

    //srand(time(NULL));
    srand(222);

    //x1,x2,....xN,C
    float *plain = new float[dim+1];

    for (int i = 0 ; i < dim+1 ; i++)
        plain[i] = random_between_two_int(-10.0,10.0);

    for (int i = 0 ; i < dim+1 ; i++)
        printf("%0.3f ",plain[i]);
    printf("\n");


    float **data  = new float*[elems];
    //<label, x1,x2,....xN) 
    for (uint i = 0 ; i < elems; i++)
        data[i]  = new float[dim+1];


    int numTrue  = 0;
    int numFalse = 0;
    
    for (int i = 0 ; i < elems ; i++)
    { 
        for (int j = 1 ; j < dim+1 ; j++)
            data[i][j] = random_between_two_int(-1.0,1.0);
        
        //C
        float result = data[i][dim];
        for (int j = 0 ; j < dim ; j++)
            result += data[i][j+1]*plain[j];
         
        data[i][0] = result > 0 ? 1 : 0;
        
        if (data[i][0] > 0)
            numTrue++;
        else 
            numFalse++;
        
    }
    
    printf(" %d / %d \n", numTrue,numFalse);

    char filename[100];
    sprintf(filename,"lr_%d_%d.txt",elems,dim);

    FILE* fp = fopen(filename,"w");

    for (int i = 0 ; i < elems; i++)
    {
        for (int j = 0 ; j < dim+1;  j++)
            fprintf(fp,"%0.3f ",(data[i][j]));
        fprintf(fp,"\n");
    }  


}
    
