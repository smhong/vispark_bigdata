#include<stdio.h>
#include<math.h>
#include<iostream>
#include<stdlib.h>
using namespace std;

typedef unsigned int uint;

float random_between_two_int(float min, float max)    
{    
    return (min + 1) + (((float) rand()) / (float) RAND_MAX) * (max - (min + 1));    
}

int main(int argc, char** argv)
{

    if (argc < 5)
    {
        printf("Usage :: %s numPoints numDim numK numSigma [numSplit]\n",argv[0]);
        return 0;
    }
   
 
    uint numPoints = atoi(argv[1]);
   
    uint numDim = atoi(argv[2]);

    uint numKPoints = atoi(argv[3]);

    float sigma = 0.15;
   
    uint numSplit = 1;
    
    if (argc >= 6)
        numSplit = atoi(argv[5]);
 
    int spa_rate = 90;

    float d_min = 0;
    float d_max = 16;

    float **data  = new float*[numPoints];
    for (uint i = 0 ; i < numPoints; i++)
        data[i]  = new float[numDim];

 
    float **Kdata  = new float*[numKPoints];
    
    uint *Klabel  = new uint[numPoints];

    for (uint i = 0 ; i < numKPoints; i++)
    {
        Kdata[i]  = new float[numDim];
        
        for (uint j = 0 ; j < numDim ; j++)
            if (rand() % 100 > spa_rate ) 
                Kdata[i][j] = floor(random_between_two_int(d_min,d_max));
            else 
                Kdata[i][j] = 0;

/*    
        for (uint j = 0 ; j < 10; j++){    
            for (uint k= 0 ; k < 10 ; k++)
                printf("%03d ",(int)Kdata[i][j*10+k]);
            printf("\n");
        }
        printf("\n"); 
*/
    } 
   
 

    for (uint i = 0 ; i < numPoints; i++)
    {
        uint targetK = rand()%numKPoints;

        Klabel[i] = targetK;   
    
        for (uint j = 0 ; j < numDim ; j++)
        {
            data[i][j] = floor(Kdata[targetK][j] + (rand()%(int)(d_max-d_min)-(d_max-d_min)/2)*sigma);
            if (data[i][j] < d_min) data[i][j] = d_min;
            if (data[i][j] > d_max) data[i][j] = d_max;
        }
   }


    if(numSplit == 1)
    {
        char filename[100];
        sprintf(filename,"km_%d_%d_%d.txt",numPoints,numDim,numKPoints);

        FILE* fp = fopen(filename,"w");

        for (uint i = 0 ; i < numPoints; i++)
        {
            fprintf(fp,"%d ",Klabel[i]);
            for (uint j = 0 ; j < numDim;  j++)
                fprintf(fp,"%d ",(int)data[i][j]);
            fprintf(fp,"\n");
        }  
    }
    else
    {
        for (uint k = 0 ; k < numSplit ; k++)
        {
            char filename[100];
            sprintf(filename,"km_%d_%d_%d_%03d.txt",numPoints,numDim,numKPoints,k);

            FILE* fp = fopen(filename,"w");

            int chunksize = numPoints/numSplit;

            int start = k*chunksize;
            int end   = (k+1)*chunksize;

            if (k == numSplit -1) end = numPoints;

            for (uint i = start ; i < end ; i++)
            {
                fprintf(fp,"%d ",Klabel[i]);
                for (uint j = 0 ; j < numDim;  j++)
                    fprintf(fp,"%d ",(int)data[i][j]);
                fprintf(fp,"\n");
            }  
        }
    }

    char filename1[100];
    sprintf(filename1,"km_label_%d_%d_%d.txt",numPoints,numDim,numKPoints);

    FILE* fp1 = fopen(filename1,"w");

    for (uint i = 0 ; i < numKPoints; i++)
    {
        fprintf(fp1,"%d ",i);
        for (uint j = 0 ; j < numDim;  j++)
            fprintf(fp1,"%d ",(int)Kdata[i][j]);
        fprintf(fp1,"\n");
    }  
    

    return 0;

    
}


