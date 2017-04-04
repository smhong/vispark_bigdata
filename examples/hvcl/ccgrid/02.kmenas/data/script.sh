./kmeans_data 3000000 784 40 1 
./kmeans_data 3000000 784 80 1
./kmeans_data 3000000 784 160 1
./kmeans_data 3000000 784 320 1

python kmeans_uploader.py km_3000000_784_40.txt 3000000 64  
python kmeans_uploader.py km_3000000_784_80.txt 3000000 64  
python kmeans_uploader.py km_3000000_784_160.txt 3000000 64  
python kmeans_uploader.py km_3000000_784_320.txt 3000000 64  




