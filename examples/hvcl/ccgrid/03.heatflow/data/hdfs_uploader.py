#!/usr/bin/python
import Image, numpy
from hdfs import InsecureClient
import time
import math
import os


def main():
    import getpass
    client = InsecureClient('http://emerald.world:50070', user=getpass.getuser())

    import sys
    if len(sys.argv) < 2:
        print "USAGE (image case) : %s ImageFileName [X split] [Y split] [Z split] [halo=0]"%(sys.argv[0])
        print "USAGE  (raw case)  : %s ImageFileName X Y Z Dtype [X split] [Y split] [Z split] [halo=0]"%(sys.argv[0])
        exit(1)


    FileName = sys.argv[1]
    BaseName = FileName[:FileName.find('.')]
    extension = FileName[FileName.find('.')+1:]

    if extension != 'raw':
        data = numpy.array(Image.open(FileName))



        Y, X = data.shape
        data_size = X * Y
        num_of_unit = data_size / (128.0 * (1024**2))
#        X_split = Y_split = int(math.ceil(pow(num_of_unit,1/2.0)))
    
        split_info = []
        file_info = []
    
        X_split = X_split if len(sys.argv) < 3 else int(sys.argv[2])
        Y_split = Y_split if len(sys.argv) < 4 else int(sys.argv[3])
    
        client.delete(BaseName, True)
        client.makedirs(BaseName)
        
        # for 2D image data
        if len(data.shape) < 3:
            for jj in range(X_split):
                for ii in range(Y_split):
                    split_info.append([ii, jj])
                    file_info.append(BaseName+'_'+str(ii*X_split+jj))
    
            for i in range(len(split_info)):
    
                elem = {}
                elem['x'] = [(split_info[i][1])*data.shape[1]/X_split,(split_info[i][1]+1)*data.shape[1]/X_split]
                elem['y'] = [(split_info[i][0])*data.shape[0]/Y_split,(split_info[i][0]+1)*data.shape[0]/Y_split]
            
                if split_info[i][0] + 1== Y_split:
                    elem['y'][1] = data.shape[0]
                if split_info[i][1] + 1== X_split:
                    elem['x'][1] = data.shape[1]
            
            
                local_data = data[elem['y'][0]:elem['y'][1], elem['x'][0]:elem['x'][1]]
    
                #print elem, local_data.shape
 #           Image.fromarray(local_data).save(file_info[i]+'.png')

                client.write(BaseName+'/'+file_info[i],local_data.tostring(),replication=8)
    
                
            metaStr = "X : %s\nY : %s\n"%(data.shape[1], data.shape[0])
            metaStr += "X split : %s\nY split : %s\n"%(X_split, Y_split)
            for elem in range(len(file_info)):
                metaStr += "%s\n"%(file_info[elem])
        
            client.write(BaseName+'/' + '.meta',encoding='utf-8',data=metaStr,replication=8)
    

    # for 3D data (later)
    else:
        X = int(sys.argv[2])
        Y = int(sys.argv[3])
        Z = int(sys.argv[4])

        ori_dtype = sys.argv[5]
        if ori_dtype=='float':
            dtype = numpy.float32
        elif ori_dtype=='uchar':
            dtype = numpy.uint8

        if ori_dtype == 'float':
            elem_bytes = 4
        elif ori_dtype == 'uchar':
            elem_bytes = 1

        data_size = elem_bytes * X * Y * Z
        # calc data unit
        num_of_unit = data_size / (128.0 * (1024**2))
        

        import math
        #print num_of_unit

        X_split = Y_split = Z_split = int(math.ceil(pow(num_of_unit,1/3.0)))

        #Z_split *= 2

        
        start_time = time.time()


 
        X_split = X_split if len(sys.argv) < 7 else int(sys.argv[6])
        Y_split = Y_split if len(sys.argv) < 8 else int(sys.argv[7])
        Z_split = Z_split if len(sys.argv) < 9 else int(sys.argv[8])
    
        #data = numpy.fromstring(open(FileName).read(), dtype=dtype).reshape(Z, Y, X)
        file_pointer = open(FileName)
    
        split_info = []
        file_info = []
    
    
        client.delete(BaseName, True)
        client.makedirs(BaseName)


        # resume uploading
        #client.delete(BaseName+'/'+BaseName+'_928', True)
        #client.makedirs(BaseName)
        
        for ii in range(Z_split):
            for jj in range(Y_split):
                for kk in range(X_split):
                    # resume uploading
                    #if ii*X_split*Y_split+jj*X_split+kk >= 2048:
                        #split_info.append([ii, jj, kk])
                        #file_info.append(BaseName+'_'+str(ii*X_split*Y_split+jj*X_split+kk))

                    split_info.append([ii, jj, kk])
                    file_info.append(BaseName+'_'+str(ii*X_split*Y_split+jj*X_split+kk))

    
        prev_z_val = -1

        os.system("mkdir -p tmp_directory")
        abs_path = os.path.abspath(os.getcwd())
        server_list = ['ib1', 'ib2', 'ib3', 'ib4', 'ib5', 'ib6', 'ib7', 'ib8']

        for i in range(len(split_info)):

            elem = {}
            elem['x'] = [(split_info[i][2])*X/X_split,(split_info[i][2]+1)*X/X_split]
            elem['y'] = [(split_info[i][1])*Y/Y_split,(split_info[i][1]+1)*Y/Y_split]
            elem['z'] = [(split_info[i][0])*Z/Z_split,(split_info[i][0]+1)*Z/Z_split]

            if split_info[i][0] + 1== Z_split:
                elem['z'][1] = Z
            if split_info[i][1] + 1== Y_split:
                elem['y'][1] = Y
            if split_info[i][2] + 1== X_split:
                elem['x'][1] = X


            
            # update buffer
            if split_info[i][0] != prev_z_val:
                prev = time.time()
                depth = elem['z'][1] - elem['z'][0]
                file_pointer.seek(elem['z'][0]*Y*X)
                data = numpy.fromstring(file_pointer.read(depth*Y*X*elem_bytes),dtype=dtype).reshape(depth, Y, X)
                prev_z_val = split_info[i][0]
                
                print "Loading time : %f"%(time.time()-prev)
        
        
        
            prev = time.time()
            local_data = data[:,elem['y'][0]:elem['y'][1], elem['x'][0]:elem['x'][1]]
            print "Split data : %f"%(time.time()-prev)

            #print elem, local_data.shape
#           Image.fromarray(local_data).save(file_info[i]+'.png')
            prev = time.time()
            #client.write(BaseName+'/'+file_info[i],local_data.tostring())
            # upload data using pyHDFS
            #client.write(BaseName+'/'+file_info[i],local_data.tostring(), blocksize=128*(1024**2), replication=3)

            # upload data using hdfs dfs -put
            with open("tmp_directory/%s"%file_info[i], "wb") as fp:
                fp.write(local_data.tostring())

            f_size = os.path.getsize("tmp_directory/%s"%file_info[i])
    
            if f_size < 1024*1014:
                f_size = 1024*1024
    
            if f_size % 512 != 0:
                f_size = f_size + 512 - (f_size % 512)
            #print f_size

            
            os.system("ssh %s hdfs dfs -D dfs.block.size=%d -put %s %s"%(server_list[i%len(server_list)],f_size, "%s/tmp_directory/%s"%(abs_path, file_info[i]), BaseName))
            
            print "Uploading : %f"%(time.time()-prev)
            
        metaStr = "X : %s\nY : %s\nZ : %s\n"%(X, Y, Z)
        metaStr += "Dtype : %s\n"%ori_dtype
        metaStr += "X split : %s\nY split : %s\nZ split : %s\n"%(X_split, Y_split, Z_split)
        for elem in range(len(file_info)):
            metaStr += "%s\n"%(file_info[elem])
    
        client.write(BaseName+'/' + '.meta',encoding='utf-8',data=metaStr, blocksize=2*(1024**2), replication=3)
        print "TOTAL TIME : %f"%(time.time()-start_time)

        os.system("rm -rf tmp_directory")
        
    
if __name__ == "__main__":
    main()
