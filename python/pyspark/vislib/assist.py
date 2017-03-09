import os
import time
import numpy

colors = [[136, 46, 114], [177, 120, 166], [214, 193, 222], [25, 101, 176], [82, 137, 199], [123, 175, 222], [78, 178, 101], [144, 201, 135], [202, 224, 171], [247, 238, 85], [246, 193, 65], [241, 147, 45], [232, 96, 28], [220, 5, 12]]

class profiler(object):
   
    _path = ""
    _prefix = ""

    _init_time = 0
    _close_time = 0

    _work = True
 
    def __init__(self,path="/home/whchoi/profile",prefix="vispark_prof",work=True):

        self._path = path
        self._prefix = prefix
        self._work = work


    def init(self,rm_data=False):

        if self._work:
            print "Vispark Profiler will be recorded log files on ", self._path

            if not os.path.exists(self._path):
                os.makedirs(self._path)

            if rm_data :
                try :
                    os.system("rm -rf %s/%s_*"%(self._path,self._prefix))
                except :
                    pass

        self._init_time = time.time()

    def start(self,tag="default", pid =None):

        if self._work :
            pid = os.getpid() if pid == None else pid 

            with open("%s/%s_%d"%(self._path,self._prefix,pid), "a") as myfile:
                myfile.write("%s %0.3f,"%(tag,time.time()))

    
    def stop(self,tag="default",pid =None):

        if self._work :
            pid = os.getpid() if pid == None else pid 
            
            with open("%s/%s_%d"%(self._path,self._prefix,pid), "a") as myfile:
                myfile.write("%s %0.3f\n"%(tag,time.time()))


    def close(self):
        self._close_time = time.time()
    
    def draw(self,filename="profile_result.png",width = 1600, height = 30, tic = 10):

        if self._work == False:
            return 

        _files = os.listdir(self._path)

        #print _files
        #print self._prefix
        #_files = sorted(os.listdir(self._path))
        files = sorted([x for x in _files if x.find(self._prefix) != -1 and x.find(".") == -1])

        #print files 

        data_dict = {}
        tag_dict = {}

        for name in files:
            data = open("%s/%s"%(self._path, name)).readlines()
        
            data_start = [elem[:elem.find(',')] for elem in data]
            data_end   = [elem[elem.find(',')+1:] for elem in data]

            data_start_tag = [elem[:elem.find(' ')] for elem in data_start]
            data_end_tag = [elem[:elem.find(' ')] for elem in data_end]

            err = 0
            for i in range(len(data_start_tag)):
                if data_start_tag[i] != data_end_tag[i]:
                    err += 1

            if err > 0:
                print "Profiler fail to log reading"
                #return -1 
            
            data_dict[name] = []
            tag_dict[name]  = []

            for i in range(len(data_start)):
                str_time = data_start[i][data_start[i].find(' ')+1:]
                end_time = data_end[i][data_end[i].find(' ')+1:]
    
                data_dict[name].append([float(str_time),float(end_time)])
                tag_dict[name].append(data_start_tag[i])

        #print data_dict

        nametag = 100
        tic_margin = 10
        margin  = 5

        Min = self._init_time
        Max = self._close_time
    
        result_array = numpy.ones((len(files)*(height+margin) + margin + tic_margin, nametag+width+margin*2, 3), dtype=numpy.uint8) * 255

        alpha = 0.25

        def apply_alpha(col,mul):
            return [int(col[0]*alpha*mul),int(col[1]*alpha*mul) , int(col[2]*alpha*mul)]
 
        # draw data 
        for elem in range(len(files)):
            name = files[elem]
            #tags = tag_dict[name]
            for cnt in range(len(data_dict[name])):
                task = data_dict[name][cnt]
                #tag  = tag_dict[name][cnt]

                start, end = task
                x_start = int((start-Min)/(Max-Min)*width) + margin + nametag
                x_end   = int((end  -Min)/(Max-Min)*width) + margin + nametag
                y_start = int(margin + elem * (margin + height))
                y_end   = int(margin + elem * (margin + height) + height) 
    

                colmap = numpy.array([apply_alpha(colors[elem % len(colors)],cnt%4)] * (y_end-y_start) * (x_end-x_start)).reshape(y_end-y_start, x_end-x_start, 3)
                result_array[y_start:y_end, x_start:x_end, :] = colmap

        # draw tic
        tics = range(int(Min), int(Max), tic)

        for elem in tics:
            x_start = int((elem-Min)/(Max-Min)*width) + margin + nametag - 1
            x_end   = int((elem-Min)/(Max-Min)*width) + margin + nametag + 1
            y_start = int(-7)
            y_end   = int(-1)

            colmap = numpy.array([[0,0,0]] * (y_end-y_start) * (x_end-x_start)).reshape(y_end-y_start, x_end-x_start, 3)
            result_array[y_start:y_end, x_start:x_end, :] = colmap

        import Image
        from PIL import ImageFont
        from PIL import ImageDraw 

        # Write file name
        img = Image.fromarray(result_array)
        draw = ImageDraw.Draw(img)
        fonts_path = '/usr/share/fonts/truetype/freefont/'
        font = ImageFont.truetype(os.path.join(fonts_path, 'FreeSerif.ttf'), 24)

        for elem in range(len(files)):
            name = files[elem]
            y_start = int(margin + elem * (margin + height))
            draw.text((margin, y_start),name,(0,0,0),font=font)

        img.save(filename)
        print "Vispark Profiler writes log files on %s/%s",os.getcwd(),filename
  
        return 0


if __name__ == "__main__":
    
    profiler = profiler()
    profiler.draw()
