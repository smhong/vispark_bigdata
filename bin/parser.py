#!/bin/bash

import sys



if __name__ == "__main__":
    if len(sys.argv) < 3:
        print "usage : %s infile_name outfile_name"%(argv[0])
        quit()
    argv = sys.argv
    lines = open(argv[1]).readlines()
    
    out_lines = ''
    modelview_flag = False
    seed_flag = False
    for elem in lines:
        if elem.find('inv_modelview') != -1:
            modelview_flag = True
        if elem.replace(' ','').find('#') == 0:
            #out_lines = out_lines + elem 
            continue
        elif elem.find('.vmap(') == -1:
            out_lines = out_lines + elem 
            continue
        elif elem == '':
            out_lines = out_lines + '\n'
            continue

        argv = elem.replace(' ','')
        
        result  = ""
        for elem in range(elem.find(argv[0])):
            result = result + ' '
        r_idx = argv.find('.vmap(') + 6
        result = result + argv[:r_idx]
        #result = result.replace('vmap', 'map')
    
        # find funcname
        e_idx = r_idx + argv[r_idx:].find('(')
        result = result + "\"" +argv[r_idx: e_idx] + "\","


        # find functions args
        result = result + 'func_args = ['
        while(True):
            if argv[e_idx+1:].find(')') < argv[e_idx+1:].find(',') or argv[e_idx+1:].find(',') == -1:
                s_idx = e_idx+1
                e_idx = e_idx + argv[s_idx:].find(')') + 1

                result = result + "'" + argv[s_idx:e_idx] + "']"

                break




            s_idx = e_idx+1
            e_idx = e_idx + argv[s_idx:].find(',') + 1



            result = result + "'" + argv[s_idx:e_idx] + "',"

    
        # attach additional args
        if modelview_flag == True:
            result = result + ",etc_args={'inv_modelview':inv_modelview, 'modelview':modelview},"
        else:
            result = result + ",etc_args={},"
    
        # attach  range
        if argv.find('.range(') != -1:
            r_idx = argv.find('.range(') + 7
            start = r_idx + argv[r_idx:].find('x=')+2
            mid   = start + argv[start:].find(':')
            if argv[mid:].find('y=') != -1:
                end   = mid + argv[mid:].find(',')
            else:
                end   = mid + argv[mid:].find(')')
        
            x_start = int(argv[start:mid])
            x_end   = int(argv[mid+1:end])
    
            if argv[end:].find('y=') != -1:
        
                start = end + argv[end:].find('y=')+2
                mid   = start + argv[start:].find(':')

                if argv[mid:].find('z=') != -1:
                    end   = mid + argv[mid:].find(',')
                else:
                    end   = mid + argv[mid:].find(')')
        
                y_start = int(argv[start:mid])
                y_end   = int(argv[mid+1:end])
                if argv[end:].find('z=') != -1:
        
                    start = end + argv[end:].find('z=')+2
                    mid   = start + argv[start:].find(':')
                    end   = mid + argv[mid:].find(')')
        
                    z_start = int(argv[start:mid])
                    z_end   = int(argv[mid+1:end])
        
        
                    result = result + "work_range={'%s':[%d, %d],'%s':[%d,%d],'%s':[%d,%d]},"%('x',x_start,x_end,'y',y_start,y_end,'z',z_start,z_end)
                else:
                    result = result + "work_range={'%s':[%d, %d],'%s':[%d,%d]},"%('x',x_start,x_end,'y',y_start,y_end)
            else:
                result = result + "work_range={'%s':[%d, %d]},"%('x',x_start,x_end)
        else:
            result = result + "work_range=None,"

        
        # halo
        if argv.find('.halo(') != -1:
            r_idx = argv.find('.halo(') + 6
            start = r_idx + argv[r_idx:].find(',') + 1
            end   = start + argv[start:].find(')')
            halo = argv[start:end]
            
            if halo[0] == 's':
                result = result + "halo=%s, comm_type='simple',"%(halo[1:])
            else:
                result = result + "halo=%s,"%halo
        else:
            result = result + "halo=0,"

        target = '.extern_code('
        if argv.find(target) != -1:
            r_idx = argv.find(target) + len(target)
            start = r_idx 
            end   = start + argv[start:].find(')')
            code  = argv[start:end]

            result = result + "extern_code=%s,"%code

        else:
            result = result + "extern_code=None,"


        target = '.output('
        if argv.find(target) != -1:
            r_idx = argv.find(target) + len(target)
            start = r_idx 
            end   = start + argv[start:].find(')')
            code  = argv[start:end]

            dtype = code[:code.find(',')]
            cnt   = code[code.find(',')+1:]


            result = result + "output=[%s,%d],"%(dtype, int(cnt))

        else:
            result = result + "output=[],"
            
    
        result = result + 'code=\"\"\"%s\"\"\", main_data=locals())'%(open(sys.argv[1]).read())

        out_lines = out_lines + result + '\n'


    #print argv[x_idx:]



    open(sys.argv[2],'w').write(out_lines)

    
