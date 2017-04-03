#!/usr/bin/python

import time
import math
import sys
from socket import *
import os
import string
from gpu_worker import send_signal,send_signal_socket,get_tag_code

msg_size = 2*1024

server_list=[]
server_list_file="%s/conf/slaves"%(os.environ.get('SPARK_HOME'))

#print server_list_file



def get_command(msg):
    return msg[:msg.find('**')], msg[msg.find('**')+2:]

with open(server_list_file) as f:
    lines =f.readlines()

    for elem in lines:
        elem = elem.strip()

        if elem.find('#')==0:
            continue
        if elem.find('\n')!=-1: 
            elem = elem[:-2]
       
        if len(elem) > 0: 
            server_list.append(elem)

#print server_list

                
port = int(3939)
if len(sys.argv) > 1: 
    port = int(sys.argv[1])

svrsock = socket(AF_INET, SOCK_STREAM)
svrsock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
host_name = gethostname()

svrsock.bind((host_name, port))
svrsock.listen(0)


loop = True

#host_name = host_name.replace('emerald','ib')
new_list=[]
for elem in server_list:
    new_list.append(elem.replace('ib','emerald'))
server_list = new_list

print server_list
print "[%s:%d] initialization"%(host_name,port)

activate_dict = {}

while loop:

    conn, addr = svrsock.accept()
    #print "[%s] Connected to %s"%(host_name,addr)

    msg = conn.recv(msg_size)

    command , msg = get_command(msg)

    if command == 'wake':
        t1 = time.time()        
        activate_code, msg = get_command(msg)
        #print activate_code
        requester, msg = get_command(msg)
        print "[%s] wake up by %s "%(host_name,requester)
        #activate_code = get_tag_code(msg)
        
        #if activate_code in activate_dict.keys():
        #    #print "Used Activate Code", activate_code
        #    continue

        activate_dict[activate_code] = time.time()

        for elem in server_list:
            if elem != host_name:
                #print "[%s] send message of _follow_ o [%s]"%(host_name,elem)
                send_signal_socket('follow',activate_code,elem,requester)

        #print "[%s] is now ready %f"%(host_name,time.time())
        #send_signal("shuffle_ready",activate_code)
        #print "Wake",requester
        #print "[%s] send message of shuffle_ready"%(host_name)


        send_signal("shuffle_ready",activate_code,[requester],reply=True)
        sending_str = "done**"
        sending_str += '0'*msg_size
        conn.send(sending_str[:msg_size])
        t2 = time.time()        
        
        print t2-t1
    
    if command == 'follow':
        
        #activate_code = get_tag_code(msg)
        activate_code, msg = get_command(msg)
        requester, msg = get_command(msg)
       
        #if activate_code in activate_dict.keys():
        #    #print "Used Activate Code", activate_code
        #    continue

        activate_dict[activate_code] = time.time()
        #print "[%s] send message of shuffle_ready"%(host_name)
        #print "Follow",requester
        send_signal("shuffle_ready",activate_code,[requester],reply=True)
        #print "[%s] is now ready %f"%(host_name,time.time())
                   
 
