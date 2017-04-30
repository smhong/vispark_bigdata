from socket import *
import sys

target=sys.argv[1]
port=int(sys.argv[2])
msg_size=int(sys.argv[3])

key="kill_message"
command="suicide"

sending_str="%s**%s**"%(key,command)
sending_str+='0'*msg_size

clisock = socket(AF_INET, SOCK_STREAM)
clisock.connect((target,port))
clisock.send(sending_str[:msg_size])
clisock.close()

#print "Kill %s,%d with %s"%(target,port,sending_str[:20])
#msg_size = 2*1024


