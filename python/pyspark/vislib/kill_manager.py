#!/usr/bin/python

from socket import *
msg_size = 4*1024

address = "/tmp/gpu_manager"
clisock = socket(AF_UNIX, SOCK_STREAM)
clisock.connect(address)

sending_str = "%s**"%('drop')
sending_str += '0'*msg_size
clisock.send(sending_str[:msg_size])


