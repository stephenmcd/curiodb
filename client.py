#!/usr/bin/env python

import sys
import socket
import time


host = "localhost"
port = 9999
timeout = 30
now = time.time()


while True:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.connect((host, port))
    except socket.error:
        if time.time() - now < timeout:
            time.sleep(.1)
        else:
            print "Connect timeout"
            break
    else:
        sock.send("%s\n" % " ".join(sys.argv[1:]))
        print sock.recv(1024)
        break
