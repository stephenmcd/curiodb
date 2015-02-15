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
        args = sys.argv[1:]
        if args == ["lol"]:
            args = ["msetnx"] + map(str, range(30000))
        sock.send("%s\n" % " ".join(args))
        print sock.recv(1024)
        break
