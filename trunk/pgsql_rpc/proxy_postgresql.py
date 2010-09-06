#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: pgproxy.py
# Date: 2010-07-13
# Author: gashero

"""
PostgreSQL proxy to research its protocol
"""

import os
import sys
import re
import time
import struct
import socket
import select
import traceback
import threading

import pgpro

PGSERVER=('localhost',5432)
LOOPING=True

def trans(sock_up,sock_down):
    global LOOPING
    sock_client,addr=sock_down.accept()
    print 'Client: %s:%d'%addr
    sock_up.connect(PGSERVER)
    while LOOPING:
        readlist,writelist,errorlist=select.select([sock_client.fileno(),sock_up.fileno()],[],[])
        if sock_client.fileno() in readlist:
            data=sock_client.recv(1048576)
            if data:
                sock_up.send(data)
                print '[U++]: %s'%repr(data)
            else:
                print '[U++]: close'
                sock_client.close()
                sock_up.close()
                break
        if sock_up.fileno() in readlist:
            data=sock_up.recv(1048576)
            if data:
                sock_client.send(data)
                print '[D--]: %s'%repr(data)
                _buffer=data
                while True:
                    packet,mtype,_buffer=pgpro.extract_packet(_buffer)
                    if packet:
                        print '[D--XX]: %s %s'%(repr(mtype),repr(packet))
                    else:
                        print '[D--XX]: left %d byte'%len(_buffer)
                        break
            else:
                print '[D--]: close'
                sock_up.close()
                sock_client.close()
                break
    LOOPING=False
    return

def main():
    sock_up=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    #sock_up.connect(PGSERVER)
    sock_down=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock_down.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    sock_down.bind(('0.0.0.0',5440))
    sock_down.listen(5)
    thrd=threading.Thread(target=trans,args=(sock_up,sock_down))
    thrd.setDaemon(True)
    thrd.start()
    thrd=threading.Thread(target=trans,args=(sock_up,sock_down))
    thrd.setDaemon(True)
    thrd.start()
    try:
        while LOOPING:
            time.sleep(0.5)
    except KeyboardInterrupt:
        print
    return

if __name__=='__main__':
    main()
