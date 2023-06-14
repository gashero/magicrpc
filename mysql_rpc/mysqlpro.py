# -*- coding: UTF-8 -*-
# File: mysqlpro.py
# Date: 2020-11-27
# Author: gashero

import os
import sys
import time
import socket
import threading

class MySQLServer(object):
    """接收请求的服务器，处理所有Socket部分"""

    def __init__(self,host='0.0.0.0',port=3306):
        self.host=host
        self.port=port
        self.init_server()
        return

    def init_server(self):
        self.ssock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        self.ssock.setsockopt(socket.SOL_SOCKET,
                socket.SO_REUSEADDR,1)
        self.ssock.bind((self.host,self.port))
        self.ssock.listen(5)
        return

    def serve_forever(self):
        """持续接受请求，多线程模式"""
        print('Serving at %s:%d'%(self.host,self.port))
        while True:
            try:
                csock,peerinfo=self.ssock.accept()
                msqconn=MySQLConnection(csock,peerinfo,csock.send)
                thrd=threading.Thread(target=self.data_recv,args=(csock,msqconn))
                thrd.setDaemon(True)
                thrd.start()
            except KeyboardInterrupt:
                break
        return

    def data_recv(self,csock,msqconn):
        """持续接收数据的线程"""
        while True:
            chunk=csock.recv(4096)
            if not chunk:   #连接断开了
                break
            msqconn.dataReceived(chunk)
        return

class MySQLConnection(object):
    """单一连接的处理，与Socket隔绝"""

    def __init__(self,csock,peerinfo,sendfunc):
        self.csock=csock        #不建议使用
        self.peerinfo=peerinfo
        self.send=sendfunc
        print(csock,peerinfo)
        return

    def dataReceived(self,chunk):
        print(repr(chunk))
        return

    def handshake(self):
        """刚连接上的握手包"""
        self.send()
        return

## Unittest ####################################################################

import unittest

class TestMySQL(unittest.TestCase):

    def setUp(self):
        return

    def tearDown(self):
        return

if __name__=='__main__':
    #unittest.main()
    server=MySQLServer(port=3344)
    server.serve_forever()
