# -*- coding: UTF-8 -*-
# File: mysqlpro.py
# Date: 2020-11-27
# Author: gashero

import os
import sys
import time
import socket
import threading
import random
import struct

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
            chunk=csock.recv(1048576)
            if not chunk:   #连接断开了
                print('close')
                break
            msqconn.dataReceived(chunk)
        return

class MySQLConnection(object):
    """单一连接的处理，与Socket隔绝"""

    def __init__(self,csock,peerinfo,sendfunc):
        self.csock=csock        #不建议使用
        self.peerinfo=peerinfo
        self.send=sendfunc
        print('Income connection',peerinfo)
        self.handshake()
        return

    def dataReceived(self,chunk):
        pktlen=struct.unpack('I',chunk[:3]+b'\x00')
        reqid=ord(chunk[3])
        reqtype=chunk[4]
        req=chunk[5:]
        print(pktlen,reqtype,req,repr(chunk))
        if req=='select @@version_comment limit 1':
            hh=b"\x01\x00\x00\x01\x01'\x00\x00\x02\x03def\x00\x00\x00\x11@@version_comment\x00\x0c!\x00\x18\x00\x00\x00\xfd\x00\x00\x1f\x00\x00\t\x00\x00\x03\x08(Ubuntu)\x07\x00\x00\x04\xfe\x00\x00\x02\x00\x00\x00"
            self.send(hh)
        elif req==b'\x01':  #断开请求
            self.csock.close()
        else:
            #pktid必须处理，否则连接会断开
            resp=self.okpacket(pktid=reqid+1)
            self.send(resp)
        return

    def handshake(self):
        """刚连接上的握手包"""
        salt=random.random()*2**64
        hh1=b'[\x00\x00\x00\n5.7.33-0ubuntu0.16.04.1\x00\x05\x00\x00\x00VuqU8{mj'+\
                '\x00\xff\xf7\x08\x02\x00\xff\x81\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00'+\
                ';\x10\x1aA1PCG\t\x06\x1a\x18\x00mysql_native_password\x00'
        self.send(hh1)
        authpkt=self.csock.recv(65536)
        #TODO:以后需要验证密码是否正确
        #hh2=b'\x07\x00\x00\x02\x00\x00\x00\x02\x00\x00\x00'
        hh2=self.okpacket(pktid=2)
        self.send(hh2)
        return

    def packone(self,cont,pktid):
        """cont为包内容，pktid为包序列号，session全局自增的"""
        contlen=len(cont)
        return struct.pack('<I',contlen)[:3]+struct.pack('B',pktid)+cont

    def okpacket(self,field_count=0,affected_rows=0,insert_id=0,
            server_status=2,warning_count=0,pktid=0):
        packet=struct.pack('BBB',field_count,affected_rows,insert_id)+\
                struct.pack('HH',server_status,warning_count)
        packet=self.packone(packet,pktid)
        return packet

    def table(self):
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
