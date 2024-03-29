#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: clienttester.py
# Date: 2020-11-27
# Author: gashero

"""协议测试客户端，测试连接的是MySQL-5.7"""

import os
import sys
import time
import socket
import struct

import dpkt

def connsock(host,port):
    csock=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    csock.connect((host,port))
    return csock

def unpackone(data):
    pktlen=struct.unpack('<I',data[:4])[0]
    if len(data)<pktlen+4:
        return None,data
    return data[4:pktlen+4],data[pktlen+4:]

def test1():
    csock=connsock('127.0.0.1',3306)
    time.sleep(0.5)
    data=csock.recv(4096)
    print(len(data),repr(data))
    pkt,rest=unpackone(data)
    print(len(pkt),len(rest),type(pkt))
    #首次收到的数据  '[\x00\x00\x00\n5.7.32-0ubuntu0.16.04.1\x00\x08\x00\x00\x00`N \rk^\x1fR\x00\xff\xff\x08\x02\x00\xff\xc1\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x10Hg\x07\x11\x07\x1f\x07[Nj8\x00mysql_native_password\x00'
    pos_svend=pkt.index(b'\x00')
    leftpkt=pkt[pos_svend+1:]
    hsinfo={
            'protocol_version':     pkt[0],
            'server_version':       pkt[1:pos_svend],
            'n1':                   pkt[pos_svend],
            'thread_id':            struct.unpack('<I',leftpkt[0:4])[0],
            'scrambuf':             leftpkt[4:12],
            'n2':                   leftpkt[12],
            'server_capa':          leftpkt[13:15],
            'server_lang':          leftpkt[15],
            'server_status':        leftpkt[16:18],
            'capa2':                leftpkt[18:20],
            'n3':                   leftpkt[20],
            'n3s13':                leftpkt[18:18+13],
            'n4s13':                leftpkt[31:31+13],
            'left':                 leftpkt[44:],
            }
    if not hsinfo['protocol_version']=='\x0a':
        print('protocol_version not match 0x0a')
    print(hsinfo)
    assert hsinfo['n1']==0
    assert hsinfo['n2']==0
    assert hsinfo['server_lang']==255   #服务器字符集，也叫character set
    return

def test2():
    fi=open('port3306_p2.pcap','rb')
    fi.seek(24)
    while True:
        hdr=fi.read(16)
        if not hdr: break
        t1,t2,l1,l2=struct.unpack('IIII',hdr)
        data=fi.read(l1)
        pkteth=dpkt.ethernet.Ethernet(data)
        #print(pkteth.data.data.data)
        pkttcp=pkteth.data.data
        if not pkttcp.data: continue
        print(pkttcp.sport,pkttcp.data)
    return

# https://dev.mysql.com/doc/internals/en/connection-phase-packets.html#packet-Protocol::Handshake

def main():
    test2()
    return

if __name__=='__main__':
    main()
