# -*- coding: UTF-8 -*-
# File: pgpro.py
# Date: 2010-07-12
# Author: gashero

"""
PostgreSQL server protocol implementation.
"""

import re
import os
import sys
import sha
import md5
import random
import struct
import traceback

from twisted.internet import reactor
from twisted.internet import protocol
from twisted.python import log
from twisted.application import service,internet
from twisted.internet import threads

ProSts_AskSSL=1
ProSts_Startup=2
ProSts_AuthClearText=3
ProSts_AuthMD5=4
ProSts_Query=50

ProSts_Startup=10

INT32=lambda i:struct.pack('!l',i)
salt=lambda length:''.join([chr(random.choice(range(33,120))) for x in range(length)])

def extract_packet(_buffer):
    """按照PostgreSQL的协议进行解包"""
    if len(_buffer)>=5:
        mtype=_buffer[0]
        msglen=struct.unpack('!L',_buffer[1:5])[0]
        if len(_buffer)>=msglen+1:
            return _buffer[5:msglen+1],mtype,_buffer[msglen+1:]
    return None,None,_buffer

class PGBuffer(object):
    """按照PostgreSQL协议定义的缓存管理"""
    _buffer=''

    def __init__(self):
        return

    def feed(self,chunk):
        return

    def __iter__(self):
        return

class PGProtocol(protocol.Protocol):
    """PostgreSQL protocol"""

    _buffer='\x00'
    _authed=False
    _status=ProSts_AskSSL

    def connectionMade(self):
        print 'ConnectionMade()'
        #self.transport.write('N')
        self.saltstr=salt(4)
        return

    def connectionLost(self,reason):
        print 'ConnectionLost()'
        return

    def dataReceived(self,chunk):
        self._buffer+=chunk
        #print 'dataReceived()=%s'%repr(chunk)
        while True:
            packet,mtype,self._buffer=extract_packet(self._buffer)
            if packet==None:
                break
            if self._status==ProSts_AskSSL:
                if packet=='\x04\xd2\x16\x2f':
                    self._status=ProSts_Startup
                    self.transport.write('N')
                    self._buffer+='\x00'
                elif packet.startswith('\x00\x03\x00\x00'):
                    protocol_version=packet[:4]
                    pairlist=packet[4:].split('\x00')[:-2]
                    infodict={}
                    for idx in range(len(pairlist)/2):
                        infodict[pairlist[idx*2]]=pairlist[idx*2+1]
                    self.username=infodict['user']
                    self.cmdmapping['startup'](protocol_version,infodict)
                    self._status=ProSts_AuthMD5
                    self.sendPacket('R',INT32(5)+self.saltstr)
                else:
                    print 'AskSSL_ERROR: mtype=%s packet=%s'%(
                            repr(mtype),repr(packet))
                    self.transport.loseConnection()
            elif self._status==ProSts_Startup:
                if packet.startswith('\x00\x03\x00\x00'):
                    protocol_version=packet[:4]
                    pairlist=packet[4:].split('\x00')[:-2]
                    infodict={}
                    for idx in range(len(pairlist)/2):
                        infodict[pairlist[idx*2]]=pairlist[idx*2+1]
                    self.username=infodict['user']
                    self.cmdmapping['startup'](protocol_version,infodict)
                    self._status=ProSts_AuthMD5
                    self.sendPacket('R',INT32(5)+self.saltstr)
                else:
                    print 'Startup_ERROR: mtype=%s packet=%s'%(
                            repr(mtype),repr(packet))
                    self.transport.loseConnection()
            elif self._status==ProSts_AuthMD5:
                if not mtype=='p':
                    print 'AuthMD5: mtype=%s packet=%s'%(
                            repr(mtype),repr(packet))
                    self.transport.loseConnection()
                if self.cmdmapping['authmd5'](self.username,self.saltstr,
                        packet[3:-1]):
                    self._status=ProSts_Query
                    self.sendPacket('R',INT32(0))
                    for (k,v) in self.cmdmapping['deslist']:
                        self.sendPacket('S','%s\x00%s\x00'%(k,v))
                    self.sendPacket('K','\x00\x00&\xefY3>\xc1')
                    self.sendPacket('Z','I')
                else:
                    self.sendPacket('E','S\xe8\x87\xb4\xe5\x91\xbd\xe9\x94\x99\xe8\xaf\xaf\x00C28000\x00M\xe7\x94\xa8\xe6\x88\xb7 "dbu" Password \xe8\xae\xa4\xe8\xaf\x81\xe5\xa4\xb1\xe8\xb4\xa5\x00Fauth.c\x00L1017\x00Rauth_failed\x00\x00')
                    self.transport.loseConnection()
            elif self._status==ProSts_Query:
                if mtype=='Q':
                    query=packet[:-1]
                    print 'Query[%d]: %s,query=%s'%(len(packet),repr(packet),query)
                    self.sendPacket('T','\x00\x01idx\x00\x00\x00@\x00\x00\x01\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00')
                    self.sendPacket('C','SELECT\x00')
                    self.sendPacket('Z','I')
                elif mtype=='X':
                    assert packet==''
                    self.transport.loseConnection()
                else:
                    print 'UnknownQuery[%d]: mtype=%s, packet=%s'%repr(len(packet),repr(mtype),repr(packet))
                    self.transport.loseConnection()
            else:
                print 'UnknownPacket: mtype=%s,packet=%s'%(repr(mtype),repr(packet))
                self.transport.loseConnection()
        return

    def _dataReceived(self,data):
        self._buffer+=data
        print 'DataReceived()=%s'%repr(data)
        if len(self._buffer)>=5:
            if self._status==ProSts_AskSSL:
                #询问SSL的部分直接拒绝掉
                pktlen=struct.unpack('!L',self._buffer[:4])[0]
                if len(self._buffer)>=pktlen:
                    pktbuf=self._buffer[4:pktlen]
                    self._buffer=self._buffer[pktlen:]
                    if pktbuf[:4]=='\x00\x03\x00\x00':
                        #self._status=ProSts_WaitAuthClearText
                        #self.sendPacket('R',INT32(3))        #要求clear-text密码
                        self._status=ProSts_WaitAuthMD5
                        self.sendPacket('R',INT32(5)+self.saltstr)
                    elif pktbuf[:4]=='\x04\xd2\x16\x2f':
                        self.transport.write('N')
                        self._status=ProSts_WaitStartup
            elif self._status==ProSts_WaitStartup:
                pktlen=struct.unpack('!L',self._buffer[:4])[0]
                if len(self._buffer)>=pktlen:
                    pktbuf=self._buffer[4:pktlen]
                    self._buffer=self._buffer[pktlen:]
                    protocol_version=pktbuf[:4]
                    pairlist=pktbuf[4:].split('\x00')[:-2]
                    #print 'StartUp[%d]: %s'%(len(pktbuf),repr(pktbuf))  #8.3发来的是\x04\xd2\x16\x2f
                    #print 'Startup[%d]: ProtoVer=%s Pairlist=%s'%(len(pktbuf),repr(protocol_version),repr(pairlist))
                    #print 'Pairlist: %s'%repr(pairlist)
                    infodict={}
                    for idx in range(len(pairlist)/2):
                        infodict[pairlist[idx*2]]=pairlist[idx*2+1]
                    self.username=infodict['user']
                    self.cmdmapping['startup'](protocol_version,infodict)
                    #self.sendPacket('R',struct.pack('!l',5)+'aaaa') #要求MD5认证
                    #self.sendPacket('R',INT32(3))        #要求clear-text密码
                    self._status=ProSts_WaitAuthMD5
                    self.sendPacket('R',INT32(5)+self.saltstr)
            elif self._status==ProSts_WaitAuthClearText:
                pkttype=self._buffer[0]
                pktlen=struct.unpack('!L',self._buffer[1:5])[0]
                #print 'WaitAuth[%s][%d]'%(repr(pkttype),pktlen)
                if len(self._buffer)>=pktlen+1:
                    pktbuf=self._buffer[5:pktlen+1]
                    self._buffer=self._buffer[pktlen+1:]
                    password=pktbuf[:-1]
                    print 'Auth[%d]: %s, password=%s'%(len(pktbuf),repr(pktbuf),password)
                    assert pkttype=='p','PasswordMessage must have pkttype [%s]!=[p]'%repr(pkttype)
                    assert pktbuf[-1]=='\x00'
                    #self.sendPacket('R',INT32(0))
                    #self.sendPacket('Z',INT32(5)+'I')
                    self._status=ProSts_WaitQuery
                    self.sendPacket('R',INT32(0))
                    self.sendPacket('S','client_encoding\x00UTF8\x00')
                    self.sendPacket('S','DateStyle\x00ISO, YMD\x00')
                    self.sendPacket('S','integer_datetimes\x00on\x00')
                    self.sendPacket('S','is_superuser\x00on\x00')
                    self.sendPacket('S','server_encoding\x00UTF8\x00')
                    self.sendPacket('S','server_version\x008.3.11\x00')
                    self.sendPacket('S','session_authorization\x00postgres\x00')
                    self.sendPacket('S','standard_conforming_strings\x00off\x00')
                    self.sendPacket('S','TimeZone\x00PRC\x00')
                    self.sendPacket('K','\x00\x00&\xefY3>\xc1')
                    self.sendPacket('Z','I')
            elif self._status==ProSts_WaitAuthMD5:
                pkttype=self._buffer[0]
                pktlen=struct.unpack('!L',self._buffer[1:5])[0]
                if len(self._buffer)>=pktlen+1:
                    pktbuf=self._buffer[5:pktlen+1]
                    self._buffer=self._buffer[pktlen+1:]
                    print 'AuthMD5[%d]: %s'%(len(pktbuf),repr(pktbuf))
                    if self.cmdmapping['authmd5'](self.username,self.saltstr,pktbuf[3:-1]):
                        self._status=ProSts_WaitQuery
                        self.sendPacket('R',INT32(0))
                        for (k,v) in self.cmdmapping['deslist']:
                            self.sendPacket('S','%s\x00%s\x00'%(k,v))
                        self.sendPacket('K','\x00\x00&\xefY3>\xc1')
                        self.sendPacket('Z','I')
                    else:
                        #认证失败，干掉连接
                        #print 'Failed'
                        #self.sendPacket('E','Fuck you!')
                        self.sendPacket('E','S\xe8\x87\xb4\xe5\x91\xbd\xe9\x94\x99\xe8\xaf\xaf\x00C28000\x00M\xe7\x94\xa8\xe6\x88\xb7 "dbu" Password \xe8\xae\xa4\xe8\xaf\x81\xe5\xa4\xb1\xe8\xb4\xa5\x00Fauth.c\x00L1017\x00Rauth_failed\x00\x00')
                        self.transport.loseConnection()
            elif self._status==ProSts_WaitQuery:
                pkttype=self._buffer[0]
                pktlen=struct.unpack('!L',self._buffer[1:5])[0]
                if len(self._buffer)>=pktlen+1:
                    pktbuf=self._buffer[5:pktlen+1]
                    self._buffer=self._buffer[pktlen+1:]
                    if pkttype=='Q':
                        #查询
                        query=pktbuf[:-1]
                        print 'Query[%d]: %s, query=%s'%(len(pktbuf),repr(pktbuf),query)
                        self.sendPacket('T','\x00\x01idx\x00\x00\x00@\x00\x00\x01\x00\x00\x00\x17\x00\x04\xff\xff\xff\xff\x00\x00')
                        self.sendPacket('C','SELECT\x00')
                        self.sendPacket('Z','I')
                    elif pkttype=='X':
                        #关闭
                        assert pktbuf==''
                        self.transport.loseConnection()
                    else:
                        print 'UCommand[%d]: %s'%(len(pktbuf),repr(pktbuf))
            else:
                pkttype=self._buffer[0]
                pktlen=struct.unpack('!L',self._buffer[1:5])[0]
                if len(self._buffer)>=pktlen+1:
                    pktbuf=self._buffer[5:pktlen+1]
                    self._buffer=self._buffer[pktlen+1:]
                    print 'Unknown[%s][%d]: %s'%(repr(pkttype),len(pktbuf),repr(pktbuf))
        return

    def sendPacket(self,pkttype,pktbuf):
        databuf=pkttype+struct.pack('!L',len(pktbuf)+4)+pktbuf
        #print 'Sent[%d]: %s'%(len(databuf),repr(databuf))
        self.transport.write(databuf)
        return

    def queryReceived(self,query):
        return

class PGFactory(protocol.ServerFactory):
    """PostgreSQL factory"""
    protocol=PGProtocol

    def __init__(self,cmdmapping):
        self.cmdmapping=cmdmapping
        return

    def buildProtocol(self,addr):
        p=protocol.ServerFactory.buildProtocol(self,addr)
        p.cmdmapping=self.cmdmapping
        return p

class PGService(internet.TCPServer):
    """PostgreSQL twistd daemon"""

    def __init__(self,cmdmapping,port=5432):
        internet.TCPServer.__init__(self,port,PGFactory(cmdmapping))
        return

def start_console(cmdmapping,port=5432,threadcount=10):
    """start at console"""
    log.startLogging(sys.stdout)
    reactor.listenTCP(port,PGFactory(cmdmapping))
    reactor.suggestThreadPoolSize(threadcount)
    reactor.run()
    return

def start_daemon(cmdmapping,port=5432,threadcount=10):
    app=service.Application('pgsql protocol')
    pgservice=PGService(cmdmapping,port)
    pgservice.setServiceParent(app)
    reactor.suggestThreadPoolSize(threaccount)
    return app

## unittest ####################################################################

import unittest

class TestProtocol(unittest.TestCase):

    def setUp(self):
        return

    def tearDown(self):
        return

if __name__=='__main__':
    unittest.main()
