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
ProSts_WaitStartup=2
ProSts_WaitAuthClearText=3
ProSts_WaitQuery=4

INT32=lambda i:struct.pack('!l',i)

class PGProtocol(protocol.Protocol):
    """PostgreSQL protocol"""

    _buffer=''
    _authed=False
    _status=ProSts_AskSSL

    def connectionMade(self):
        print 'ConnectionMade()'
        #self.transport.write('N')
        return

    def connectionLost(self,reason):
        print 'ConnectionLost()'
        return

    def dataReceived(self,data):
        self._buffer+=data
        print 'DataReceived()=%s'%repr(data)
        if len(self._buffer)>=5:
            if self._status==ProSts_AskSSL:
                pktlen=struct.unpack('!L',self._buffer[:4])[0]
                if len(self._buffer)>=pktlen:
                    pktbuf=self._buffer[4:pktlen]
                    self._buffer=self._buffer[pktlen:]
                    if pktbuf[:4]=='\x00\x03\x00\x00':
                        self._status=ProSts_WaitAuthClearText
                        self.sendPacket('R',INT32(3))        #要求clear-text密码
                    elif pktbuf[:4]=='\x04\xd2\x16\x2f':
                        self.transport.write('N')
                        self._status=ProSts_WaitStartup
            elif self._status==ProSts_WaitStartup:
                pktlen=struct.unpack('!L',self._buffer[:4])[0]
                if len(self._buffer)>=pktlen:
                    pktbuf=self._buffer[4:pktlen]
                    self._buffer=self._buffer[pktlen:]
                    print 'StartUp[%d]: %s'%(len(pktbuf),repr(pktbuf))  #8.3发来的是\x04\xd2\x16\x2f
                    #TODO:startup包还没解析
                    self._status=ProSts_WaitAuthClearText
                    #self.sendPacket('R',struct.pack('!l',5)+'aaaa') #要求MD5认证
                    self.sendPacket('R',INT32(3))        #要求clear-text密码
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
