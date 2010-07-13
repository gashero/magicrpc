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

ProSts_WaitStartup=1
ProSts_WaitAuth=2

class PGProtocol(protocol.Protocol):
    """PostgreSQL protocol"""

    _buffer=''
    _authed=False
    _status=ProSts_WaitStartup

    def connectionMade(self):
        print 'ConnectionMade()'
        return

    def connectionLost(self,reason):
        print 'ConnectionLost()'
        return

    def dataReceived(self,data):
        self._buffer+=data
        print 'DataReceived()=%s'%repr(data)
        if len(self._buffer)>=5:
            if self._status==ProSts_WaitStartup:
                pktlen=struct.unpack('!L',self._buffer[:4])[0]
                if len(self._buffer)>=pktlen:
                    pktbuf=self._buffer[4:pktlen]
                    self._buffer=self._buffer[pktlen:]
                    print 'StartUp[%d]: %s'%(len(pktbuf),repr(pktbuf))
                    #self.sendPacket('R',struct.pack('!l',5)+'aaaa') #要求MD5认证
                    self.sendPacket('R',struct.pack('!l',3))        #要求clear-text密码
                    self._status=ProSts_WaitAuth
            elif self._status==ProSts_WaitAuth:
                pkttype=self._buffer[0]
                pktlen=struct.unpack('!L',self._buffer[1:5])[0]
        return

    def sendPacket(self,pkttype,pktbuf):
        databuf=pkttype+struct.pack('!L',len(pktbuf)+4)+pktbuf
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
