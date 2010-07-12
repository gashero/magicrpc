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

class PGProtocol(protocol.Protocol):
    """PostgreSQL protocol"""

    _buffer=''
    _authed=False

    def connectionMade(self):
        return

    def connectionLost(self,reason):
        return

    def dataReceived(self,data):
        self._buffer+=data
        return

    def sendPacket(self,pkt):
        self.transport.write(pkt)
        return

class PGFactory(protocol.Factory):
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
    log.startlogging(sys.stdout)
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
