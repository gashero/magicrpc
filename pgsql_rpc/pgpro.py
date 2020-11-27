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
salt=lambda length:''.join([chr(random.choice(range(33,120))) for x in range(length)]).encode('utf-8')

def extract_packet(_buffer):
    """extract PostgreSQL protocol packet"""
    if len(_buffer)>=5:
        mtype=_buffer[0]
        msglen=struct.unpack('!L',_buffer[1:5])[0]
        if len(_buffer)>=msglen+1:
            return _buffer[5:msglen+1],mtype,_buffer[msglen+1:]
    return None,None,_buffer

class PGBuffer(object):
    """Manage buffer and get PostgreSQL protocol packet"""
    _buffer=''

    def __init__(self):
        return

    def feed(self,chunk):
        return

    def __iter__(self):
        return

class PGSimpleError(Exception):

    def __init__(self,message,detail):
        self.message=message
        self.detail=detail
        self.args=(message,detail)
        return

    def __str__(self):
        return 'PGSimpleError(%s,%s)'%self.args

class OriginPacket(Exception):

    def __init__(self,packetlist):
        self.packetlist=packetlist
        return

class StatusPacket(Exception):

    def __init__(self,command):
        self.command=command
        return

class PGProtocol(protocol.Protocol):
    """PostgreSQL protocol"""

    _buffer=b'\x00'
    _authed=False
    _status=ProSts_AskSSL

    def connectionMade(self):
        print 'ConnectionMade()'
        self.saltstr=salt(4)
        return

    def connectionLost(self,reason):
        print('ConnectionLost()',reason)
        return

    def dataReceived(self,chunk):
        self._buffer+=chunk
        #print 'dataReceived()=%s'%repr(chunk)
        while True:
            packet,mtype,self._buffer=extract_packet(self._buffer)
            if packet==None:
                break
            if self._status==ProSts_AskSSL:
                if packet==b'\x04\xd2\x16\x2f':
                    self._status=ProSts_Startup
                    self.transport.write(b'N')
                    self._buffer+=b'\x00'
                elif packet.startswith(b'\x00\x03\x00\x00'):
                    protocol_version=packet[:4]
                    pairlist=packet[4:].split(b'\x00')[:-2]
                    infodict={}
                    for idx in range(len(pairlist)/2):
                        infodict[pairlist[idx*2]]=pairlist[idx*2+1]
                    self.username=infodict[b'user']
                    self.cmdmapping['startup'](protocol_version,infodict)
                    self._status=ProSts_AuthMD5
                    self.sendPacket('R',INT32(5)+self.saltstr)
                else:
                    print('AskSSL_ERROR: mtype=%s packet=%s'%(
                            repr(mtype),repr(packet)))
                    self.transport.loseConnection()
            elif self._status==ProSts_Startup:
                if packet.startswith(b'\x00\x03\x00\x00'):
                    protocol_version=packet[:4]
                    pairlist=packet[4:].split(b'\x00')[:-2]
                    infodict={}
                    for idx in range(int(len(pairlist)/2)):
                        infodict[pairlist[idx*2]]=pairlist[idx*2+1]
                    self.username=infodict[b'user']
                    self.cmdmapping['startup'](protocol_version,infodict)
                    self._status=ProSts_AuthMD5
                    self.sendPacket('R',INT32(5)+self.saltstr)
                else:
                    print('Startup_ERROR: mtype=%s packet=%s'%(
                            repr(mtype),repr(packet)))
                    self.transport.loseConnection()
            elif self._status==ProSts_AuthMD5:
                if not mtype=='p':
                    print('AuthMD5: mtype=%s packet=%s'%(
                            repr(mtype),repr(packet)))
                    self.transport.loseConnection()
                if self.cmdmapping['authmd5'](self.username,self.saltstr,
                        packet[3:-1]):
                    self._status=ProSts_Query
                    self.sendPacket('R',INT32(0))
                    for (k,v) in self.cmdmapping['deslist']:
                        self.sendPacket('S','%s\x00%s\x00'%(k,v))
                    self.sendPacket('K',b'\x00\x00&\xefY3>\xc1') #TODO: backendkeydata
                    self.sendPacket('Z',b'I')
                else:
                    self.sendPacket('E',b'S\xe8\x87\xb4\xe5\x91\xbd\xe9\x94\x99'+\
                            b'\xe8\xaf\xaf\x00C28000\x00M\xe7\x94\xa8\xe6\x88'+\
                            b'\xb7 "dbu" Password \xe8\xae\xa4\xe8\xaf\x81\xe5'+\
                            b'\xa4\xb1\xe8\xb4\xa5\x00Fauth.c\x00L1017\x00'+\
                            b'Rauth_failed\x00\x00')
                    self.transport.loseConnection()
            elif self._status==ProSts_Query:
                if mtype=='Q':
                    query=packet[:-1]
                    #print 'Query[%d]: %s,query=%s'%(len(packet),repr(packet),query.replace('\n',' '))
                    reactor.callInThread(self.process_query,query)
                    #self.process_query(query)
                elif mtype=='X':
                    assert packet==''
                    self.transport.loseConnection()
                else:
                    print('UnknownQuery[%d]: mtype=%s, packet=%s'%repr(
                            len(packet),repr(mtype),repr(packet)))
                    self.transport.loseConnection()
            else:
                print('UnknownPacket: mtype=%s,packet=%s'%(
                        repr(mtype),repr(packet)))
                self.transport.loseConnection()
        return

    def sendPacket(self,pkttype,pktbuf,inthread=False):
        if sys.version_info.major==3:
            if type(pkttype)==str:
                pkttype=pkttype.encode('utf-8')
            if type(pktbuf)==str:
                pktbuf=pktbuf.encode('utf-8')
        databuf=pkttype+struct.pack(b'!L',len(pktbuf)+4)+pktbuf
        #print 'Sent[%d]: %s'%(len(databuf),repr(databuf))
        if not inthread:
            self.transport.write(databuf)
        else:
            reactor.callFromThread(self.transport.write,databuf)
        return

    def process_query(self,query):
        try:
            func=self.cmdmapping['query']
            colname,dataset=func(query)
            coldef,pktlist,complete=simple_dataset(colname,dataset)
            self.sendPacket('T',coldef,True)
            for pkt in pktlist:
                self.sendPacket('D',pkt,True)
            self.sendPacket('C',complete,True)
            self.sendPacket('Z',b'I',True)
        except PGSimpleError as ex:
            self.sendPacket('E',simple_error(ex.message,ex.detail),True)
            self.sendPacket('Z',b'I',True)
        except OriginPacket as ex:
            for pk in ex.packetlist:
                self.sendPacket(pk[0],pk[1],True)
        except StatusPacket as ex:
            self.sendPacket('C',ex.command+'\x00',True)
            self.sendPacket('Z',b'I',True)
        except Exception as ex:
            traceback.print_exc()
            self.sendPacket('E',simple_error('InternalError',repr(ex)),True)
            self.sendPacket('Z',b'I',True)
        return

def simple_dataset(colname,strlist):
    #coldef='\x00\x01%s\x00\x00\x00@\x00\x00\x02\x00\x00\x04\x17\x00\x04\xff\xff\xff\xff\x00\x00'%(colname,)
    #coldef='\x00\x01%s\x00\x00\x00@\x05\x00\x02\x00\x00\x04\x13\xff\xff\x00\x00\x00D\x00\x00'%(colname,)
    coldef='\x00\x01%s\x00\x00\x00\x00\x00\x00\x00\x00\x00\x04\x13\xff\xff\x00\x00\x00\x00\x00\x00'%(colname,)
    #TODO:need test
    pktlist=[]
    for strcontent in strlist:
        pktlist.append(
                '\x00\x01%s%s'%(struct.pack('!L',len(strcontent)),strcontent)
                )
    complete='SELECT\x00'
    return coldef,pktlist,complete

def simple_error(message,detail=''):
    if detail:
        detail='D%s\x00'%detail
    return 'SERROR\x00CP0001\x00M%s\x00%s\x00'%(message,detail)

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
    reactor.suggestThreadPoolSize(threadcount)
    return app

## unittest ####################################################################

import unittest
import subprocess

class TestProtocol(unittest.TestCase):

    def setUp(self):
        import psycopg2
        return

    def tearDown(self):
        return

    def test_psql_show_is_superuser(self):
        #cmd='psql -h localhost -Udbu -p5440 test -c "SHOW IS_SUPERUSER"'
        cmd='psql -h localhost -Udbu -p5440 test -c "exit"'
        pipe=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout
        output=pipe.read()
        pipe.close()
        #self.assertEqual(output.strip().startswith(b'is_superuser'),True)
        return

if __name__=='__main__':
    unittest.main()
