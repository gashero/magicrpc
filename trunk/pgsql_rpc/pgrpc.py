# -*- coding: UTF-8 -*-
# File: pgrpc.py
# Date: 2010-09-05
# Author: gashero

"""
A rpc wrapper to call and return
"""

import os
import re
import sys
import md5
import time
import traceback

import psycopg2

import pgpro
from pgpro import PGSimpleError as LogicError

RUNNING=False

md5sum=lambda d:md5.md5(d).hexdigest()
now=lambda :time.strftime('%Y-%m-%d %H:%M:%S')
RE_FUNCCALL_REPR=re.compile(r'''^(?P<funcname>\w+)\((?P<args>.*?)\)$''')

DescribeList=[
        ('client_encoding','UTF8'),
        ('DateStyle','ISO, YMD'),
        ('integer_datetimes','on'),
        ('is_superuser','off'),
        ('server_encoding','UTF8'),
        ('server_version','8.3.11'),
        ('session_authorization','postgres'),   #this value is username
        ('standard_conforming_strings','off'),
        ('TimeZone','PRC'),
        ]

CommonResponse={
        "SET DATESTYLE TO 'ISO'":[
            ('S','DateStyle\x00ISO, YMD\x00'),
            ('C','SET\x00'),
            ('Z','I'),
            ],
        'SHOW client_encoding':[
            ('T','\x00\x01client_encoding\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\xff\xff\xff\xff\xff\xff\x00\x00'),
            ('D','\x00\x01\x00\x00\x00\x04UTF8'),
            ('C','SHOW\x00'),
            ('Z','I'),],
        'SHOW default_transaction_isolation':[
            ('T','\x00\x01default_transaction_isolation\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\xff\xff\xff\xff\xff\xff\x00\x00'),
            ('D','\x00\x01\x00\x00\x00\x0eread committed'),
            ('C','SHOW\x00'),
            ('Z','I'),],
        'BEGIN; SET TRANSACTION ISOLATION LEVEL READ COMMITTED':[
            ('C','BEGIN\x00'),
            ('C','SET\x00'),
            ('Z','T'),
            ],
        'ROLLBACK':[
            ('C','ROLLBACK\x00'),
            ('Z','I'),
            ],
        }

exposed_funcmapping={}
def expose(funcname=None):
    """declare function that call by rpc"""
    global exposed_funcmapping
    def inter1(func,funcname):
        if funcname==None:
            funcname=func.__name__
        if not exposed_funcmapping.has_key(funcname):
            exposed_funcmapping[funcname]=func
        else:
            if not RUNNING:
                raise KeyError,'funcname declare more than once.'
            else:
                pass
        def inter2(*args,**kwargs):
            return func(*args,**kwargs)
        return inter2
    return lambda func:inter1(func,funcname)

def makepass(userpassdict):
    """create userpass_md5dict from username and password dictionary."""
    userpass_md5dict={}
    for (username,password) in userpassdict.items():
        userpass_md5dict[username]=md5sum(password+username)
    return userpass_md5dict

def assert_pgerror(expr,msg,detail=None):
    """assert expr is true, else raise PGSimpleError with msg"""
    if not expr:
        if not detail:
            detail=msg
        raise pgpro.PGSimpleError(msg,detail)

class PgRpc(object):

    def __init__(self,userpass_md5dict,funcmapping={}):
        self.userpass_md5dict=userpass_md5dict
        self.cmdmapping={
                'authmd5':self.authmd5,
                'startup':self.startup,
                'deslist':DescribeList,
                'query':self.query,
                #internal mapping
                'cmd_select':self.cmd_select,
                'cmd_show':self.cmd_show,
                'cmd_set':self.cmd_set,
                'cmd_reload':self.cmd_reload,
                'cmd_call':self.cmd_call,
                }
        self.funcmapping=funcmapping
        self.funcmapping.update(exposed_funcmapping)
        return

    def startup(self,protocol_version,infodict):
        #print 'Startup: %s %s'%(repr(protocol_version),repr(infodict))
        return

    def authmd5(self,username,saltstr,md5pass):
        try:
            userpass_md5=self.userpass_md5dict[username]
            if md5pass==md5sum(userpass_md5+saltstr):
                return True
            else:
                return False
        except KeyError:
            return False
        except Exception,ex:
            traceback.print_exc()
            #print str(ex)
            return False

    def cmd_select(self,querystring):
        #print 'SELECT: %s'%repr(querystring)
        if querystring=='name FROM pgtest1':
            #return ('name',['hello1','hello2','hello3'])
            return 'hello1'
        else:
            raise pgpro.PGSimpleError('hello','fuck')

    def cmd_show(self,querystring):
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_set(self,querystring):
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_reload(self,querystring):
        try:
            modname=querystring
            mod=sys.modules[modname]
            reload(mod)
            raise pgpro.StatusPacket('reload')
        except KeyError:
            raise pgpro.PGSimpleError('ModuleNameError',repr(modname))

    def cmd_call(self,querystring):
        matchobj=RE_FUNCCALL_REPR.search(querystring)
        if matchobj:
            gdict=matchobj.groupdict()
            funcname=gdict['funcname']
            args=gdict['args']
            #print 'funcmapping: %s'%repr(self.funcmapping)
            func=self.funcmapping.get(funcname)
            if func:
                ret=eval(querystring,{},self.funcmapping)
                return ret
            else:
                raise pgpro.PGSimpleError('FunctionNameError',\
                        repr(funcname))
        else:
            raise pgpro.PGSimpleError('FunctionFormatError',\
                    repr(querystring))
        return

    def query(self,querystring):
        try:
            cresp=CommonResponse[querystring]
            raise pgpro.OriginPacket(cresp)
        except KeyError:
            pass
        try:
            cmd,arg=querystring.strip().split(' ',1)
            cmd=cmd.strip().lower()
            arg=arg.strip()
            if arg.endswith(';'):
                arg=arg[:-1]
            func=self.cmdmapping.get('cmd_'+cmd)
            if func:
                ret=func(arg)
                return ('result',[repr(ret),])
            else:
                print 'UnknownQuery: %s'%repr(querystring)
                raise pgpro.PGSimpleError('UnknownQuery',repr(querystring))
        except pgpro.PGSimpleError,ex:
            raise
        except pgpro.StatusPacket,ex:
            raise
        except Exception,ex:
            #traceback.print_exc()
            print '%s: querystring=%s'%(repr(ex),repr(querystring))
            raise pgpro.PGSimpleError(repr(ex),repr(ex))

MAX_USAGE=1000
class PgRpcClient(object):
    """An rpc client to call pgrpc"""

    def __init__(self,params):
        from DBUtils.PooledDB import PooledDB
        self._dbpool=PooledDB(maxusage=MAX_USAGE,creator=psycopg2,**params)
        return

    def __getattr__(self,funcname):
        if funcname.startswith('_'):
            return object.__getitem__(self,funcname)
        else:
            func=lambda *args,**kwargs:self.__call__(funcname,*args,**kwargs)
            func.__name__='PgRpcClient.'+funcname
            return func

    def __call__(self,funcname,*args,**kwargs):
        params=map(lambda xx:repr(xx),args)
        for (k,v) in kwargs.items():
            params.append('%s=%s'%(k,repr(v)))
        callstr='CALL %s(%s)'%(funcname,','.join(params))
        conn=None
        cur=None
        try:
            conn=self._dbpool.connection()
            cur=conn.cursor()
            cur.execute(callstr)
            dataset=cur.fetchall()
            return eval(dataset[0][0])
        finally:
            if cur:
                cur.close()
            if conn:
                conn.close()

def start_console(*args,**kwargs):
    global RUNNING
    RUNNING=True
    return pgpro.start_console(*args,**kwargs)

def start_daemon(*args,**kwargs):
    global RUNNING
    RUNNING=True
    return pgpro.start_daemon(*args,**kwargs)

## unittest ####################################################################

import unittest
import subprocess

class TestPgRpc(unittest.TestCase):

    def setUp(self):
        return

    def tearDown(self):
        return

    def _test_psql_show_is_superuser(self):
        #cmd='psql -h localhost -Udbu -p5440 test -c "SHOW IS_SUPERUSER"'
        cmd='psql -h localhost -Udbu -p5440 test -c "exit"'
        pipe=subprocess.Popen(cmd,shell=True,stdout=subprocess.PIPE).stdout
        output=pipe.read()
        pipe.close()
        #self.assertEqual(output.strip().startswith('is_superuser'),True)
        return

    def test_psycopg2_connect(self):
        conn=psycopg2.connect(host='localhost',user='dbu',database='test',password='dddd',port=5440)
        cur=conn.cursor()
        cur.execute('SELECT name FROM pgtest1')
        dataset=cur.fetchall()
        self.assertEqual(dataset,[("'hello1'",)])
        cur.close()
        conn.close()
        return

    def test_psycopg2_call_now(self):
        conn=psycopg2.connect(host='localhost',user='dbu',database='test',password='dddd',port=5440)
        cur=conn.cursor()
        cur.execute('CALL now()')
        dataset=cur.fetchall()
        self.assertEqual(dataset[0],(repr(now()),))
        cur.execute('CALL add(2,3)')
        dataset=cur.fetchall()
        self.assertEqual(dataset[0],(repr(5),))
        cur.execute('CALL add(5,4);')
        dataset=cur.fetchall()
        self.assertEqual(dataset[0],(repr(9),))
        cur.execute('CALL inc2(7);')
        dataset=cur.fetchall()
        self.assertEqual(dataset[0],(repr(9),))
        cur.close()
        conn.close()
        return

    def test_psycopg2_call_raiseerror(self):
        conn=psycopg2.connect(host='localhost',user='dbu',database='test',password='dddd',port=5440)
        cur=conn.cursor()
        try:
            cur.execute('CALL raiseerror()')
        except psycopg2.InternalError,ex:
            #print 'pgcode=',ex.pgcode
            #print 'pgerror=',repr(ex.pgerror)
            #print 'message=',repr(ex.message)
            self.assertEqual(ex.message,'error\nDETAIL:  sth wrong\n')
        cur.close()
        conn.close()
        return

    def test_assert_pgerror(self):
        self.assertEqual(assert_pgerror(1==1,'ok'),None)
        try:
            assert_pgerror(1==2,'1!=2','failed')
            self.assertEqual('not run here','here')
        except pgpro.PGSimpleError,ex:
            self.assertEqual(ex.message,'1!=2')
            self.assertEqual(ex.detail,'failed')
        return

class TestPgRpcClient(unittest.TestCase):

    def test_call(self):
        pgc=PgRpcClient({'host':'localhost','port':5440,'user':'dbu','password':'dddd',})
        #print pgc.add(2,y=3)
        self.assertEqual(pgc.add(2,y=3),5)
        self.assertEqual(pgc.add(x=2,y=9),11)
        self.assertEqual(pgc.inc2(x=4),6)
        self.assertEqual(pgc.inc2(4),6)
        return

if __name__=='__main__':
    unittest.main()
