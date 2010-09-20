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

import pgpro
from pgpro import start_console,start_daemon

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
        }

exposed_funcmapping={}
def expose(funcname,*args,**kwargs):
    """declare function that call by rpc"""
    global exposed_funcmapping
    def inter1(func):
        if not exposed_funcmapping.has_key(funcname):
            exposed_funcmapping[funcname]=func
        else:
            raise KeyError,'funcname declare more than once.'
        def inter2(*args,**kwargs):
            return func(*args,**kwargs)
        return inter2
    return inter1

def makepass(userpassdict):
    """create userpass_md5dict from username and password dictionary."""
    userpass_md5dict={}
    for (username,password) in userpassdict.items():
        userpass_md5dict[username]=md5sum(password+username)
    return userpass_md5dict

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

    def cmd_call(self,querystring):
        if querystring.endswith(";"):
            querystring=querystring[:-1]
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
            func=self.cmdmapping.get('cmd_'+cmd)
            if func:
                ret=func(arg)
                return ('result',[repr(ret),])
            else:
                print 'UnknownQuery: %s'%repr(querystring)
                raise pgpro.PGSimpleError('UnknownQuery',repr(querystring))
        except pgpro.PGSimpleError,ex:
            raise
        except Exception,ex:
            traceback.print_exc()
            raise
            #raise pgpro.PGSimpleError('hello','fuck')

## unittest ####################################################################

import unittest
import psycopg2
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
        #print dataset
        self.assertEqual(dataset,[("'hello1'",)])
        cur.close()
        conn.close()
        return

    def test_psycopg2_call_now(self):
        conn=psycopg2.connect(host='localhost',user='dbu',database='test',password='dddd',port=5440)
        cur=conn.cursor()
        cur.execute('CALL now()')
        dataset=cur.fetchall()
        #print dataset
        self.assertEqual(dataset[0],(repr(now()),))
        cur.execute('CALL add(2,3)')
        dataset=cur.fetchall()
        #print dataset
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

if __name__=='__main__':
    unittest.main()
