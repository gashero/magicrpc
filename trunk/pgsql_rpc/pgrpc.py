# -*- coding: UTF-8 -*-
# File: pgrpc.py
# Date: 2010-09-05
# Author: gashero

"""
A rpc wrapper to call and return
"""

import os
import sys
import md5
import traceback

import pgpro

md5sum=lambda d:md5.md5(d).hexdigest()

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

class PgRpc(object):

    def __init__(self,userpass_md5dict):
        self.userpass_md5dict=userpass_md5dict
        self.cmdmapping={
                'authmd5':self.authmd5,
                'startup':self.startup,
                'deslist':DescribeList,
                'cmd_select':self.cmd_select,
                'cmd_show':self.cmd_show,
                'cmd_set':self.cmd_set,
                'cmd_all':self.cmd_all,
                }
        return

    def startup(self,protocol_version,infodict):
        print 'Startup: %s %s'%(repr(protocol_version),repr(infodict))
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
        return ('idx',['hello1','hello2'])
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_show(self,querystring):
        if querystring=='client_encoding':
            raise pgpro.SpecialPacket([
                ('T','\x00\x01client_encoding\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\xff\xff\xff\xff\xff\xff\x00\x00'),
                ('D','\x00\x01\x00\x00\x00\x04UTF8'),
                ('C','SHOW\x00'),
                ('Z','I'),
                ])
        elif querystring=='default_transaction_isolation':
            raise pgpro.SpecialPacket([
                ('T','\x00\x01default_transaction_isolation\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x19\xff\xff\xff\xff\xff\xff\x00\x00'),
                ('D','\x00\x01\x00\x00\x00\x0eread committed'),
                ('C','SHOW\x00'),
                ('Z','I'),
                ])
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_set(self,querystring):
        print 'QueryString: %s'%repr(querystring)
        if querystring=="DATESTYLE TO 'ISO'":
            raise pgpro.SpecialPacket([
                ('S','DateStyle\x00ISO, YMD\x00'),
                ('C','SET\x00'),
                ])
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_all(self,querystring):
        print 'ALL: %s'%repr(querystring)
        raise pgpro.PGSimpleError('hello','fuck')

    #def funccall(self,callstring):
    #    return

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
        cur.execute('select hello')
        dataset=cur.fetchall()
        print dataset
        cur.close()
        conn.close()
        return

if __name__=='__main__':
    unittest.main()
