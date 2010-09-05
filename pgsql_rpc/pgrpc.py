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
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_show(self,querystring):
        raise pgpro.PGSimpleError('hello','fuck')

    def cmd_set(self,querystring):
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

if __name__=='__main__':
    unittest.main()
