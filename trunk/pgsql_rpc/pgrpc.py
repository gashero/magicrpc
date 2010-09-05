# -*- coding: UTF-8 -*-
# File: pgrpc.py
# Date: 2010-09-05
# Author: gashero

"""
A rpc wrapper to call and return
"""

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
                }
        return

    def startup(self,protocol_version,infodict):
        print 'Startup: %s %s'%(repr(protocol_version),repr(infodict))
        return

    def authmd5(self,username,saltstr,md5pass):
        return

    def query(self,querystring):
        return

    def funccall(self,callstring):
        return

## unittest ####################################################################

import unittest
import psycopg2

class TestPgRpc(unittest.TestCase):

    def setUp(self):
        return

    def tearDown(self):
        return

if __name__=='__main__':
    unittest.main()
