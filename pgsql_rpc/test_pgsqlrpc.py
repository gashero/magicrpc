#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: test_pgsqlrpc.py
# Date: 2010-07-12
# Author: gashero

import os
import sys
import md5

import pgpro
import pgrpc

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

def startup(protocol_version,infodict):
    print 'Startup: %s %s'%(repr(protocol_version),repr(infodict))
    return

def authmd5(username,saltstr,md5pass):
    password='dddd'
    print 'AuthMD5: %s %s %s'%(username,saltstr,md5pass)
    if md5pass==md5sum(md5sum(password+username)+saltstr):
        return True
    else:
        return False

cmdmapping={
        'authmd5':authmd5,
        'startup':startup,
        'deslist':DescribeList,
        }

rpcserver=pgrpc.PgRpc({
    'dbu':md5sum('dddd'+'dbu'),
    })

if __name__=='__main__':
    #pgpro.start_console(cmdmapping,port=5440)
    pgpro.start_console(rpcserver.cmdmapping,port=5440)
elif __name__=='__builtin__':
    pgpro.start_daemon(cmdmapping,port=5440)
else:
    raise ValueError,'Unknown start style.'
