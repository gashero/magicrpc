#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: test_pgsqlrpc.py
# Date: 2010-07-12
# Author: gashero

import os
import sys
import time

import pgrpc

@pgrpc.expose('now')
def now():
    return time.strftime('%Y-%m-%d %H:%M:%S')

@pgrpc.expose('add')
def add(x,y):
    return x+y

@pgrpc.expose('inc2')
def inc2(x):
    return x+2

@pgrpc.expose('raiseerror')
def raiseerror():
    pgrpc.assert_pgerror(1==1,'er','err')
    raise pgrpc.LogicError('error','sth wrong')

rpcserver=pgrpc.PgRpc(
        pgrpc.makepass({b'dbu':b'dddd'}),
        )

if __name__=='__main__':
    pgrpc.start_console(rpcserver.cmdmapping,port=5440)
elif __name__=='__builtin__':
    pgrpc.start_daemon(rpcserver.cmdmapping,port=5440)
else:
    raise ValueError('Unknown start style.')
