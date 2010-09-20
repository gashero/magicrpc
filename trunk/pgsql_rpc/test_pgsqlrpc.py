#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: test_pgsqlrpc.py
# Date: 2010-07-12
# Author: gashero

import os
import sys
import time

import pgrpc

now=lambda :time.strftime('%Y-%m-%d %H:%M:%S')
_add=lambda x,y:x+y

funcmapping={
        'now':now,
        'add':_add,
        'inc2':lambda x:x+2,
        }

rpcserver=pgrpc.PgRpc({
    'dbu':md5sum('dddd'+'dbu'),
    },funcmapping)

if __name__=='__main__':
    pgrpc.start_console(rpcserver.cmdmapping,port=5440)
elif __name__=='__builtin__':
    pgrpc.start_daemon(rpcserver.cmdmapping,port=5440)
else:
    raise ValueError,'Unknown start style.'
