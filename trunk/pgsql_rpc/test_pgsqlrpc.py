#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: test_pgsqlrpc.py
# Date: 2010-07-12
# Author: gashero

import os
import sys

import pgpro
import pgrpc

cmdmapping={}

if __name__=='__main__':
    pgpro.start_console(cmdmapping,port=5440)
elif __name__=='__builtin__':
    pgpro.start_daemon(cmdmapping,port=5440)
else:
    raise ValueError,'Unknown start style.'
