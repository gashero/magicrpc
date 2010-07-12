#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: test_pgsqlrpc.py
# Date: 2010-07-12
# Author: gashero

if __name__=='__main__':
    pgpro.start_console(cmdmapping)
elif __name__=='__builtin__':
    pgpro.start_daemon(cmdmapping)
else:
    raise ValueError,'Unknown start style.'
