#! /usr/bin/env python
# -*- coding: UTF-8 -*-
# File: test_mysqlrpc.py
# Date: 2023-06-16
# Author: gashero

import os
import sys
import time

import mysqlpro

def test1():
    server=mysqlpro.MySQLServer()
    server.serve_forever()
    return

test1()
