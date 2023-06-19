===================
mysql rpc工作日志
===================

:作者: gashero
:日期: 2023-06-16

.. contents:: 目录
.. sectnum::

简介
======

记录设计与调试。

设计
======

服务器框架
------------

当前用的是自己实现的网络框架，以后打算用Python内置的Socket框架。以前的pgsql用的是twisted，但感觉依赖性很麻烦。

日志
======

2023-06-16
------------

实测在ubuntu-20.04上的mysql-8.0用的是sha2验证，挺麻烦的。

一个抓取到的握手包::

    $ python3 clienttester.py 
    95 b'[\x00\x00\x00\n8.0.33-0ubuntu0.20.04.3\x00\x16\x00\x00\x00\x0cG9\x01^B\x07q\x00\xff\xff\xff'+\
       \x02\x00\xff\xdf\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00_\x13UQAN/Gc*yV\x00caching_sha2_password\x00'
    91 0 <class 'bytes'>
    protocol_version not match 0x0a
    {'protocol_version': 10, 'server_version': b'8.0.33-0ubuntu0.20.04.3', 'n1': 0, 'thread_id': 22,
        'scrambuf': b'\x0cG9\x01^B\x07q', 'n2': 0, 'server_capa': b'\xff\xff', 'server_lang': 255,
        'server_status': b'\x02\x00', 'capa2': b'\xff\xdf', 'n3': 21,
        'n3s13': b'\xff\xdf\x15\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00',
        'n4s13': b'_\x13UQAN/Gc*yV\x00', 'left': b'caching_sha2_password\x00'}

我实在不想实现这么复杂的验证协议。所以试着安装mariadb。

在本机安装mysql-5.5挺麻烦的。

在很老的虚拟机上倒是找到mysql-server-5.5，但由于其debian版本为8.7，至于源都关闭了。
