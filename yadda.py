#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
yadda - Yet Another Door for Database access acts as an easy way to access an 
existing database. It is a layer on top of SqlAlchemy's SqlSoup library and is 
intended to make accessing legacy databases easier. 

Homepage and documentation: http://code.google.com/p/yadda/

License (New BSD License)
-------------
    Copyright (c) 2010, Nicol van der Merwe
    All rights reserved.

    Redistribution and use in source and binary forms, with or without 
    modification, are permitted provided that the following conditions are 
    met:
        * Redistributions of source code must retain the above copyright 
          notice, this list of conditions and the following disclaimer.
        * Redistributions in binary form must reproduce the above copyright 
          notice, this list of conditions and the following disclaimer in the 
          documentation and/or other materials provided with the distribution.
        * Neither the name of the <ORGANIZATION> nor the names of its 
          contributors may be used to endorse or promote products derived from
          this software without specific prior written permission.

    THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS 
    IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
    THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR 
    PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR 
    CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, 
    EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, 
    PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR 
    PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF 
    LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING 
    NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS 
    SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Example
-------

This is an example::
    db = DAL(
        "%s+%s://%s:%s@%s/%s" 
        % 
        (
            "mssql",
            "pymssql",
            "username",
            "userpassword",
            "hostname",
            "databasename"
        )
    )
"""

__author__ = ["Nicol van der Merwe"]
__version__ = "0.1"
__license__ = "New BSD License"

import time
from sqlalchemy.ext import sqlsoup
from sqlalchemy.exceptions import OperationalError
from sqlalchemy.ext.sqlsoup import objectstore

class DAL(sqlsoup.SqlSoup):
    def _init_(self, *args, **kw):
        self._session = None
        self._retrylimit = 1
        self.retrycount = 0

    def begin(self, *args, **kw):
        return sqlsoup.Session.begin(*args, **kw)

    def commit(self, *args, **kw):
        return sqlsoup.Session.commit(*args, **kw)

    def rollback(self, *args, **kw):
        return sqlsoup.Session.rollback(*args, **kw)

    def close(self, *args, **kw):
        return sqlsoup.Session.close(*args, **kw)

    def flush(self, *args, **kw):
        return sqlsoup.Session.flush(*args, **kw)

    def execute(self, *args, **kw):
        try:
            return sqlsoup.Session.execute(*args, **kw)
        except OperationalError, err:
            if self._retrylimit > 0 and self.retrycount < self._retrylimit:
                self.retrycount += 1
                time.sleep(15)
                self.execute(self, *args, **kw)
            else:
                self.retrycount = 0

    def connect(self):
        try:
            self._session = objectstore.current
        except Exception, err:
            print str(err)

    @property
    def session(self):
        return self._session

    @property
    def retrylimit(self):
        return self._retrylimit
