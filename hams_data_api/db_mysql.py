#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Feb 18 18:54:09 2015

@author: Alwin


database handing functionality
- this class holds alls DB connetions 
- provides all DB commands required for Data API

MYSQL

does not contain the respective DB credentials, these are provided at the
initialization by the credentials file


"""

import logging, os
logging.basicConfig(format="%(asctime)s %(name)s %(levelname)-10s %(message)s")
log = logging.getLogger("hams_data_api/data_api.py")
log.setLevel( os.environ.get("LOG_LEVEL", logging.DEBUG) )


import pymysql
import pandas.io.sql as psql
import pandas as pd

import warnings
warnings.simplefilter(action = "ignore", category = RuntimeWarning)



class database_handler:
    
    def __init__(self, DB_host, DB_user, DB_passwd, DB_db ):
        """
        input:
            DB_host - database host url
            DB_user - database user name
            DB_passwd - user password
            DB_db - database name to connect to 
        """
        self.init = True
        self.host                = 	DB_host
        self.passwd              = 	DB_passwd
        self.user                = 	DB_user
        self.db	                 =	   DB_db

        self.getConn()
        self.init = False


    def closeConn(self):
        try:
            self.conn.close()
        except:
            if not self.init:
                log.exception('can not close mysql connection')
            pass


    def getConn(self):
        self.closeConn()
        try:
            if not self.init:
                log.info( 'try to get new mysql connection')
                
            self.conn = pymysql.connect( host = self.host, passwd = self.passwd, \
                                     user = self.user, db = self.db)
            if not self.init:
                log.info('got new mysql connection')

        except:
             log.exception("Exception thrown in getting new mysql connection!")
             raise
        else:
            return True


    def getResultsAsDF(self, query, retry=0):
        #returns results of query in pandas.DataFrame format  
        results = pd.DataFrame()
        try:
            results = psql.read_sql(query, con=self.conn)
        except:
            if retry==0:
                log.warning('problem in mysql execurteQuery: try to reset mysql connection')
                log.info("problem query: \n %s" % query)
                self.getConn()
                results = self.getResultsAsDF(query, retry=1)
                pass
            else:
                log.exception("Exception thrown in getResults!")
                log.info("problem query: \n %s" % query)
                pass
        return results