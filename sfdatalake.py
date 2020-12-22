#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#
# sfdatalake.py
# Data Culpa Snowflake Connector
#
# Copyright (c) 2020 Data Culpa, Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to 
# deal in the Software without restriction, including without limitation the 
# rights to use, copy, modify, merge, publish, distribute, sublicense, and/or
# sell copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
# all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS 
# OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING 
# FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER 
# DEALINGS IN THE SOFTWARE.
#

import argparse
import dotenv
#import logging
import os
import uuid
import sqlite3
import sys

import snowflake.connector

from dataculpa import DataCulpaValidator


#logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARN)


class Config:
    def __init__(self):
        self.snowflake_user    = os.environ.get('SNOWFLAKE_USER')
        self.snowflake_pass    = os.environ.get('SNOWFLAKE_PASSWORD')
        self.snowflake_account = os.environ.get('SNOWFLAKE_ACCOUNT')
        self.snowflake_region  = os.environ.get('SNOWFLAKE_REGION')
        self.snowflake_db      = os.environ.get('SNOWFLAKE_DB')
        self.snowflake_wh      = os.environ.get('SNOWFLAKE_WH')

        #self.snowflake_ = os.environ.get('SNOWFLAKE_USER')
        #self.file_ext         = os.environ.get('AZURE_FILE_EXT')
        #self.storage_cache_db = os.environ.get('AZURE_STORAGE_CACHE')
        #self.error_log        = os.environ.get('AZURE_ERROR_LOG', "error.log")

        # Data Culpa parameters
        self.pipeline_name      = os.environ.get('DC_PIPELINE_NAME')
        self.pipeline_env       = os.environ.get('DC_PIPELINE_ENV', 'default')
        self.pipeline_stage     = os.environ.get('DC_PIPELINE_STAGE', None)
        self.pipeline_version   = os.environ.get('DC_PIPELINE_VERSION', 'default')
        self.dc_host            = os.environ.get('DC_HOST')
        self.dc_port            = os.environ.get('DC_PORT')
        self.dc_protocol        = os.environ.get('DC_PROTOCOL')
        self.dc_secret          = os.environ.get('DC_SECRET')

        self.table_is_stage = os.environ.get('DC_TABLE_IS_STAGE', False)

        if self.table_is_stage:
            assert self.pipeline_stage is None, "cannot set both DC_TABLE_IS_STAGE and DC_DIR_IS_STAGE"


sf_context = None

def ConnectToSnowflake():
    global gConfig
    global sf_context

    print("connecting")
    # Gets the version
    sf_context = snowflake.connector.connect(
        user=gConfig.snowflake_user,
        password=gConfig.snowflake_pass,
        account=gConfig.snowflake_account
        ) # FIXME: add region?
    cs = sf_context.cursor()

    table_names = []

    try:
        if gConfig.snowflake_wh is not None:
            print("@@@ using warehouse")
            cs.execute("USE warehouse %s" % gConfig.snowflake_wh)
        if gConfig.snowflake_db is not None:
            print("@@@ using warehouse")
            cs.execute("USE database %s" % gConfig.snowflake_db)

        cs.execute('show tables')
        r = cs.fetchall()
        #print("show tables = __%s__" % r)
        for rr in r:
            tname = rr[1] # FIXME: obviously there must be a better way
            table_names.append(tname)
        # endfor
    except Exception as e:
        print(e)
        # !
        # something.
    finally: 
        cs.close()

    return table_names

def FetchTable(table):
    global sf_context

    cs = sf_context.cursor()
    #print("\n\n\n@@@ TABLE __%s__" % table)
    #cs.execute('SELECT table_schema FROM INFORMATION_SCHEMA.TABLES WHERE table_name=%s' % table)
    #cs.execute("select * from %s" % table)

    print("@@@ FETCHTABLE %s @@@" % table)

    cs.execute('show columns in ' + table)

    field_types = {}
    field_names = []
    r = cs.fetchall()
    for rr in r:
        # (table name, public [is this the schema?], field_name, type_info...)
        field_name = rr[2]
        field_type = rr[3]

        field_names.append(field_name)
        field_types[field_name] = field_type
#        print(field_name, field_type)
    # endfor
#    print(r)

    # build select.

    fields_str = ",".join(field_names)
    sql = "select %s from %s" % (fields_str, table) # order by / limit N

    cs.execute(sql)

    r = cs.fetchall()
    for rr in r:
        print(rr)

    cs.close()

    # need to bundle this up for pushing to Data Culpa
    # 


    return

#        cs.execute("SELECT current_database()")
#        one_row = cs.fetchone()
#        print(one_row[0])

    #    cs.execute("CREATE TABLE test_table(name string,age integer)")
#        cs.execute("INSERT INTO test_table VALUES ('bob', 43)")
#        cs.execute("SELECT * FROM test_table");
#        print(cs.fetchone())

#    finally:
#        cs.close()
#    ctx.close()


def CloseSnowflake():
    global sf_context
    sf_context.close()
    return

gConfig = None

def main():
    global gConfig

    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env",
                    help="Use provided env file instead of default .env")

    ap.add_argument("--init", help="Init a yaml config file to fill in.")
    ap.add_argument("--discover", help="Run the specified configuration to discover available databases/tables in Snowflake")
    ap.add_argument("--test", help="Test the configuration specified.")
    
    args = ap.parse_args()

    env_path = ".env"
    if args.env:
        env_path = args.env
    if not os.path.exists(env_path):
        sys.stderr.write("Error: missing env file at %s\n" % env_path)
        os._exit(1)
        return
    # endif

    dotenv.load_dotenv()

    gConfig = Config()
    table_names = ConnectToSnowflake()

    for t in table_names:
        FetchTable(t)

    if sf_context is not None:
        sf_context.close()
    return

if __name__ == "__main__":
    main()

