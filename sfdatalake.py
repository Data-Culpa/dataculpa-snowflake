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
import yaml

import snowflake.connector

from dataculpa import DataCulpaValidator


#logging.basicConfig(format='%(asctime)s %(message)s', level=logging.WARN)


def FatalError(rc, message):
    sys.stderr.write(message)
    sys.stderr.write("\n")
    sys.stderr.flush()
    os._exit(rc)
    return

class Config:
    def __init__(self):
        self._d = { 
                    'dataculpa_controller': {
                        'host': 'dataculpa-api',
                        'port': 7777,
                        #'url': 'http://192.168.1.13:7777',
                        'api-secret': 'set in .env file $DC_CONTROLLER_SECRET; not here',
                    },
                    'snowflake': {
                        'user': 'user',
                        'account': 'account',
                        'region': 'region',
                        'database': 'database',
                        'warehouse': 'warehouse',
                        'table_list': []
                    },
                    'dataculpa_pipeline': {
                        'name': '[required] pipeline_name',
                        'environment': '[optional] environment, e.g., test or production',
                        'stage': '[optional] a string representing the stage of this part of the pipeline',
                        'table_is_stage': True, # use the table name as the stage name
                        'version': '[optional] a string representing the version of implementation'
                    }
        }


    def save(self, fname):
        if os.path.exists(fname):
            print("%s exists already; rename it before creating a new example config." % fname)
            os._exit(1)
            return

        f = open(fname, 'w')
        yaml.safe_dump(self._d, f, default_flow_style=False)
        f.close()
        return

    def load(self, fname):
        with open(fname, "r") as f:
            #print(f)
            self._d = yaml.load(f, yaml.SafeLoader)
            #print(self._d)
        
        # No--just keep this in main.
        #dotenv.load_dotenv(env_file)

        # FIXME: error checking?
        # dump to stderror, non-zero exit... maybe need an error path to push to Data Culpa.
        return

    def load_env(self, env_file):
        return

    def get_snowflake(self):
        return self._d.get('snowflake')
    
    def get_sf_user(self):
        return self.get_snowflake().get('user')
    
    def get_sf_account(self):
        return self.get_snowflake().get('account')
    
    def get_sf_password(self):
        return os.environ.get('SNOWFLAKE_PASSWORD')
    
    def get_sf_region(self):
        return self.get_snowflake().get('region')

    def get_sf_database(self):
        return self.get_snowflake().get('database')

    def get_sf_warehouse(self):
        return self.get_snowflake().get('warehouse')

    def get_sf_table_list(self):
        return self.get_snowflake().get('table_list')

    def get_controller(self):
        return self._d.get('dataculpa_controller')
    
    def get_pipeline(self):
        return self._d.get('dataculpa_pipeline')

    def get_pipeline_name(self):
        return self.get_pipeline().get('name')

    def get_pipeline_table_is_stage(self):
        return self.get_pipeline().get('table_is_stage', False)

    def connect_controller(self):
        pipeline_name = self.get_pipeline_name()
        
        cc = self.get_controller()
        host = cc.get('host')
        port = cc.get('port')

        # FIXME: load Data Culpa secret
        v = DataCulpaValidator(pipeline_name,
                               protocol=DataCulpaValidator.HTTP,
                               dc_host=host,
                               dc_port=port)
        return v

def ConnectToSnowflake(config):
    print("connecting")
    # Gets the version
    sf_context = snowflake.connector.connect(
        user=config.get_sf_user(),
        password=config.get_sf_password(),
        account=config.get_sf_account()
        ) # FIXME: add region?
#    cs = sf_context.cursor()

    return sf_context


def UseWarehouseDatabaseFromConfig(config, cursor):
    try:
        if config.get_sf_warehouse() is not None:
            print("@@@ using warehouse")
            cursor.execute("USE warehouse %s" % config.get_sf_warehouse())
        if config.get_sf_database() is not None:
            print("@@@ using database")
            cursor.execute("USE database %s" % config.get_sf_database())

    except Exception as e:
        print(e)
        os._exit(1)

    return

def DiscoverTables(config, sf_context):

    table_names = []

    cs = sf_context.cursor()
    UseWarehouseDatabaseFromConfig(config, cs)
    cs.execute('show tables')
    r = cs.fetchall()
    #print("show tables = __%s__" % r)
    for rr in r:
        tname = rr[1] # FIXME: obviously there must be a better way
        table_names.append(tname)
    # endfor
    cs.close()

    return table_names

def FetchTable(table, config, sf_context):
    cs = sf_context.cursor()
    UseWarehouseDatabaseFromConfig(config, cs)
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
    # endfor

    # build select.
    fields_str = ",".join(field_names)
    sql = "select %s from %s" % (fields_str, table) # order by / limit N

    cs.execute(sql)

    dc = config.connect_controller()

    r = cs.fetchall()
    for rr in r:
        #print(rr)
        df_entry = {}
        for i in range(0, len(rr)):
            df_entry[field_names[i]] = rr[i]
        dc.queue_record(df_entry)

    cs.close()

    (_queue_id, _result) = dc.queue_commit()
    print("server_result: __%s__" % _result)

    return

def CloseSnowflake(sf_context):
    sf_context.close()
    return

def do_init(filename):
    print("Initialize new file")
    config = Config()
    config.save(filename)
    return

def do_discover(filename):
    print("discover with config from file %s" % filename)
    config = Config()
    config.load(filename)
    sf_context = ConnectToSnowflake(config)

    table_names = DiscoverTables(config, sf_context)
    print(table_names)

    return

def do_test(filename):
    print("test with config from file %s" % filename)
    return

def do_run(filename):
    print("run with config from file %s" % filename)
    config = Config()
    config.load(filename)
    
    # get the table list...
    table_list = config.get_sf_table_list()
    if len(table_list) == 0:
        FatalError(1, "no tables listed to triage!")
        return

    sf_context = ConnectToSnowflake(config)

    for t in table_list:
        FetchTable(t, config, sf_context)
    
    return

def main():
    global gConfig

    ap = argparse.ArgumentParser()
    ap.add_argument("-e", "--env",
                    help="Use provided env file instead of default .env")

    ap.add_argument("--init", help="Init a yaml config file to fill in.")
    ap.add_argument("--discover", help="Run the specified configuration to discover available databases/tables in Snowflake")
    ap.add_argument("--test", help="Test the configuration specified.")
    ap.add_argument("--run", help="Normal operation: run the pipeline")
    args = ap.parse_args()

    env_path = ".env"
    if args.env:
        env_path = args.env
    if not os.path.exists(env_path):
        sys.stderr.write("Error: missing env file at %s\n" % os.path.realpath(env_path))
        os._exit(1)
        return
    # endif

    dotenv.load_dotenv(env_path)

    if args.init:
        do_init(args.init)
        return
    elif args.discover:
        do_discover(args.discover)
        return
    elif args.test:
        do_test(args.test)
        return
    elif args.run:
        do_run(args.run)
        return

    ap.print_help()
    return

if __name__ == "__main__":
    main()

"""
    table_names = ConnectToSnowflake()

    for t in table_names:
        FetchTable(t)

    if sf_context is not None:
        sf_context.close()

"""