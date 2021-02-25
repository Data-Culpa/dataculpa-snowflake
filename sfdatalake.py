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
import json
import os
import pickle
import uuid
import sqlite3
import sys
import traceback
import yaml

import snowflake.connector

import logging

if False:
    for k,v in  logging.Logger.manager.loggerDict.items():
        if k.find(".") > 0:
            continue        
        print(k)#, v)
        print("---")

for logger_name in ['snowflake.connector', 'urllib3', 'botocore', 'boto3']:
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.WARN)
    #ch = logging.FileHandler('/tmp/python_connector.log')
    #ch.setLevel(logging.DEBUG)
    #ch.setFormatter(logging.Formatter('%(asctime)s - %(threadName)s %(filename)s:%(lineno)d - %(funcName)s() - %(levelname)s - %(message)s'))
    #logger.addHandler(ch)


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
                        'host': 'localhost',
                        'port': 7777,
                    },
                    'configuration': {
                        'user': '[required] user',
                        'account': '[required] account',
                        'region': '[optional] region',
                        'database': '[required] database',
                        'schema': '[optional] schema',
                        'warehouse': '[optional] warehouse',
                        'table_list': {}
                    },
                    'dataculpa_pipeline': {
                        'name': '[required] pipeline_name',
                        'environment': '[optional] environment, e.g., test or production',
                        'stage': '[optional] a string representing the stage of this part of the pipeline',
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
        return self._d.get('configuration')
    
    def get_sf_local_cache_file(self):
        return self.get_snowflake().get('session_history_cache', 'session_history_cache.db')

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

    def connect_controller(self, table_name):
        pipeline_name = self.get_pipeline_name()

        if pipeline_name.find("$TABLE") >= 0:
            pipeline_name = pipeline_name.replace("$TABLE", table_name)

        cc = self.get_controller()
        host = cc.get('host')
        port = cc.get('port')

        # FIXME: load Data Culpa secret
        v = DataCulpaValidator(pipeline_name,
                               protocol=DataCulpaValidator.HTTP,
                               dc_host=host,
                               dc_port=port)
        return v


class SessionHistory:
    def __init__(self):
        self.history = {}

    def add_history(self, table_name, field, value):
        self.history[table_name] = (field, value)
        return
    
    def has_history(self, table_name):
        return self.history.get(table_name) is not None

    def get_history(self, table_name):
        return self.history.get(table_name)
    
    def _handle_new_cache(self, cache_path):
        needs_tables = False
        if not os.path.exists(cache_path):
            # create new
            needs_tables = True
        
        if needs_tables:
            # table will likely have collisions prety quick... 
            c = sqlite3.connect(cache_path)
            c.execute("create table cache (object_name text unique, field_name text, field_value)")
            c.commit()
        # endif
    
        return

    def save(self, config):
        # write to disk
        cache_path = config.get_sf_local_cache_file()
        assert cache_path is not None

        self._handle_new_cache(cache_path)

        c = sqlite3.connect(cache_path)
        for table, f_pair in self.history.items():
            (fn, fv) = f_pair
            fv_pickle = pickle.dumps(fv)
            # Note that this might be dangerous if we add new fields later and we don't set them all...
            print(table, fn, fv)
            c.execute("insert or replace into cache (object_name, field_name, field_value) values (?,?,?)", 
                      (table, fn, fv_pickle))

        c.commit()

        return
    
    def load(self, config):
        # read from disk
        cache_path = config.get_sf_local_cache_file()
        assert cache_path is not None

        self._handle_new_cache(cache_path)

        c = sqlite3.connect(cache_path)
        r = c.execute("select object_name, field_name, field_value from cache")
        for row in r:
            (table, fn, fv_pickle) = row
            fv = pickle.loads(fv_pickle)
            self.add_history(table, fn, fv)
        # endfor
        return


gCache = SessionHistory()

def ConnectToSnowflake(config):
    print("connecting...")
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
            cursor.execute("USE warehouse %s" % config.get_sf_warehouse())
        if config.get_sf_database() is not None:
            cursor.execute("USE database %s" % config.get_sf_database())

    except Exception as e:
        print(e)
        os._exit(1)

    return

def DiscoverTablesAndViews(config, sf_context):
    db_name = config.get_sf_database()
    print("DiscoverTables:", db_name)
    table_names = []

    cs = sf_context.cursor()
    UseWarehouseDatabaseFromConfig(config, cs)

    cs.execute("SHOW TABLES IN DATABASE %s" % db_name)
    r = cs.fetchall()
    for _r in r:
        # https://docs.snowflake.com/en/sql-reference/sql/show-tables.html
        # created datetime, table name, kind, database_name, schema_name... 
        # but there's a lot of other goodies in here too.
        #print("*** %40s %20s" % (_r[1], _r[0]))
        t_name = _r[1]
        if not (t_name in table_names):
            table_names.append(_r[1]) # FIXME: can exist across schemas and such... need to handle this better.
    # endfor

    cs.execute("SHOW VIEWS IN DATABASE %s" % db_name)
    r = cs.fetchall()
    view_names = []
    for _r in r:
        # https://docs.snowflake.com/en/sql-reference/sql/show-tables.html
        # created datetime, table name, kind, database_name, schema_name... 
        # but there's a lot of other goodies in here too.
        #print("*** %40s %20s" % (_r[1], _r[0]))
        t_name = _r[1]
        if not (t_name in view_names):
            view_names.append(_r[1]) # FIXME: can exist across schemas and such... need to handle this better.
    # endfor

    cs.close()

    return table_names, view_names


def DescribeTable(table, config, sf_context):
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

    cs.execute('select count(*) from ' + table)
    r = cs.fetchall()
    print(r)

    return field_names, field_types


def FetchTable(table, config, sf_context, t_order_by, t_initial_limit):
    print(":fetching ... ", table)
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
    # ok we need to see if we have fetched this table before..

    fields_str = ",".join(field_names)
    sql = "select %s from %s " % (fields_str, table)

    # check our history.
    gCache.load(config)
    marker_pair = gCache.get_history(table)
    if marker_pair is not None:
        (fk, fv) = marker_pair
        sql += " WHERE %s > '%s'" % (fk, fv)
    if t_order_by is not None:
        sql += " ORDER BY %s DESC" % t_order_by
    if t_initial_limit is not None:
        # we want to do this only if we don't have a cached object for this table.
        if not gCache.has_history(table):
            sql += " LIMIT %s" % t_initial_limit
    #print(sql)
    cs.execute(sql)

    dc = config.connect_controller(table)

    cache_marker = None
    r = cs.fetchall()
    r_count = 0
    for rr in r:
        r_count += 1
        df_entry = {}
        for i in range(0, len(rr)):
            df_entry[field_names[i]] = rr[i]

            if field_names[i] == t_order_by:
                cache_marker = rr[i]
        dc.queue_record(df_entry)

    if r_count > 0:
        if cache_marker is None:
            if t_order_by is not None:
                print("ERROR: we specified an order by constraint for caching that is missing from the table schema.")
                os._exit(2)
        else:
            # OK, save it off...
            gCache.add_history(table, t_order_by, cache_marker)
            gCache.save(config)
        # endif
    # endif

    cs.close()

    if r_count > 0:
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

    # Put out an .env template too.
    with open(filename + ".env", "w") as f:
        #f.write("DC_CONTROLLER_SECRET=empty\n")
        f.write("SNOWFLAKE_PASSWORD=[required]\n")
        f.close()

    return

def do_discover(filename, table_name):
    print("discover with config from file %s" % filename)
    config = Config()
    config.load(filename)
    sf_context = ConnectToSnowflake(config)

    if table_name:
        # we want to get the schema for the specified table.
        (_names, _type_dict) = DescribeTable(table_name, config, sf_context)

        for n in _names:
            js_str = _type_dict[n]
            js_obj = None
            try:
                js_obj = json.loads(js_str)
            except:
                traceback.print_exc()
                pass
            
            if js_obj is None:
                print(n, js_str)
            else:
                print(n, js_obj.get('type', "missing type in %s" % js_str))
        #print(_names)
        #print(_type_dict)
        return

    (table_names, view_names) = DiscoverTablesAndViews(config, sf_context)
    if len(table_names) == 0 and len(view_names):
        print("No tables or views found; check configuration and/or permissions?")
        os._exit(2)
    # endif

    i = 1
    for t in table_names:
        print(i, ": Found table:", t)
        i += 1
        #DescribeTable(config, sf_context, config.get_sf_database(), t)
    # endfor

    for v in view_names:
        if v in table_names:
            continue
        
        print(i, ": Found view:", v)
        i += 1

    return

def do_test(filename):
    print("test with config from file %s" % filename)
    config = Config()
    config.load(filename)

    # get the table list...
    table_list = config.get_sf_table_list()
    if len(table_list) == 0:
        FatalError(1, "no tables listed to triage!")
        return

    sf_context = ConnectToSnowflake(config)

    for t in table_list:
        print(t)
        #FetchTable(t, config, sf_context)
    
    return

def do_run(filename, table_name):
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
        t_name          = t.get('table')
        t_order_by      = t.get('desc_order_by')
        t_initial_limit = t.get('initial_limit')

        # little safeguard while we test
        if t_initial_limit is None:
            t_initial_limit = 1000

        if table_name is not None:
            if t_name.lower() == table_name.lower():
                FetchTable(t_name, config, sf_context, t_order_by, t_initial_limit)                
        else:
            # normal operation
            FetchTable(t_name, config, sf_context, t_order_by, t_initial_limit)
        # endif
    # endfor

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

    ap.add_argument("--table", help="Operate on the specified table name")
    args = ap.parse_args()


    if args.init:
        do_init(args.init)
        return
    else:
        env_path = ".env"
        if args.env:
            env_path = args.env
        if not os.path.exists(env_path):
            sys.stderr.write("Error: missing env file at %s\n" % os.path.realpath(env_path))
            os._exit(1)
            return
        # endif

        if args.discover:
            dotenv.load_dotenv(env_path)
            do_discover(args.discover, args.table)
            return
        elif args.test:
            dotenv.load_dotenv(env_path)
            do_test(args.test)
            return
        elif args.run:
            dotenv.load_dotenv(env_path)
            do_run(args.run, args.table)
            return
        # endif
    # endif

    ap.print_help()
    return

if __name__ == "__main__":
    main()
