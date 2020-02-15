# -*- coding: utf-8 -*-
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import print_function
import os 
import airflow
import logging
from datetime import datetime, timedelta
from airflow.operators.python_operator import PythonOperator
from airflow import models
from airflow.settings import Session
from configparser import ConfigParser


args = {
    'owner': 'airflow',
    'start_date': airflow.utils.dates.days_ago(1),
    'provide_context': True
}


def initialize_etl_example():
    logging.info('Reading config')
    session = Session()

    def create_new_conn(session, attributes):
        new_conn = models.Connection()
        new_conn.conn_id = attributes.get("conn_id")
        new_conn.conn_type = attributes.get('conn_type')
        new_conn.host = attributes.get('host')
        new_conn.port = attributes.get('port')
        new_conn.schema = attributes.get('schema')
        new_conn.login = attributes.get('login')
        new_conn.set_password(attributes.get('password'))

        session.add(new_conn)
        session.commit()

    def config(filename='../.postgresql.ini'):
        # create a parser
        parser = ConfigParser(strict=False)
        # read config file
        parser.read(filename)
        # get section, default to postgresql
        db_config = {}
        logging.info('Creating connections, pool and sql path')

        for section in parser.sections():
            conn_id = section
            conn_type = section.split('_')
            for param in parser.items(section):
                db_config[param[0]] = param[1]

            create_new_conn(session,
                    {"conn_id": conn_id,
                     "conn_type": conn_type[0],
                     "host": db_config['host'],
                     "port": db_config['port'],
                     "schema": db_config['dbname'],
                     "login": db_config['user'],
                     "password": db_config['password']})

    # create_new_conn(session,
    #                 {"conn_id": "postgres_dwh",
    #                  "conn_type": "postgres",
    #                  "host": "localhost",
    #                  "port": 5432,
    #                  "schema": "dwh",
    #                  "login": "jackychu",
    #                  "password": "!A(2813236)#$"})

    config()

    new_var = models.Variable()
    new_var.key = "sql_path"
    new_var.set_val("/Users/jackychu/airflow/sql")
    session.add(new_var)
    session.commit()

    new_pool = models.Pool()
    new_pool.pool = "postgres_dwh"
    new_pool.slots = 10
    new_pool.description = "Allows max. 10 connections to the DWH"
    session.add(new_pool)
    session.commit()

    session.close()

dag = airflow.DAG(
    'init_example',
    schedule_interval="@once",
    default_args=args,
    max_active_runs=1)

t1 = PythonOperator(task_id='initialize_etl_example',
                    python_callable=initialize_etl_example,
                    provide_context=False,
                    dag=dag)
