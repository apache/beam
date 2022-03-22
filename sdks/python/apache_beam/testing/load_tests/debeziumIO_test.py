#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import logging 
import unittest
import time 
import random
import psycopg2

from apache_beam.io.debezium import DriverClassName
from apache_beam.io.debezium import ReadFromDebezium
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

try:
  from testcontainers.postgres import PostgresContainer
except ImportError:
  PostgresContainer = None

@unittest.skipIf(
    PostgresContainer is None, 'testcontainers package is not installed')
@unittest.skipIf(
    TestPipeline().get_pipeline_options().view_as(StandardOptions).runner is
    None,
    'Do not run this test on precommit suites.')

class DebeziumIOLoadTest():
    def startDbContainers(self,retries):
        for i in range (retries):
            try:
                self.db = PostgresContainer(
                    'debezium/example-postgres:latest',
                    user=self.username,
                    password=self.password, 
                    dbname=self.database
                )
                self.db.start()
                break
            except Exception as e:
                if i == retries - 1:
                    logging.error('Unable to initialize DB container.')
                    raise e

    def setUp(self):
        self.username = 'debezium'
        self.password = 'dbz'
        self.database = 'inventory'
        self.startDbContainers(retries=1)
        self.host = self.db.get_container_host_ip()
        self.port = self.db.get_exposed_port(5432)
        self.connector_class = DriverClassName.POSTGRESQL
        self.connection_properties = [
            "database.dbname=inventory",
            "database.server.name=dbserver1",
            "database.include.list=inventory",
            "include.schema.changes=false"
        ]

    def tearDown(self):
        try:
            self.db.stop()
        except:
            logging.error('Could not stop the DB contatiner.')

    def initConnection(self):
        connection = psycopg2.connect(
            host = self.host,
            database = self.database,
            user = self.username,
            password = self.password
        )
        return connection

    def randomInsertTest(self,connection):
        insert = 0
        update = 0
        delete = 0 
        cursor = connection.cursor()
        createTable = """
            CREATE TABLE IF NOT EXISTS debeziumtest(
                id NUMERIC,
                word VARCHAR(50),
                number NUMERIC,
                date DATE,
                bool BOOLEAN
            )
        """
        cursor.execute(createTable)
        startTime = time.time()
        testDuration = 120 # Seconds , 20 Minutes
        timeFlag = True
        while timeFlag:
            action = random.randint(1,10)
            if action == 1: # Delete 
                deleteQuery  = """DELETE FROM debeziumtest
                                    WHERE id IN (
                                    SELECT id FROM
                                    debeziumtest WHERE word='apacheBeam' LIMIT 1
                                    )"""
                cursor.execute(deleteQuery)
                delete += 1                 
            elif action == 2: # Updatet 
                updateQuery = """UPDATE debeziumtest 
                                    SET word = 'apache'
                                    WHERE id IN (SELECT max(id) from debeziumtest)"""
    
                cursor.execute(updateQuery)
                update += 1
            else: # Insert all the other numbers
                number = random.randint(1,1000)
                boolean = bool(random.getrandbits(1))
                insertQuery  = """INSERT INTO debeziumtest(id,word,number,date,bool)
                                    VALUES(%s,%s,%s,%s,%s);"""
                cursor.execute(insertQuery,(str(insert),"apacheBeam",str(number),"24/03/1999",str(boolean))) 
                insert += 1
            currentTime = time.time()
            elapsedTime = currentTime - startTime
            time.sleep(1)
            if elapsedTime > testDuration:
                timeFlag = False
        
        connection.commit()
        cursor.close()  

if __name__ == '__main__':
  logging.basicConfig(level=logging.INFO)
  print ("Hello world testing ")
  logging.info("Hello world log testing")
  debezium = DebeziumIOLoadTest()
  debezium.setUp()
  print ("Testing set up database ")
  logging.info("Testing set up database")
  debezium.randomInsertTest(debezium.initConnection())
  print ("Testing random insert database ")
  logging.info("Testing random insert database")
  debezium.tearDown()
  print ("Testing tear down database ")
  logging.info("Testing test down  database")


