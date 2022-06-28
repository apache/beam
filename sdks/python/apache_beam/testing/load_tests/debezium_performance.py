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

"""
Load test for operations involving side inputs.

The purpose of this test is to measure the cost of materialization and
accessing side inputs. The test uses synthetic source which can be
parametrized to generate records with various sizes of keys and values,
impose delays in the pipeline and simulate other performance challenges.

This test can accept the following parameters:
  * side_input_type (str) - Required. Specifies how the side input will be
    materialized in ParDo operation. Choose from (dict, iter, list).
  * window_count (int) - The number of fixed sized windows to subdivide the
    side input into. By default, a global window will be used.
  * access_percentage (int) - Specifies the percentage of elements in the side
    input to be accessed. By default, all elements will be accessed.

Example test run:

// Modify this 
python -m apache_beam.testing.load_tests.debezium_performancee \
    --test-pipeline-options="
    --side_input_type=iter
    --input_options='{
    \"num_records\": 300,
    \"key_size\": 5,
    \"value_size\": 15
    }'"

or:

./gradlew -PloadTest.args="
    --side_input_type=iter
    --input_options='{
      \"num_records\": 300,
      \"key_size\": 5,
      \"value_size\": 15}'" \
-PloadTest.mainClass=apache_beam.testing.load_tests.debezium_performance \
-Prunner=DirectRunner :sdks:python:apache_beam:testing:load_tests:run
"""

import psycopg2
import os
import time
import random
import logging

import apache_beam as beam 
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.testing.load_tests.load_test import LoadTest
from apache_beam.io.debezium import DriverClassName
from apache_beam.io.debezium import ReadFromDebezium
from apache_beam.options.pipeline_options import StandardOptions
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

class DebeziumLoadTest(LoadTest, beam.DoFn):

    # Define todas las variables que va a utilizar 
    def setUp(self):
        self.username = 'postgres'
        self.password = 'uuinkks' # El password es el que esta en el archivo de configuracion
        # de kubernetes
        self.database = 'postgres'
        self.port = "5432"
        #self.host = os.environ['kubernetesPostgres']
        self.host = '35.188.113.25' # Ip provisional, una vez que 
        self.connector_class = DriverClassName.POSTGRESQL
        self.connection_properties = [
            "database.dbname=postgres",
            "database.server.name=postgres",
            "database.include.list=postgres",
            "include.schema.changes=false",
            "plugin.name=pgoutput"
        ]
        # Configuraciones tomadas de /sdks/python/apache_beam/io/external/xlang_debeziumio_it_test.py
    # donde se utiliza el conector de debezium 

    def initConnection(self): # Conectarse con la base de datos y regresa un objeto de conexion
        connection = psycopg2.connect(
            host = self.host,
            database = self.database,
            user = self.username,
            password = self.password
        )
        return connection
    # Metodo para hacer inserts, updates y deletes a la base
    def randomInsertTest(self,connection): 
        insert = 0
        update = 0
        delete = 0
        cursor = connection.cursor()
        createTable = """
            CREATE TABLE IF NOT EXISTS postgres(
                id NUMERIC,
                word VARCHAR(50),
                number NUMERIC,
                date DATE,
                bool BOOLEAN
            )
        """
        cursor.execute(createTable)
        startTime = time.time()
        # La duracion que sugirio pablo fue de 20 minutos, 
        # para pruebas estoy usando 2 minutos
        testDuration = 120 # Seconds , 20 Minutes
        timeFlag = True
        while timeFlag:
            action = random.randint(1,10)
            if action == 1: # Delete 
                deleteQuery  = """DELETE FROM postgres
                                    WHERE id IN (
                                    SELECT id FROM
                                    postgres WHERE word='apacheBeam' LIMIT 1
                                    )"""
                cursor.execute(deleteQuery)
                delete += 1
            elif action == 2: # Update 
                updateQuery = """UPDATE postgres 
                                    SET word = 'apache'
                                    WHERE id IN (SELECT max(id) from postgres)"""

                cursor.execute(updateQuery)
                update += 1
            else: # Insert all the other numbers
                number = random.randint(1,1000)
                boolean = bool(random.getrandbits(1))
                insertQuery  = """INSERT INTO postgres(id,word,number,date,bool)
                                    VALUES(%s,%s,%s,%s,%s);"""
                cursor.execute(insertQuery,(str(insert),"apacheBeam",str(number),"05/03/1999",str(boolean)))
                insert += 1
            currentTime = time.time()
            elapsedTime = currentTime - startTime
            time.sleep(1)
            if elapsedTime > testDuration:
                timeFlag = False

        connection.commit()
        cursor.close()
    # En esta funcion, creare el pipeline que se enviara a los diferentes runners 
    def createPipeline(self):
        beam_options = PipelineOptions(
        runner='DataflowRunner',
        project='apache-beam-testing',
        job_name='debezium-load-test',
        temp_location='gs://my-bucket/temp',
        )       
        with beam.Pipeline() as pipeline:
            debeziumTest = (
                pipeline 
                | beam.Create(['']
                | beam.ParDo(self.randomInsertTest(self.initConnection()))
                | # PPARDO read from debezium 
                )
            )     
    #Utilizar reed from debezium 
    def readFromDebezium(self):
        with TestPipeline() as p:
            p.not_use_test_runner_api = True
            results = (
                p
                | 'Read from debezium' >> ReadFromDebezium(
                    username=self.username,
                    password=self.password,
                    host=self.host,
                    port=self.port,
                    max_number_of_records=1,
                    connector_class=self.connector_class,
                    connection_properties=self.connection_properties)
                | p >> beam.map(print))
                # Assert para verificar que todo este bien 
            
if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    debeziumTest = DebeziumLoadTest()
    debeziumTest.setUp()
    print ("Hello world testing ")
    logging.info("Hello world log testing")
    #debeziumTest.createPipeline()
    #print ("Dataflow Job run well")
    #logging.info("Dataflow Job run well")
    debeziumTest.randomInsertTest(debeziumTest.initConnection())
    print ("Insert into database succefully")
    logging.info("Insert into database succefully ")
    debeziumTest.readFromDebezium()
    print ("Debezium read correctly")
    logging.info("Debezium read correctly ")
    


 