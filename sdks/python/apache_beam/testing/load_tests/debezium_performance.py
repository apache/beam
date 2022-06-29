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
    def __init__(self):
        self.username = 'postgres'
        self.password = 'uuinkks' 
        # de kubernetes
        self.database = 'postgres'
        self.port = "5432"
        self.host = os.environ['kubernetesPostgres']
        #self.host = '146.148.98.7' # Ip provisional, una vez que 
        self.connector_class = DriverClassName.POSTGRESQL
        self.connection_properties = [
            "database.dbname=postgres",
            "database.server.name=postgres",
            "database.include.list=postgres",
            "include.schema.changes=false",
            "plugin.name=pgoutput"
        ]

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
        alterTableReplica = "ALTER TABLE postgres REPLICA IDENTITY FULL;"
        cursor.execute(alterTableReplica)
        startTime = time.time()
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
            elif action == 2: # Update 
                updateQuery = """UPDATE postgres 
                                    SET word = 'apache'
                                    WHERE id IN (SELECT max(id) from postgres)"""

                cursor.execute(updateQuery)                
            else: # Insert all the other numbers
                number = random.randint(1,1000)
                boolean = bool(random.getrandbits(1))
                insertQuery  = """INSERT INTO postgres(id,word,number,date,bool)
                                    VALUES(%s,%s,%s,%s,%s);"""
                cursor.execute(insertQuery,(str(insert),"apacheBeam",str(number),"05/03/1999",str(boolean)))
                insert += 1
            currentTime = time.time()
            elapsedTime = currentTime - startTime
            time.sleep(1) # Ask pablo if it's correct wait between inserts, updates and deletes 
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
                | "label1" >> beam.Create(['debezium'])
                | "label2" >> beam.ParDo(lambda x: self.randomInsertTest(self.initConnection()))
                | "label3" >> beam.ParDo(lambda x: self.readFromDebezium())
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
            ) 

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    debeziumTest = DebeziumLoadTest()
    print ("Hello world testing ")
    logging.info("Hello world log testing")
    debeziumTest.createPipeline()
    #print ("Dataflow Job run well")
    #logging.info("Dataflow Job run well")
    #debeziumTest.randomInsertTest(debeziumTest.initConnection())
    print ("Insert into database succefully")
    logging.info("Insert into database succefully ")
    #debeziumTest.readFromDebezium()
    print ("Debezium read correctly")
    logging.info("Debezium read correctly ")
