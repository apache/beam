/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.beam.sdk.io.influxdb;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.powermock.api.mockito.PowerMockito.doReturn;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;
import okhttp3.OkHttpClient;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.PipelineResult.State;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.values.PCollection;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.influxdb.dto.QueryResult.Series;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 80a6c0bda3 (spotlessCheck)
@PrepareForTest({
  org.influxdb.InfluxDBFactory.class,
  org.apache.beam.sdk.io.influxdb.InfluxDbIO.InfluxDBSource.class
})
public class InfluxDbIOTest {
<<<<<<< HEAD
=======
@PrepareForTest({org.influxdb.InfluxDBFactory.class, org.apache.beam.sdk.io.influxdb.InfluxDbIO.InfluxDBSource.class})
public class InfluxDbIOTest  {
>>>>>>> c793375e14 (Addressing the comments)
=======
>>>>>>> 80a6c0bda3 (spotlessCheck)

  @Rule public final TestPipeline pipeline = TestPipeline.create();
  InfluxDB influxDb;

  @Before
<<<<<<< HEAD
<<<<<<< HEAD
  public void setupTest() {
    PowerMockito.mockStatic(InfluxDBFactory.class);
    influxDb = Mockito.mock(InfluxDB.class);
    PowerMockito.when(
            InfluxDBFactory.connect(
                anyString(), anyString(), anyString(), any(OkHttpClient.Builder.class)))
        .thenReturn(influxDb);
    PowerMockito.when(InfluxDBFactory.connect(anyString(), anyString(), anyString()))
        .thenReturn(influxDb);
  }

=======
  public void setupTest(){
=======
  public void setupTest() {
>>>>>>> 80a6c0bda3 (spotlessCheck)
    PowerMockito.mockStatic(InfluxDBFactory.class);
    influxDb = Mockito.mock(InfluxDB.class);
    PowerMockito.when(
            InfluxDBFactory.connect(
                anyString(), anyString(), anyString(), any(OkHttpClient.Builder.class)))
        .thenReturn(influxDb);
    PowerMockito.when(InfluxDBFactory.connect(anyString(), anyString(), anyString()))
        .thenReturn(influxDb);
  }
<<<<<<< HEAD
>>>>>>> c793375e14 (Addressing the comments)
=======

>>>>>>> 80a6c0bda3 (spotlessCheck)
  @Test
  public void validateWriteTest() {
    String influxHost = "http://localhost";
    String userName = "admin";
    String password = "admin";
    String influxDatabaseName = "testDataBase";
<<<<<<< HEAD
<<<<<<< HEAD
=======
>>>>>>> 80a6c0bda3 (spotlessCheck)
    AtomicInteger countInvocation = new AtomicInteger();
    Mockito.doAnswer(invocation -> countInvocation.getAndIncrement())
        .when(influxDb)
        .write(any(String.class));
<<<<<<< HEAD
    doReturn(getDatabase(influxDatabaseName)).when(influxDb).query(new Query("SHOW DATABASES"));
    final int numOfElementsToWrite = 1000;
    pipeline
        .apply("Generate data", Create.of(GenerateData.getMetric(numOfElementsToWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(influxHost, userName, password))
                .withDatabase(influxDatabaseName)
                .withSslInvalidHostNameAllowed(true)
                .withSslEnabled(false));
    PipelineResult result = pipeline.run();
=======
    AtomicInteger countInvocation= new AtomicInteger();
    Mockito.doAnswer(invocation -> countInvocation.getAndIncrement()).when(influxDb).write(any(String.class));
    doReturn(getDatabase(influxDatabaseName)).when(influxDb).query(new Query("SHOW DATABASES"));
    final int numOfElementsToWrite = 1000;
    pipeline
          .apply("Generate data", Create.of(GenerateData.getMetric(numOfElementsToWrite)))
          .apply(
              "Write data to InfluxDB",
              InfluxDbIO.write()
                  .withConfiguration(
                      InfluxDbIO.DataSourceConfiguration.create(
                          influxHost,
                          userName,
                          password))
                  .withDatabase(influxDatabaseName)
                  .withSslInvalidHostNameAllowed(true)
                  .withSslEnabled(false));
    PipelineResult result =  pipeline.run();
>>>>>>> c793375e14 (Addressing the comments)
=======
    doReturn(getDatabase(influxDatabaseName)).when(influxDb).query(new Query("SHOW DATABASES"));
    final int numOfElementsToWrite = 1000;
    pipeline
        .apply("Generate data", Create.of(GenerateData.getMetric(numOfElementsToWrite)))
        .apply(
            "Write data to InfluxDB",
            InfluxDbIO.write()
                .withConfiguration(
                    InfluxDbIO.DataSourceConfiguration.create(influxHost, userName, password))
                .withDatabase(influxDatabaseName)
                .withSslInvalidHostNameAllowed(true)
                .withSslEnabled(false));
    PipelineResult result = pipeline.run();
>>>>>>> 80a6c0bda3 (spotlessCheck)
    Assert.assertEquals(State.DONE, result.waitUntilFinish());
    Assert.assertEquals(numOfElementsToWrite, countInvocation.get());
  }

  @Test
  public void validateReadTest() {
    String influxHost = "http://localhost";
    String userName = "admin";
    String password = "admin";
    String influxDatabaseName = "testDataBase";

    doReturn(getDatabase(influxDatabaseName)).when(influxDb).query(new Query("SHOW DATABASES"));
    doReturn(getDatabase(influxDatabaseName)).when(influxDb).query(new Query("show shards"));
<<<<<<< HEAD
<<<<<<< HEAD
    doReturn(mockResult("cpu", 20))
        .when(influxDb)
        .query(new Query("SELECT * FROM cpu", influxDatabaseName));

    PCollection<Long> data =
        pipeline
            .apply(
                "Read data to InfluxDB",
                InfluxDbIO.read()
                    .withDataSourceConfiguration(
                        InfluxDbIO.DataSourceConfiguration.create(influxHost, userName, password))
                    .withDatabase(influxDatabaseName)
                    .withQuery("SELECT * FROM cpu")
                    .withSslInvalidHostNameAllowed(true)
                    .withSslEnabled(false))
            .apply(Count.globally());
    PAssert.that(data).containsInAnyOrder(20L);
    PipelineResult result = pipeline.run();
    Assert.assertEquals(State.DONE, result.waitUntilFinish());
  }

=======
    doReturn(mockResult("cpu", 20)).when(influxDb).query(new Query("SELECT * FROM cpu", influxDatabaseName));
=======
    doReturn(mockResult("cpu", 20))
        .when(influxDb)
        .query(new Query("SELECT * FROM cpu", influxDatabaseName));
>>>>>>> 80a6c0bda3 (spotlessCheck)

    PCollection<Long> data =
        pipeline
            .apply(
                "Read data to InfluxDB",
                InfluxDbIO.read()
                    .withDataSourceConfiguration(
                        InfluxDbIO.DataSourceConfiguration.create(influxHost, userName, password))
                    .withDatabase(influxDatabaseName)
                    .withQuery("SELECT * FROM cpu")
                    .withSslInvalidHostNameAllowed(true)
                    .withSslEnabled(false))
            .apply(Count.globally());
    PAssert.that(data).containsInAnyOrder(20L);
    PipelineResult result = pipeline.run();
    Assert.assertEquals(State.DONE, result.waitUntilFinish());
  }
<<<<<<< HEAD
>>>>>>> c793375e14 (Addressing the comments)
=======

>>>>>>> 80a6c0bda3 (spotlessCheck)
  private QueryResult getDatabase(String name) {
    QueryResult queryResult = new QueryResult();
    QueryResult.Series series = new Series();
    series.setName("databases");
<<<<<<< HEAD
<<<<<<< HEAD
    List<Object> db = new ArrayList<>();
=======
    List<Object> db= new ArrayList<>();
>>>>>>> c793375e14 (Addressing the comments)
=======
    List<Object> db = new ArrayList<>();
>>>>>>> 80a6c0bda3 (spotlessCheck)
    db.add(name);
    List<List<Object>> values = new ArrayList<>();
    values.add(db);
    series.setValues(values);
    List<QueryResult.Series> qSeries = new ArrayList<>();
    qSeries.add(series);
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(qSeries);
    List<QueryResult.Result> listResult = new ArrayList<>();
    listResult.add(result);
    queryResult.setResults(listResult);
    return queryResult;
  }

  private QueryResult mockResult(String metricName, int numberOfRecords) {
    QueryResult queryResult = new QueryResult();
    QueryResult.Series series = new Series();
    series.setName(metricName);
    series.setColumns(Arrays.asList("time", "value"));
    List<List<Object>> values = new ArrayList<>();
<<<<<<< HEAD
<<<<<<< HEAD
    for (int i = 0; i < numberOfRecords; i++) {
=======
    for (int i=0; i< numberOfRecords; i++) {
>>>>>>> c793375e14 (Addressing the comments)
=======
    for (int i = 0; i < numberOfRecords; i++) {
>>>>>>> 80a6c0bda3 (spotlessCheck)
      List<Object> metricData = new ArrayList<>();
      Date now = new Date();
      SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ssZ");
      metricData.add(simpleDateFormat.format(now));
      metricData.add(new Random().nextInt(100));
      values.add(metricData);
    }
    series.setValues(values);
    List<QueryResult.Series> queryResultSeries = new ArrayList<>();
    queryResultSeries.add(series);
    QueryResult.Result result = new QueryResult.Result();
    result.setSeries(queryResultSeries);
    List<QueryResult.Result> listResult = new ArrayList<>();
    listResult.add(result);
    queryResult.setResults(listResult);
    return queryResult;
  }
}
