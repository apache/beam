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
package org.apache.beam.sdk.io.aws.redshift;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import java.sql.Connection;
import java.sql.Statement;
import javax.sql.DataSource;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.aws.redshift.Redshift.DataSourceConfiguration;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.ParDo;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Mockito;

/** Tests {@link Copy}. */
@RunWith(JUnit4.class)
public class CopyTest {

  @Test
  public void testCopy() throws Exception {
    DataSource mockDataSource = Mockito.mock(DataSource.class);
    Connection mockConnection = Mockito.mock(Connection.class);
    Statement mockStatement = Mockito.mock(Statement.class);

    when(mockDataSource.getConnection()).thenReturn(mockConnection);
    when(mockConnection.createStatement()).thenReturn(mockStatement);

    AWSCredentialsProvider awsCredentialsProvider =
        new AWSStaticCredentialsProvider(new BasicAWSCredentials("id", "secret"));

    DataSourceConfiguration dataSourceConfiguration =
        DataSourceConfiguration.create(mockDataSource);

    Copy copyFn =
        Copy.builder()
            .setDataSourceConfiguration(dataSourceConfiguration)
            .setDelimiter('^')
            .setSourceCompression(Compression.BZIP2)
            .setDestinationTableSpec("test_table")
            .build();

    TestPipeline testPipeline = TestPipeline.create();
    testPipeline.getOptions().as(AwsOptions.class).setAwsRegion("us-east-1");
    testPipeline.apply(Create.of("s3://bucket/path/prefix-")).apply(ParDo.of(copyFn));
    testPipeline.run();

    String sql =
        "COPY test_table FROM 's3://bucket/path/prefix-' "
            + "CREDENTIALS 'aws_access_key_id=id;aws_secret_access_key=secret' "
            + "DELIMITER AS '^' "
            + "BZIP2 "
            + "NULL AS 'NULL' "
            + "BLANKSASNULL "
            + "IGNOREBLANKLINES "
            + "ESCAPE ";

    verify(mockStatement.execute(sql), times(1));
  }
}
