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

/**
 * Tests {@link Copy}.
 */
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

    Copy copyFn = Copy.builder()
        .setDataSourceConfiguration(dataSourceConfiguration)
        .setDelimiter('^')
        .setSourceCompression(Compression.BZIP2)
        .setDestinationTableSpec("test_table")
        .build();

    TestPipeline testPipeline = TestPipeline.create();
    testPipeline.getOptions().as(AwsOptions.class).setAwsRegion("us-east-1");
    testPipeline
        .apply(Create.of("s3://bucket/path/prefix-"))
        .apply(ParDo.of(copyFn));
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
