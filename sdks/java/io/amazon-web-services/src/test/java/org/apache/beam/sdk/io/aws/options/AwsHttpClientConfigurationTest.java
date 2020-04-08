package org.apache.beam.sdk.io.aws.options;

import static org.junit.Assert.assertEquals;

import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

/** {@link AwsHttpClientConfigurationTest}.
 * Test to verify that aws http client configuration are
 * correctly being set for the respective AWS services.
 */
@RunWith(JUnit4.class)
public class AwsHttpClientConfigurationTest {

    @Test
    public void testAwsHttpClientConfigurationValues() {
        S3Options s3Options = getOptions();
        assertEquals(5000, s3Options.getClientConfiguration().getSocketTimeout());
        assertEquals(1000, s3Options.getClientConfiguration().getClientExecutionTimeout());
        assertEquals(10, s3Options.getClientConfiguration().getMaxConnections());
    }

    private static S3Options getOptions() {
        String[] args = {
                "--s3ClientFactoryClass=org.apache.beam.sdk.io.aws.s3.DefaultS3ClientBuilderFactory",
                "--clientConfiguration={\"clientExecutionTimeout\":\"1000\"," +
                        "\"maxConnections\":\"10\"," +
                        "\"socketTimeout\":\"5000\"}"
        };
        return PipelineOptionsFactory.fromArgs(args).as(S3Options.class);
    }
}
