package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

public interface JdbcTestOptions extends TestPipelineOptions {
    @Description("IP for postgres server")
    @Default.String("postgres-ip")
    String getPostgresIp();
    void setPostgresIp(String value);

    @Description("Username for postgres server")
    @Default.String("postgres-username")
    String getPostgresUsername();
    void setPostgresUsername(String value);

    @Description("Password for postgres server")
    @Default.String("postgres-password")
    String getPostgresPassword();
    void setPostgresPassword(String value);
}
