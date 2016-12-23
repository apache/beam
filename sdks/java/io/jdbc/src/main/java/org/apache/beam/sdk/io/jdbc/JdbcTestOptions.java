package org.apache.beam.sdk.io.jdbc;

import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.testing.TestPipelineOptions;

/**
 * TODO - non horrible javadoc.
 */
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

    @Description("Database name for postgres server")
    @Default.String("postgres-database-name")
    String getPostgresDatabaseName();
    void setPostgresDatabaseName(String value);

    @Description("Port for postgres server")
    @Default.Integer(0)
    Integer getPostgresPort();
    void setPostgresPort(Integer value);

    @Description("Whether the postgres server uses SSL")
    @Default.Boolean(true)
    Boolean getPostgresSsl();
    void setPostgresSsl(Boolean value);
}
