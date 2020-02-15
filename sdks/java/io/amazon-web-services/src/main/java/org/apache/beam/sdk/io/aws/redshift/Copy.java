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

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.auth.AWSCredentials;
import com.google.auto.value.AutoValue;
import java.io.IOException;
import java.io.Serializable;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Strings;

/**
 * Implements the Redshift SQL COPY command as a {@link DoFn}. Copies the contents of files
 * identified by a S3 URIs, or S3 URI prefixes, to a Redshift table.
 */
@AutoValue
public abstract class Copy extends DoFn<String, Void> implements Serializable {

  private AWSCredentials awsCredentials;

  abstract Redshift.DataSourceConfiguration getDataSourceConfiguration();

  abstract String getDestinationTableSpec();

  abstract String getIAMRole();

  abstract String getOptions();

  public static Builder builder() {
    return new AutoValue_Copy.Builder();
  }

  /** Builder for {@link Copy}. */
  @AutoValue.Builder
  public abstract static class Builder {

    /** Sets the data source configuration. */
    public abstract Builder setDataSourceConfiguration(Redshift.DataSourceConfiguration value);

    /** Sets the destination table name, and optional column list. */
    public abstract Builder setDestinationTableSpec(String value);

    public abstract Builder setIAMRole(String iamRole);

    public abstract Builder setOptions(String options);

    abstract Copy autoBuild();

    /** Builds a {@link Copy} instance. */
    public Copy build() {
      Copy copy = autoBuild();

      checkArgument(
          !Strings.isNullOrEmpty(copy.getDestinationTableSpec()), "destination table spec");
      checkArgument(
          !Strings.isNullOrEmpty(copy.getOptions()),"at least delimiter is specified");

      return copy;
    }
  }

  @StartBundle
  public void startBundle(StartBundleContext context) {
    if (awsCredentials == null) {
      awsCredentials =
          context
              .getPipelineOptions()
              .as(AwsOptions.class)
              .getAwsCredentialsProvider()
              .getCredentials();
    }
  }

  private static final String COPY_STATEMENT_FORMAT =
      "COPY %s FROM " //table
          + "'%s' " //s3 prefix
          + "%s " //credentials
          + "%s "; //options e.g. delimiter

  @ProcessElement
  public void copy(ProcessContext context) throws IOException {
    String sourcePathPrefix = context.element();

    String statementSql =
        String.format(
            COPY_STATEMENT_FORMAT,
            getDestinationTableSpec(),
            sourcePathPrefix,
            this.getCredentials(),
            getOptions());

    try (Connection connection = getDataSourceConfiguration().buildDataSource().getConnection();
        Statement statement = connection.createStatement()) {
      statement.execute(statementSql);
    } catch (SQLException e) {
      throw new IOException("Redshift COPY failed", e);
    }

    context.output(null);
  }

  private String getCredentials() {
    if (!Strings.isNullOrEmpty(getIAMRole())) {
      return String.format("iam_role '%s'" , getIAMRole());
    } else {
      return String.format(
          "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s'",
          awsCredentials.getAWSAccessKeyId(),
          awsCredentials.getAWSSecretKey());
    }
  }
}
