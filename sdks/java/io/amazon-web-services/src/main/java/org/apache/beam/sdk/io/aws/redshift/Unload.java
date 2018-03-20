/*
 * Copyright 2017 Kochava, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
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

import static com.google.common.base.Preconditions.checkArgument;

import com.amazonaws.auth.AWSCredentials;
import com.google.auto.value.AutoValue;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import org.apache.beam.sdk.io.Compression;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.aws.options.AwsOptions;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.transforms.DoFn;

/**
 * Implements the Redshift SQL UNLOAD command as a {@link DoFn}. Unloads the results of a SQL query
 * to files indicated by an S3 URI prefix.
 */
@AutoValue
public abstract class Unload extends DoFn<Void, String> {

  private AWSCredentials awsCredentials;

  abstract Redshift.DataSourceConfiguration getDataSourceConfiguration();
  abstract char getDelimiter();
  abstract Compression getDestinationCompression();
  abstract String getSourceQuery();
  abstract String getDestination();

  public static Builder builder() {
    return new AutoValue_Unload.Builder()
        .setDestinationCompression(Compression.UNCOMPRESSED);
  }

  /**
   * Builder for {@link Unload}.
   */
  @AutoValue.Builder
  public abstract static class Builder {

    /**
     * Sets the data source configuration.
     */
    public abstract Builder setDataSourceConfiguration(Redshift.DataSourceConfiguration value);

    /**
     * Sets the delimiter used in the destination CSV files.
     */
    public abstract Builder setDelimiter(char value);

    /**
     * Sets the compression used when writing the destination files; {@link
     * Compression#UNCOMPRESSED} by default.
     */
    public abstract Builder setDestinationCompression(Compression value);

    /**
     * Sets the source query used to generate the destination files.
     */
    public abstract Builder setSourceQuery(String value);

    /**
     * Sets the destination S3 filename prefix.
     */
    public abstract Builder setDestination(String value);

    abstract Unload autoBuild();

    /**
     * Builds an {@link Unload} object.
     */
    public Unload build() {
      Unload unload = autoBuild();

      checkArgument(!Character.isISOControl(unload.getDelimiter()), "delimiter");
      checkArgument(!Strings.isNullOrEmpty(unload.getSourceQuery()), "source query");
      checkArgument(
          ImmutableList.of(Compression.UNCOMPRESSED, Compression.BZIP2, Compression.GZIP)
              .contains(unload.getDestinationCompression()));

      return unload;
    }
  }

  @StartBundle
  public void startBundle(StartBundleContext context) {
    if (awsCredentials == null) {
      awsCredentials = context.getPipelineOptions().as(AwsOptions.class)
          .getAwsCredentialsProvider().getCredentials();
    }
  }

  private static final String UNLOAD_STATEMENT_FORMAT =
      "UNLOAD ('%s') TO '%s' "
          + "CREDENTIALS 'aws_access_key_id=%s;aws_secret_access_key=%s' "
          + "DELIMITER AS '%s' "
          + "%s " // compression
          + "NULL AS 'NULL' "
          + "ESCAPE "
          + "PARALLEL ON";

  @ProcessElement
  public void processElement(ProcessContext context) throws IOException {
    // Check that no objects already exist with this S3 prefix
    MatchResult existingFiles =
        FileSystems.match(getDestination() + "**", EmptyMatchTreatment.ALLOW);
    if (!existingFiles.metadata().isEmpty()) {
      // metadata() throws IOException if Filesystems.match() runs into other error.
      throw new IOException("files already exist at the S3 destination");
    }

    String statementSql = String.format(UNLOAD_STATEMENT_FORMAT,
        getSourceQuery(),
        getDestination(),
        awsCredentials.getAWSAccessKeyId(),
        awsCredentials.getAWSSecretKey(),
        getDelimiter(),
        getDestinationCompression() == Compression.UNCOMPRESSED
            ? ""
            : getDestinationCompression().name());

    try (Connection connection = getDataSourceConfiguration().buildDataSource().getConnection()) {
      try (Statement statement = connection.createStatement()) {
        statement.execute(statementSql);
      }
    } catch (SQLException e) {
      throw new IOException("Redshift UNLOAD failed", e);
    }

    // Get written S3 objects
    MatchResult createdFiles = FileSystems.match(getDestination() + "**");
    if (createdFiles.status() != MatchResult.Status.OK) {
      throw new IOException(
          String.format("check for already-existing files returned %s",
              createdFiles.status().toString()));
    }
    for (MatchResult.Metadata metadata : createdFiles.metadata()) {
      context.output(metadata.resourceId().toString());
    }
  }
}
