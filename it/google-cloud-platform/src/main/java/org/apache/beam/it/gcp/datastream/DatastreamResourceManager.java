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
package org.apache.beam.it.gcp.datastream;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.api.gax.core.CredentialsProvider;
import com.google.cloud.datastream.v1.AvroFileFormat;
import com.google.cloud.datastream.v1.BigQueryDestinationConfig;
import com.google.cloud.datastream.v1.BigQueryProfile;
import com.google.cloud.datastream.v1.ConnectionProfile;
import com.google.cloud.datastream.v1.ConnectionProfileName;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DatastreamSettings;
import com.google.cloud.datastream.v1.DeleteConnectionProfileRequest;
import com.google.cloud.datastream.v1.DeleteStreamRequest;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.GcsDestinationConfig;
import com.google.cloud.datastream.v1.GcsProfile;
import com.google.cloud.datastream.v1.JsonFileFormat;
import com.google.cloud.datastream.v1.LocationName;
import com.google.cloud.datastream.v1.MysqlProfile;
import com.google.cloud.datastream.v1.MysqlSourceConfig;
import com.google.cloud.datastream.v1.OracleProfile;
import com.google.cloud.datastream.v1.OracleSourceConfig;
import com.google.cloud.datastream.v1.PostgresqlProfile;
import com.google.cloud.datastream.v1.PostgresqlSourceConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.StaticServiceIpConnectivity;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.datastream.v1.StreamName;
import com.google.cloud.datastream.v1.UpdateStreamRequest;
import com.google.protobuf.Duration;
import com.google.protobuf.FieldMask;
import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import org.apache.beam.it.common.ResourceManager;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.annotations.VisibleForTesting;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Client for Datastream resources.
 *
 * <p>This class is thread safe.
 */
public final class DatastreamResourceManager implements ResourceManager {
  private static final Logger LOG = LoggerFactory.getLogger(DatastreamResourceManager.class);
  private static final String FIELD_STATE = "state";
  private final DatastreamClient datastreamClient;
  private final String location;
  private final String projectId;
  private final Set<String> createdStreamIds;
  private final Set<String> createdConnectionProfileIds;

  enum DestinationOutputFormat {
    AVRO_FILE_FORMAT,
    JSON_FILE_FORMAT
  }

  private DatastreamResourceManager(Builder builder) throws IOException {
    this(
        DatastreamClient.create(
            DatastreamSettings.newBuilder()
                .setCredentialsProvider(builder.credentialsProvider)
                .build()),
        builder);
  }

  @VisibleForTesting
  public DatastreamResourceManager(DatastreamClient datastreamClient, Builder builder) {
    this.datastreamClient = datastreamClient;
    this.location = builder.location;
    this.projectId = builder.projectId;
    this.createdStreamIds = Collections.synchronizedSet(new HashSet<>());
    this.createdConnectionProfileIds = Collections.synchronizedSet(new HashSet<>());
  }

  public static Builder builder(
      String projectId, String location, CredentialsProvider credentialsProvider) {
    checkArgument(!Strings.isNullOrEmpty(projectId), "projectID can not be null or empty");
    checkArgument(!Strings.isNullOrEmpty(location), "location can not be null or empty");
    return new Builder(projectId, location, credentialsProvider);
  }

  /**
   * @param connectionProfileId The ID of the connection profile.
   * @param source An object representing the JDBC source.
   * @return A Datastream JDBC source connection profile.
   */
  private synchronized ConnectionProfile createJDBCSourceConnectionProfile(
      String connectionProfileId, JDBCSource source) {
    checkArgument(
        !Strings.isNullOrEmpty(connectionProfileId),
        "connectionProfileId can not be null or empty");
    LOG.info(
        "Creating JDBC Source Connection Profile {} in project {}.",
        connectionProfileId,
        projectId);
    ConnectionProfile.Builder connectionProfileBuilder = ConnectionProfile.newBuilder();
    JDBCSource.SourceType type = source.type();

    try {
      switch (type) {
        case MYSQL:
          MysqlProfile.Builder mysqlProfileBuilder = MysqlProfile.newBuilder();
          mysqlProfileBuilder
              .setHostname(source.hostname())
              .setUsername(source.username())
              .setPassword(source.password())
              .setPort(source.port());
          connectionProfileBuilder.setMysqlProfile(mysqlProfileBuilder);
          break;
        case ORACLE:
          OracleProfile.Builder oracleProfileBuilder = OracleProfile.newBuilder();
          oracleProfileBuilder
              .setHostname(source.hostname())
              .setUsername(source.username())
              .setPassword(source.password())
              .setPort(source.port());
          connectionProfileBuilder.setOracleProfile(oracleProfileBuilder);
          break;
        case POSTGRESQL:
          PostgresqlProfile.Builder postgresqlProfileBuilder = PostgresqlProfile.newBuilder();
          postgresqlProfileBuilder
              .setHostname(source.hostname())
              .setUsername(source.username())
              .setPassword(source.password())
              .setPort(source.port())
              .setDatabase(((PostgresqlSource) source).database());
          connectionProfileBuilder.setPostgresqlProfile(postgresqlProfileBuilder);
          break;
        default:
          throw new DatastreamResourceManagerException(
              "Could not recognize JDBC source type " + type.name());
      }

      ConnectionProfile connectionProfile =
          connectionProfileBuilder
              .setDisplayName(connectionProfileId)
              .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance())
              .build();
      CreateConnectionProfileRequest request =
          CreateConnectionProfileRequest.newBuilder()
              .setParent(LocationName.of(projectId, location).toString())
              .setConnectionProfile(connectionProfile)
              .setConnectionProfileId(connectionProfileId)
              .build();
      ConnectionProfile reference = datastreamClient.createConnectionProfileAsync(request).get();
      createdConnectionProfileIds.add(connectionProfileId);

      LOG.info(
          "Successfully created JDBC Source Connection Profile {} in project {}.",
          connectionProfileId,
          projectId);
      return reference;
    } catch (ExecutionException | InterruptedException e) {
      throw new DatastreamResourceManagerException(
          "Failed to create JDBC source connection profile. ", e);
    }
  }

  /**
   * @param sourceConnectionProfileId The ID of the connection profile.
   * @param source An object representing the JDBC source.
   * @return a SourceConfig object which is required to configure a Stream.
   */
  public synchronized SourceConfig buildSourceConfig(
      String sourceConnectionProfileId, JDBCSource source) {

    createJDBCSourceConnectionProfile(sourceConnectionProfileId, source);
    SourceConfig.Builder sourceConfigBuilder =
        SourceConfig.newBuilder()
            .setSourceConnectionProfile(
                ConnectionProfileName.format(projectId, location, sourceConnectionProfileId));

    switch (source.type()) {
      case MYSQL:
        sourceConfigBuilder.setMysqlSourceConfig((MysqlSourceConfig) source.config());
        break;
      case POSTGRESQL:
        sourceConfigBuilder.setPostgresqlSourceConfig((PostgresqlSourceConfig) source.config());
        break;
      case ORACLE:
        sourceConfigBuilder.setOracleSourceConfig((OracleSourceConfig) source.config());
        break;
      default:
        throw new DatastreamResourceManagerException(
            "Could not recognize JDBC source type " + source.type().name());
    }

    return sourceConfigBuilder.build();
  }

  /**
   * @param connectionProfileId The ID of the GCS connection profile.
   * @param gcsBucketName The GCS Bucket to connect to.
   * @param gcsRootPath The Path prefix to specific gcs location. Can either be empty or must start
   *     with '/'.
   * @return A Datastream GCS destination connection profile.
   */
  public synchronized ConnectionProfile createGCSDestinationConnectionProfile(
      String connectionProfileId, String gcsBucketName, String gcsRootPath) {
    checkArgument(
        !Strings.isNullOrEmpty(connectionProfileId),
        "connectionProfileId can not be null or empty");
    checkArgument(!Strings.isNullOrEmpty(gcsBucketName), "gcsBucketName can not be null or empty");
    checkArgument(gcsRootPath != null, "gcsRootPath can not be null");
    checkArgument(
        gcsRootPath.isEmpty() || gcsRootPath.charAt(0) == '/',
        "gcsRootPath must either be an empty string or start with a '/'");

    LOG.info(
        "Creating GCS Destination Connection Profile {} in project {}.",
        connectionProfileId,
        projectId);

    try {
      ConnectionProfile.Builder connectionProfileBuilder =
          ConnectionProfile.newBuilder()
              .setDisplayName(connectionProfileId)
              .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance())
              .setGcsProfile(
                  GcsProfile.newBuilder().setBucket(gcsBucketName).setRootPath(gcsRootPath));

      CreateConnectionProfileRequest request =
          CreateConnectionProfileRequest.newBuilder()
              .setParent(LocationName.of(projectId, location).toString())
              .setConnectionProfile(connectionProfileBuilder)
              .setConnectionProfileId(connectionProfileId)
              .build();

      ConnectionProfile reference = datastreamClient.createConnectionProfileAsync(request).get();
      createdConnectionProfileIds.add(connectionProfileId);
      LOG.info(
          "Successfully created GCS Destination Connection Profile {} in project {}.",
          connectionProfileId,
          projectId);

      return reference;
    } catch (ExecutionException | InterruptedException e) {
      throw new DatastreamResourceManagerException(
          "Failed to create GCS source connection profile. ", e);
    }
  }

  /**
   * @param connectionProfileId The ID of the connection profile.
   * @param path The Path prefix to specific GCS location. Can either be empty or must start with
   *     '/'.
   * @param destinationOutputFormat The format of the files written to GCS.
   * @return A DestinationConfig object representing a GCS destination configuration.
   */
  public synchronized DestinationConfig buildGCSDestinationConfig(
      String connectionProfileId, String path, DestinationOutputFormat destinationOutputFormat) {

    DestinationConfig.Builder destinationConfigBuilder =
        DestinationConfig.newBuilder()
            .setDestinationConnectionProfile(
                ConnectionProfileName.format(projectId, location, connectionProfileId));

    GcsDestinationConfig.Builder gcsDestinationConfigBuilder =
        GcsDestinationConfig.newBuilder().setPath(path);

    if (destinationOutputFormat == DestinationOutputFormat.AVRO_FILE_FORMAT) {
      gcsDestinationConfigBuilder.setAvroFileFormat(AvroFileFormat.getDefaultInstance());
    } else {
      gcsDestinationConfigBuilder.setJsonFileFormat(JsonFileFormat.getDefaultInstance());
    }

    destinationConfigBuilder.setGcsDestinationConfig(gcsDestinationConfigBuilder);
    return destinationConfigBuilder.build();
  }

  /**
   * @param connectionProfileId The ID of the connection profile.
   * @return A Datastream BigQuery destination connection profile.
   */
  public synchronized ConnectionProfile createBQDestinationConnectionProfile(
      String connectionProfileId) {

    LOG.info(
        "Creating BQ Destination Connection Profile {} in project {}.",
        connectionProfileId,
        projectId);

    try {
      ConnectionProfile.Builder connectionProfileBuilder =
          ConnectionProfile.newBuilder()
              .setDisplayName(connectionProfileId)
              .setStaticServiceIpConnectivity(StaticServiceIpConnectivity.getDefaultInstance())
              .setBigqueryProfile(BigQueryProfile.newBuilder());

      CreateConnectionProfileRequest request =
          CreateConnectionProfileRequest.newBuilder()
              .setParent(LocationName.of(projectId, location).toString())
              .setConnectionProfile(connectionProfileBuilder)
              .setConnectionProfileId(connectionProfileId)
              .build();

      ConnectionProfile reference = datastreamClient.createConnectionProfileAsync(request).get();
      createdConnectionProfileIds.add(connectionProfileId);

      LOG.info(
          "Successfully created BQ Destination Connection Profile {} in project {}.",
          connectionProfileId,
          projectId);
      return reference;
    } catch (ExecutionException | InterruptedException e) {
      throw new DatastreamResourceManagerException(
          "Failed to create BQ destination connection profile. ", e);
    }
  }

  /**
   * @param connectionProfileId The ID of the connection profile.
   * @param stalenessLimitSeconds The desired data freshness in seconds.
   * @param datasetId The ID of the BigQuery dataset.
   * @return A DestinationConfig object representing a BigQuery destination configuration.
   */
  public synchronized DestinationConfig buildBQDestinationConfig(
      String connectionProfileId, long stalenessLimitSeconds, String datasetId) {

    DestinationConfig.Builder destinationConfigBuilder =
        DestinationConfig.newBuilder()
            .setDestinationConnectionProfile(
                ConnectionProfileName.format(projectId, location, connectionProfileId));

    BigQueryDestinationConfig.Builder bigQueryDestinationConfig =
        BigQueryDestinationConfig.newBuilder()
            .setDataFreshness(Duration.newBuilder().setSeconds(stalenessLimitSeconds).build())
            .setSingleTargetDataset(
                BigQueryDestinationConfig.SingleTargetDataset.newBuilder().setDatasetId(datasetId));

    destinationConfigBuilder.setBigqueryDestinationConfig(bigQueryDestinationConfig);
    return destinationConfigBuilder.build();
  }

  /**
   * @param streamId The ID of the stream.
   * @param sourceConfig A SourceConfig object representing the source configuration.
   * @param destinationConfig A DestinationConfig object representing the destination configuration.
   * @return A Datastream stream object.
   */
  public synchronized Stream createStream(
      String streamId, SourceConfig sourceConfig, DestinationConfig destinationConfig) {

    LOG.info("Creating Stream {} in project {}.", streamId, projectId);

    try {
      CreateStreamRequest request =
          CreateStreamRequest.newBuilder()
              .setParent(LocationName.format(projectId, location))
              .setStreamId(streamId)
              .setStream(
                  Stream.newBuilder()
                      .setSourceConfig(sourceConfig)
                      .setDisplayName(streamId)
                      .setDestinationConfig(destinationConfig)
                      .setBackfillAll(Stream.BackfillAllStrategy.getDefaultInstance())
                      .build())
              .build();

      Stream reference = datastreamClient.createStreamAsync(request).get();
      createdStreamIds.add(streamId);

      LOG.info("Successfully created Stream {} in project {}.", streamId, projectId);
      return reference;
    } catch (ExecutionException | InterruptedException e) {
      throw new DatastreamResourceManagerException("Failed to create stream. ", e);
    }
  }

  public synchronized Stream updateStreamState(String streamId, Stream.State state) {

    LOG.info("Updating {}'s state to {} in project {}.", streamId, state.name(), projectId);

    try {
      Stream.Builder streamBuilder =
          Stream.newBuilder()
              .setName(StreamName.format(projectId, location, streamId))
              .setState(state);

      FieldMask.Builder fieldMaskBuilder = FieldMask.newBuilder().addPaths(FIELD_STATE);

      UpdateStreamRequest request =
          UpdateStreamRequest.newBuilder()
              .setStream(streamBuilder)
              .setUpdateMask(fieldMaskBuilder)
              .build();

      Stream reference = datastreamClient.updateStreamAsync(request).get();

      LOG.info(
          "Successfully updated {}'s state to {} in project {}.",
          streamId,
          state.name(),
          projectId);
      return reference;
    } catch (InterruptedException | ExecutionException e) {
      throw new DatastreamResourceManagerException("Failed to update stream. ", e);
    }
  }

  public synchronized Stream startStream(String streamId) {
    LOG.info("Starting Stream {} in project {}.", streamId, projectId);
    return updateStreamState(streamId, Stream.State.RUNNING);
  }

  public synchronized Stream pauseStream(String streamId) {
    LOG.info("Pausing Stream {} in project {}.", streamId, projectId);
    return updateStreamState(streamId, Stream.State.PAUSED);
  }

  @Override
  public synchronized void cleanupAll() {
    LOG.info("Cleaning up Datastream resource manager.");
    boolean producedError = false;

    try {
      for (String stream : createdStreamIds) {
        datastreamClient
            .deleteStreamAsync(
                DeleteStreamRequest.newBuilder()
                    .setName(StreamName.format(projectId, location, stream))
                    .build())
            .get();
      }
      LOG.info("Successfully deleted stream(s). ");
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed to delete stream(s).");
      producedError = true;
    }

    try {
      for (String connectionProfile : createdConnectionProfileIds) {
        datastreamClient
            .deleteConnectionProfileAsync(
                DeleteConnectionProfileRequest.newBuilder()
                    .setName(ConnectionProfileName.format(projectId, location, connectionProfile))
                    .build())
            .get();
      }
      LOG.info("Successfully deleted connection profile(s). ");
    } catch (InterruptedException | ExecutionException e) {
      LOG.error("Failed to delete connection profile(s).");
      producedError = true;
    }

    try {
      datastreamClient.close();
    } catch (Exception e) {
      LOG.error("Failed to close datastream client. ");
      producedError = true;
    }

    if (producedError) {
      throw new DatastreamResourceManagerException(
          "Failed to delete resources. Check above for errors.");
    }

    LOG.info("Successfully cleaned up Datastream resource manager.");
  }

  /** Builder for {@link DatastreamResourceManager}. */
  public static final class Builder {
    private final String projectId;
    private final String location;
    private CredentialsProvider credentialsProvider;

    private Builder(String projectId, String location, CredentialsProvider credentialsProvider) {
      this.projectId = projectId;
      this.location = location;
      this.credentialsProvider = credentialsProvider;
    }

    public Builder setCredentialsProvider(CredentialsProvider credentialsProvider) {
      this.credentialsProvider = credentialsProvider;
      return this;
    }

    public DatastreamResourceManager build() throws IOException {
      return new DatastreamResourceManager(this);
    }
  }
}
