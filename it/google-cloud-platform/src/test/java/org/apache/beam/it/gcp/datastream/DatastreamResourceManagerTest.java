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

import static com.google.common.truth.Truth.assertThat;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.cloud.datastream.v1.ConnectionProfile;
import com.google.cloud.datastream.v1.CreateConnectionProfileRequest;
import com.google.cloud.datastream.v1.CreateStreamRequest;
import com.google.cloud.datastream.v1.DatastreamClient;
import com.google.cloud.datastream.v1.DeleteConnectionProfileRequest;
import com.google.cloud.datastream.v1.DeleteStreamRequest;
import com.google.cloud.datastream.v1.DestinationConfig;
import com.google.cloud.datastream.v1.SourceConfig;
import com.google.cloud.datastream.v1.Stream;
import com.google.cloud.datastream.v1.Stream.State;
import com.google.cloud.datastream.v1.UpdateStreamRequest;
import java.util.concurrent.ExecutionException;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.Answers;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

/** Unit test for {@link DatastreamResourceManager}. */
@RunWith(JUnit4.class)
public class DatastreamResourceManagerTest {
  @Rule public final MockitoRule mockito = MockitoJUnit.rule();
  private static final String CONNECTION_PROFILE_ID = "test-connection-profile-id";
  private static final String STREAM_ID = "test-stream-id";
  private static final String PROJECT_ID = "test-project";
  private static final String LOCATION = "test-location";
  private static final String BUCKET = "test-bucket";
  private static final String ROOT_PATH = "/test-root-path";
  private static final int RESOURCE_COUNT = 5;

  @Mock(answer = Answers.RETURNS_DEEP_STUBS)
  private DatastreamClient datastreamClient;

  private DatastreamResourceManager testManager;

  @Before
  public void setup() {
    testManager =
        new DatastreamResourceManager(
            datastreamClient, DatastreamResourceManager.builder(PROJECT_ID, LOCATION));
  }

  @Test
  public void testBuilderWithInvalidProjectShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class, () -> DatastreamResourceManager.builder("", LOCATION));
    assertThat(exception).hasMessageThat().contains("projectID can not be null or empty");
  }

  @Test
  public void testBuilderWithInvalidLocationShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () -> DatastreamResourceManager.builder(PROJECT_ID, ""));
    assertThat(exception).hasMessageThat().contains("location can not be null or empty");
  }

  @Test
  public void testCreateBQDestinationConnectionProfileExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient
            .createConnectionProfileAsync(any(CreateConnectionProfileRequest.class))
            .get())
        .thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createBQDestinationConnectionProfile(CONNECTION_PROFILE_ID));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create BQ destination connection profile.");
  }

  @Test
  public void testCreateBQDestinationConnectionInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient
            .createConnectionProfileAsync(any(CreateConnectionProfileRequest.class))
            .get())
        .thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.createBQDestinationConnectionProfile(CONNECTION_PROFILE_ID));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create BQ destination connection profile.");
  }

  @Test
  public void testCreateBQDestinationConnectionShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    ConnectionProfile connectionProfile = ConnectionProfile.getDefaultInstance();
    when(datastreamClient
            .createConnectionProfileAsync(any(CreateConnectionProfileRequest.class))
            .get())
        .thenReturn(connectionProfile);
    assertThat(testManager.createBQDestinationConnectionProfile(CONNECTION_PROFILE_ID))
        .isEqualTo(connectionProfile);
  }

  @Test
  public void testCreateGCSDestinationConnectionProfileWithInvalidGCSRootPathShouldFail() {
    IllegalArgumentException exception =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                testManager.createGCSDestinationConnectionProfile(
                    CONNECTION_PROFILE_ID, BUCKET, "invalid"));
    assertThat(exception)
        .hasMessageThat()
        .contains("gcsRootPath must either be an empty string or start with a '/'");
  }

  @Test
  public void testCreateGCSDestinationConnectionProfileExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient
            .createConnectionProfileAsync(any(CreateConnectionProfileRequest.class))
            .get())
        .thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () ->
                testManager.createGCSDestinationConnectionProfile(
                    CONNECTION_PROFILE_ID, BUCKET, ROOT_PATH));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create GCS source connection profile.");
  }

  @Test
  public void testCreateGCSDestinationConnectionProfileInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient
            .createConnectionProfileAsync(any(CreateConnectionProfileRequest.class))
            .get())
        .thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () ->
                testManager.createGCSDestinationConnectionProfile(
                    CONNECTION_PROFILE_ID, BUCKET, ROOT_PATH));
    assertThat(exception)
        .hasMessageThat()
        .contains("Failed to create GCS source connection profile.");
  }

  @Test
  public void testCreateGCSDestinationConnectionShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    ConnectionProfile connectionProfile = ConnectionProfile.getDefaultInstance();
    when(datastreamClient
            .createConnectionProfileAsync(any(CreateConnectionProfileRequest.class))
            .get())
        .thenReturn(connectionProfile);
    assertThat(
            testManager.createGCSDestinationConnectionProfile(
                CONNECTION_PROFILE_ID, BUCKET, ROOT_PATH))
        .isEqualTo(connectionProfile);
  }

  @Test
  public void testCreateStreamExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)).get())
        .thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () ->
                testManager.createStream(
                    STREAM_ID,
                    SourceConfig.getDefaultInstance(),
                    DestinationConfig.getDefaultInstance()));
    assertThat(exception).hasMessageThat().contains("Failed to create stream.");
  }

  @Test
  public void testCreateStreamInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)).get())
        .thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () ->
                testManager.createStream(
                    STREAM_ID,
                    SourceConfig.getDefaultInstance(),
                    DestinationConfig.getDefaultInstance()));
    assertThat(exception).hasMessageThat().contains("Failed to create stream.");
  }

  @Test
  public void testCreateStreamShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    Stream stream = Stream.getDefaultInstance();
    when(datastreamClient.createStreamAsync(any(CreateStreamRequest.class)).get())
        .thenReturn(stream);
    assertThat(
            testManager.createStream(
                STREAM_ID,
                SourceConfig.getDefaultInstance(),
                DestinationConfig.getDefaultInstance()))
        .isEqualTo(stream);
  }

  @Test
  public void testUpdateStreamStateInterruptedExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.updateStreamAsync(any(UpdateStreamRequest.class)).get())
        .thenThrow(InterruptedException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.updateStreamState(STREAM_ID, State.RUNNING));
    assertThat(exception).hasMessageThat().contains("Failed to update stream.");
  }

  @Test
  public void testUpdateStreamStateExecutionExceptionShouldFail()
      throws ExecutionException, InterruptedException {
    when(datastreamClient.updateStreamAsync(any(UpdateStreamRequest.class)).get())
        .thenThrow(ExecutionException.class);
    DatastreamResourceManagerException exception =
        assertThrows(
            DatastreamResourceManagerException.class,
            () -> testManager.updateStreamState(STREAM_ID, State.RUNNING));
    assertThat(exception).hasMessageThat().contains("Failed to update stream.");
  }

  @Test
  public void testUpdateStreamStateShouldCreateSuccessfully()
      throws ExecutionException, InterruptedException {
    Stream stream = Stream.getDefaultInstance();
    when(datastreamClient.updateStreamAsync(any(UpdateStreamRequest.class)).get())
        .thenReturn(stream);
    assertThat(testManager.updateStreamState(STREAM_ID, State.RUNNING)).isEqualTo(stream);
  }

  @Test
  public void testCleanupAllShouldDeleteSuccessfullyWhenNoErrorIsThrown() {
    doNothing().when(datastreamClient).close();

    for (int i = 0; i < RESOURCE_COUNT; i++) {
      testManager.createGCSDestinationConnectionProfile(
          "gcs-" + CONNECTION_PROFILE_ID + i, BUCKET, ROOT_PATH);
      testManager.createBQDestinationConnectionProfile("bq-" + CONNECTION_PROFILE_ID + i);
      testManager.createStream(
          STREAM_ID + i, SourceConfig.getDefaultInstance(), DestinationConfig.getDefaultInstance());
    }

    testManager.cleanupAll();
    verify(datastreamClient, times(RESOURCE_COUNT))
        .deleteStreamAsync(any(DeleteStreamRequest.class));
    verify(datastreamClient, times(RESOURCE_COUNT * 2))
        .deleteConnectionProfileAsync(any(DeleteConnectionProfileRequest.class));
    verify(datastreamClient).close();
  }
}
