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
package org.apache.beam.sdk.io.kafka.file.aware.factories;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class FileAwareFactoryFnTest {

  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();

  private TestFactoryFn factory;
  private String baseDir;
  private static final String TEST_FACTORY_TYPE = "test-factory";

  // A concrete implementation for testing the abstract FileAwareFactoryFn
  static class TestFactoryFn extends FileAwareFactoryFn<Object> {
    public TestFactoryFn() {
      super(TEST_FACTORY_TYPE);
    }

    @Override
    protected Object createObject(Map<String, Object> config) {
      // Return the processed config for easy assertion
      return config;
    }
  }

  @Before
  public void setup() throws IOException {
    baseDir = "/tmp/" + TEST_FACTORY_TYPE;
    factory = Mockito.spy(new TestFactoryFn());
    Mockito.doReturn(baseDir).when(factory).getBaseDirectory();
  }

  @Test
  public void testHappyPathReplacesGcsPath() {
    // Arrange
    String gcsPath = "gs://test-bucket/config-file.json";
    String expectedLocalPath =
        FileAwareFactoryFn.DIRECTORY_PREFIX
            + "/"
            + TEST_FACTORY_TYPE
            + "/test-bucket/config-file.json";
    Map<String, Object> config = new HashMap<>();
    config.put("config.file.path", gcsPath);

    // Act & Assert
    // Use try-with-resources to manage the scope of the static mock on FileSystems
    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class)) {
      // 1. Mock the underlying static FileSystems calls to avoid real network I/O
      MatchResult.Metadata metadata = Mockito.mock(MatchResult.Metadata.class);
      ResourceId resourceId = Mockito.mock(ResourceId.class);
      Mockito.when(metadata.resourceId()).thenReturn(resourceId);
      mockedFileSystems.when(() -> FileSystems.matchSingleFileSpec(gcsPath)).thenReturn(metadata);

      // 2. Mock 'open' to return a channel with no data, simulating a successful download
      ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(new byte[0]));
      mockedFileSystems.when(() -> FileSystems.open(resourceId)).thenReturn(channel);

      // Act
      Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);

      // Assert
      Assert.assertEquals(expectedLocalPath, processedConfig.get("config.file.path"));
      Assert.assertTrue(
          "Local file should have been created", new File(expectedLocalPath).exists());
    }
  }

  @Test
  public void testApplyFailurePathThrowsRuntimeExceptionOnDownloadFailure() {
    // Arrange
    String gcsPath = "gs://test-bucket/failing-file.txt";
    Map<String, Object> config = new HashMap<>();
    config.put("critical.file", gcsPath);

    // Mock the static FileSystems.matchSingleFileSpec to throw an exception
    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class)) {
      mockedFileSystems
          .when(() -> FileSystems.matchSingleFileSpec(gcsPath))
          .thenThrow(new IOException("GCS file not found"));

      // Act & Assert
      RuntimeException exception =
          Assert.assertThrows(RuntimeException.class, () -> factory.apply(config));
      Assert.assertTrue(exception.getMessage().contains("Failed trying to process value"));
      Assert.assertTrue(exception.getCause() instanceof IOException);
      Assert.assertTrue(exception.getCause().getMessage().contains("Failed to download file"));
    }
  }

  @Test
  public void testApplyHappyPathIgnoresNonGcsValues() {
    // Arrange
    Map<String, Object> config = new HashMap<>();
    config.put("some.string", "/local/path/file.txt");
    config.put("some.number", 42);
    config.put("some.boolean", false);

    // Act
    Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);

    // Assert
    Assert.assertEquals(config, processedConfig);
  }

  @Test
  public void testApplyEdgeCaseMultipleGcsPathsInSingleValue() {
    // Arrange
    String gcsPath1 = "gs://bucket/keytab.keytab";
    String gcsPath2 = "gs://bucket/trust.jks";
    String originalValue =
        "jaas_config keyTab=\"" + gcsPath1 + "\" trustStore=\"" + gcsPath2 + "\"";

    String expectedLocalPath1 =
        FileAwareFactoryFn.DIRECTORY_PREFIX + "/" + TEST_FACTORY_TYPE + "/bucket/keytab.keytab";
    String expectedLocalPath2 =
        FileAwareFactoryFn.DIRECTORY_PREFIX + "/" + TEST_FACTORY_TYPE + "/bucket/trust.jks";
    String expectedProcessedValue =
        "jaas_config keyTab=\""
            + expectedLocalPath1
            + "\" trustStore=\""
            + expectedLocalPath2
            + "\"";

    Map<String, Object> config = new HashMap<>();
    config.put("jaas.config", originalValue);

    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class)) {
      // Mock GCS calls for both paths
      mockSuccessfulDownload(mockedFileSystems, gcsPath1);
      mockSuccessfulDownload(mockedFileSystems, gcsPath2);

      // Act
      Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);

      // Assert
      Assert.assertEquals(expectedProcessedValue, processedConfig.get("jaas.config"));
    }
  }

  @Test
  public void testApplyEdgeCaseLocalFileWriteFails() throws IOException {
    // Arrange
    String gcsPath = "gs://test-bucket/some-file.txt";
    Map<String, Object> config = new HashMap<>();
    config.put("a.file", gcsPath);

    // Mock GCS part to succeed
    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class);
        MockedStatic<FileChannel> mockedFileChannel = Mockito.mockStatic(FileChannel.class)) {
      mockSuccessfulDownload(mockedFileSystems, gcsPath);

      // Mock the local file writing part to fail
      mockedFileChannel
          .when(
              () ->
                  FileChannel.open(
                      ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Set.class)))
          .thenThrow(new IOException("Permission denied"));

      // Act & Assert
      RuntimeException exception =
          Assert.assertThrows(RuntimeException.class, () -> factory.apply(config));
      Assert.assertTrue(exception.getMessage().contains("Failed trying to process value"));
      Assert.assertTrue(exception.getCause() instanceof IOException);
      // Check that the root cause is our "Permission denied" mock
      Assert.assertTrue(exception.getCause().getCause().getMessage().contains("Permission denied"));
    }
  }

  @Test
  public void testApplyHappyPathResolvesSecretValue() {
    // Arrange
    String secretVersion = "secretValue:projects/p/secrets/s/versions/v";
    String secretVersionParsed = "projects/p/secrets/s/versions/v";
    String secretValue = "my-secret-password";
    String originalValue = "password=" + secretVersion;
    String expectedProcessedValue = "password=" + secretValue;

    Map<String, Object> config = new HashMap<>();
    config.put("db.password", originalValue);

    TestFactoryFn factoryWithMockedSecret =
        new TestFactoryFn() {
          @Override
          public byte[] getSecret(String secretIdentifier) {
            // Assert that the correct identifier is passed
            Assert.assertEquals(secretVersionParsed, secretIdentifier);
            // Return a predictable, hardcoded value for the test
            return secretValue.getBytes(StandardCharsets.UTF_8);
          }
        };

    // Act
    @SuppressWarnings("unchecked")
    Map<String, Object> processedConfig =
        (Map<String, Object>) factoryWithMockedSecret.apply(config);

    // Assert
    Assert.assertEquals(expectedProcessedValue, processedConfig.get("db.password"));
  }

  @Test
  public void testApplyFailurePathThrowsExceptionForInvalidSecretFormat() {
    // Arrange
    String invalidSecret = "secretValue:not-a-valid-secret-path";
    Map<String, Object> config = new HashMap<>();
    config.put("db.password", "password=" + invalidSecret);

    // Act & Assert
    RuntimeException ex = Assert.assertThrows(RuntimeException.class, () -> factory.apply(config));
    Assert.assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
  }

  // Helper method to reduce boilerplate in mocking successful GCS downloads
  private void mockSuccessfulDownload(MockedStatic<FileSystems> mockedFileSystems, String gcsPath) {
    MatchResult.Metadata metadata = Mockito.mock(MatchResult.Metadata.class);
    ResourceId resourceId = Mockito.mock(ResourceId.class);
    Mockito.when(metadata.resourceId()).thenReturn(resourceId);
    mockedFileSystems
        .when(() -> FileSystems.matchSingleFileSpec(ArgumentMatchers.eq(gcsPath)))
        .thenReturn(metadata);

    ReadableByteChannel channel = Channels.newChannel(new ByteArrayInputStream(new byte[0]));
    mockedFileSystems
        .when(() -> FileSystems.open(ArgumentMatchers.eq(resourceId)))
        .thenReturn(channel);
  }
}
