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
package org.apache.beam.sdk.extensions.kafka.factories;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class FileAwareFactoryFnTest {

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
    factory = new TestFactoryFn();

    // --- THIS IS THE FIX ---
    // Calculate the hard-coded base directory path that the *production code*
    // (replacePathWithLocal) actually uses.
    baseDir = FileAwareFactoryFn.DIRECTORY_PREFIX + "/" + TEST_FACTORY_TYPE;

    // Ensure the directory is clean *before* the test, in case a previous
    // tearDown failed.
    Path path = Paths.get(baseDir);
    if (Files.exists(path)) {
      deleteDirectoryRecursively(path);
    }
    // ---------------------

    // Ensure this directory exists before the test runs.
    Files.createDirectories(Paths.get(baseDir));
  }

  /**
   * This method runs after each test and manually cleans up the hard-coded directory, simulating
   * the behavior of TemporaryFolder.
   */
  @After
  public void tearDown() throws IOException {
    Path path = Paths.get(baseDir);
    if (Files.exists(path)) {
      deleteDirectoryRecursively(path);
    }
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testHappyPathReplacesGcsPath() throws IOException {
    // Arrange
    String gcsPath = "gs://test-bucket/config-file.json";
    String expectedLocalPath = baseDir + "/test-bucket/config-file.json";

    Map<String, Object> config = new HashMap<>();
    config.put("config.file.path", gcsPath);

    // Use try-with-resources to mock the static downloadGcsFile method
    // Suppress the "rawtypes" warning, which is a known issue with Mockito's
    // static mocking API when dealing with generics.
    try (MockedStatic<FileAwareFactoryFn> mockedFactory =
        Mockito.mockStatic(FileAwareFactoryFn.class, Mockito.CALLS_REAL_METHODS)) {
      // Mock the downloadGcsFile method to "succeed"
      mockSuccessfulDownload(mockedFactory, gcsPath, expectedLocalPath);

      // Act
      Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);

      // Assert
      Assert.assertEquals(expectedLocalPath, processedConfig.get("config.file.path"));
      // Verify that the download created the file (inside the temp folder)
      Assert.assertTrue(
          "Local file should have been created", Files.exists(Paths.get(expectedLocalPath)));
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

  @SuppressWarnings("rawtypes")
  @Test
  public void testApplyEdgeCaseMultipleGcsPathsInSingleValue() throws IOException {
    // Arrange
    String gcsPath1 = "gs://bucket/keytab.keytab";
    String gcsPath2 = "gs://bucket/trust.jks";
    String originalValue =
        "jaas_config keyTab=\"" + gcsPath1 + "\" trustStore=\"" + gcsPath2 + "\"";

    String expectedLocalPath1 = baseDir + "/bucket/keytab.keytab";
    String expectedLocalPath2 = baseDir + "/bucket/trust.jks";
    String expectedProcessedValue =
        "jaas_config keyTab=\""
            + expectedLocalPath1
            + "\" trustStore=\""
            + expectedLocalPath2
            + "\"";

    Map<String, Object> config = new HashMap<>();
    config.put("jaas.config", originalValue);

    // Use try-with-resources to mock the static downloadGcsFile method
    try (MockedStatic<FileAwareFactoryFn> mockedFactory =
        Mockito.mockStatic(FileAwareFactoryFn.class, Mockito.CALLS_REAL_METHODS)) {
      // Mock GCS calls for both paths
      mockSuccessfulDownload(mockedFactory, gcsPath1, expectedLocalPath1);
      mockSuccessfulDownload(mockedFactory, gcsPath2, expectedLocalPath2);

      // Act
      Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);

      // Assert
      Assert.assertEquals(expectedProcessedValue, processedConfig.get("jaas.config"));
    }
  }

  @SuppressWarnings("rawtypes")
  @Test
  public void testApplyEdgeCaseLocalFileWriteFails() throws IOException {
    // Arrange
    String gcsPath = "gs://test-bucket/some-file.txt";
    Map<String, Object> config = new HashMap<>();
    config.put("a.file", gcsPath);

    // Use try-with-resources to mock the static downloadGcsFile method
    try (MockedStatic<FileAwareFactoryFn> mockedFactory =
        Mockito.mockStatic(FileAwareFactoryFn.class, Mockito.CALLS_REAL_METHODS)) {
      // Mock downloadGcsFile to throw an IOException, simulating a
      // local file write failure (e.g., "Permission denied" when creating directories).
      mockedFactory
          .when(
              () ->
                  FileAwareFactoryFn.downloadGcsFile(
                      ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
          .thenThrow(new IOException("Permission denied"));

      // Act & Assert
      RuntimeException exception =
          Assert.assertThrows(RuntimeException.class, () -> factory.apply(config));
      Assert.assertTrue(exception.getMessage().contains("Failed trying to process value"));
      Assert.assertTrue(exception.getCause() instanceof IOException);
      // Check that the root cause is our "Permission denied" mock,
      // which gets wrapped by the parent 'apply' method's error handling.
      Assert.assertTrue(exception.getCause().getMessage().contains("Failed to download file"));
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

    // We can't spy on a static method (getSecret), so we must override it
    // in an anonymous class for this test.
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

  // Helper method to mock a successful call to downloadGcsFile
  @SuppressWarnings("rawtypes")
  private void mockSuccessfulDownload(
      MockedStatic<FileAwareFactoryFn> mockedFactory, String gcsPath, String expectedLocalPath) {
    mockedFactory
        .when(
            () ->
                FileAwareFactoryFn.downloadGcsFile(
                    ArgumentMatchers.eq(gcsPath), ArgumentMatchers.eq(expectedLocalPath)))
        .thenAnswer(
            invocation -> {
              String localPathStr = invocation.getArgument(1);
              Path path = Paths.get(localPathStr);
              // Simulate the behavior of the real method (which now creates dirs)
              Files.createDirectories(path.getParent());
              Files.createFile(path);
              // THIS IS THE FIX: Return the non-null local path string
              return localPathStr;
            });
  }

  /**
   * Helper method to recursively delete a directory. This is used in the @After method to clean up
   * test resources.
   */
  private void deleteDirectoryRecursively(Path path) throws IOException {
    try (Stream<Path> walk = Files.walk(path)) {
      walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
    }
  }
}

/// *
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// *
// *     http://www.apache.org/licenses/LICENSE-2.0
// *
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
// package org.apache.beam.sdk.extensions.kafka.factories;
//
// import java.io.ByteArrayInputStream;
// import java.io.File;
// import java.io.IOException;
// import java.nio.channels.Channels;
// import java.nio.channels.FileChannel;
// import java.nio.channels.ReadableByteChannel;
// import java.nio.charset.StandardCharsets;
// import java.nio.file.Files;
// import java.nio.file.Path;
// import java.nio.file.Paths;
// import java.util.HashMap;
// import java.util.Map;
// import java.util.Set;
// import org.apache.beam.sdk.io.FileSystems;
// import org.apache.beam.sdk.io.fs.MatchResult;
// import org.apache.beam.sdk.io.fs.ResourceId;
// import org.junit.Assert;
// import org.junit.Before;
// import org.junit.Rule;
// import org.junit.Test;
// import org.junit.rules.TemporaryFolder;
// import org.junit.runner.RunWith;
// import org.junit.runners.JUnit4;
// import org.mockito.ArgumentMatchers;
// import org.mockito.MockedStatic;
// import org.mockito.Mockito;
//
// @RunWith(JUnit4.class)
// public class FileAwareFactoryFnTest {
//
//  @Rule public TemporaryFolder temporaryFolder = new TemporaryFolder();
//
//  private TestFactoryFn factory;
//  private String baseDir;
//  private static final String TEST_FACTORY_TYPE = "test-factory";
//
//  // A concrete implementation for testing the abstract FileAwareFactoryFn
//  static class TestFactoryFn extends FileAwareFactoryFn<Object> {
//    public TestFactoryFn() {
//      super(TEST_FACTORY_TYPE);
//    }
//
//    @Override
//    protected Object createObject(Map<String, Object> config) {
//      // Return the processed config for easy assertion
//      return config;
//    }
//  }
//
//  @Before
//  public void setup() throws IOException {
//    baseDir = "/tmp/" + TEST_FACTORY_TYPE;
//    factory = Mockito.spy(new TestFactoryFn());
//    Mockito.doReturn(baseDir).when(factory).getBaseDirectory();
//  }
//
//  @Test
//  public void testHappyPathReplacesGcsPath() throws IOException {
//    // Arrange
//    String gcsPath = "gs://test-bucket/config-file.json";
//    String expectedLocalPath = baseDir + "/test-bucket/config-file.json";
//
//    Map<String, Object> config = new HashMap<>();
//    config.put("config.file.path", gcsPath);
//
//    // Mock the downloadGcsFile method to "succeed" by creating a file
//    mockSuccessfulDownload(gcsPath, expectedLocalPath);
//
//    // Act
//    Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);
//
//    // Assert
//    Assert.assertEquals(expectedLocalPath, processedConfig.get("config.file.path"));
//    // Verify that the download created the file
//    Assert.assertTrue("Local file should have been created",
// Files.exists(Paths.get(expectedLocalPath)));
//  }
//
//  @Test
//  public void testApplyFailurePathThrowsRuntimeExceptionOnDownloadFailure() {
//    // Arrange
//    String gcsPath = "gs://test-bucket/failing-file.txt";
//    Map<String, Object> config = new HashMap<>();
//    config.put("critical.file", gcsPath);
//
//    // Mock the static FileSystems.matchSingleFileSpec to throw an exception
//    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class)) {
//      mockedFileSystems
//          .when(() -> FileSystems.matchSingleFileSpec(gcsPath))
//          .thenThrow(new IOException("GCS file not found"));
//
//      // Act & Assert
//      RuntimeException exception =
//          Assert.assertThrows(RuntimeException.class, () -> factory.apply(config));
//      Assert.assertTrue(exception.getMessage().contains("Failed trying to process value"));
//      Assert.assertTrue(exception.getCause() instanceof IOException);
//      Assert.assertTrue(exception.getCause().getMessage().contains("Failed to download file"));
//    }
//  }
//
//  @Test
//  public void testApplyHappyPathIgnoresNonGcsValues() {
//    // Arrange
//    Map<String, Object> config = new HashMap<>();
//    config.put("some.string", "/local/path/file.txt");
//    config.put("some.number", 42);
//    config.put("some.boolean", false);
//
//    // Act
//    Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);
//
//    // Assert
//    Assert.assertEquals(config, processedConfig);
//  }
//
//  @Test
//  public void testApplyEdgeCaseMultipleGcsPathsInSingleValue() {
//    // Arrange
//    String gcsPath1 = "gs://bucket/keytab.keytab";
//    String gcsPath2 = "gs://bucket/trust.jks";
//    String originalValue =
//        "jaas_config keyTab=\"" + gcsPath1 + "\" trustStore=\"" + gcsPath2 + "\"";
//
//    String expectedLocalPath1 =
//        FileAwareFactoryFn.DIRECTORY_PREFIX + "/" + TEST_FACTORY_TYPE + "/bucket/keytab.keytab";
//    String expectedLocalPath2 =
//        FileAwareFactoryFn.DIRECTORY_PREFIX + "/" + TEST_FACTORY_TYPE + "/bucket/trust.jks";
//    String expectedProcessedValue =
//        "jaas_config keyTab=\""
//            + expectedLocalPath1
//            + "\" trustStore=\""
//            + expectedLocalPath2
//            + "\"";
//
//    Map<String, Object> config = new HashMap<>();
//    config.put("jaas.config", originalValue);
//
//    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class)) {
//      // Mock GCS calls for both paths
//      mockSuccessfulDownload(mockedFileSystems, gcsPath1);
//      mockSuccessfulDownload(mockedFileSystems, gcsPath2);
//
//      // Act
//      Map<String, Object> processedConfig = (Map<String, Object>) factory.apply(config);
//
//      // Assert
//      Assert.assertEquals(expectedProcessedValue, processedConfig.get("jaas.config"));
//    }
//  }
//
//  @Test
//  public void testApplyEdgeCaseLocalFileWriteFails() throws IOException {
//    // Arrange
//    String gcsPath = "gs://test-bucket/some-file.txt";
//    Map<String, Object> config = new HashMap<>();
//    config.put("a.file", gcsPath);
//
//    // Mock GCS part to succeed
//    try (MockedStatic<FileSystems> mockedFileSystems = Mockito.mockStatic(FileSystems.class);
//        MockedStatic<FileChannel> mockedFileChannel = Mockito.mockStatic(FileChannel.class)) {
//      mockSuccessfulDownload(mockedFileSystems, gcsPath);
//
//      // Mock the local file writing part to fail
//      mockedFileChannel
//          .when(
//              () ->
//                  FileChannel.open(
//                      ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Set.class)))
//          .thenThrow(new IOException("Permission denied"));
//
//      // Act & Assert
//      RuntimeException exception =
//          Assert.assertThrows(RuntimeException.class, () -> factory.apply(config));
//      Assert.assertTrue(exception.getMessage().contains("Failed trying to process value"));
//      Assert.assertTrue(exception.getCause() instanceof IOException);
//      // Check that the root cause is our "Permission denied" mock
//      Assert.assertTrue(exception.getCause().getCause().getMessage().contains("Permission
// denied"));
//    }
//  }
//
//  @Test
//  public void testApplyHappyPathResolvesSecretValue() {
//    // Arrange
//    String secretVersion = "secretValue:projects/p/secrets/s/versions/v";
//    String secretVersionParsed = "projects/p/secrets/s/versions/v";
//    String secretValue = "my-secret-password";
//    String originalValue = "password=" + secretVersion;
//    String expectedProcessedValue = "password=" + secretValue;
//
//    Map<String, Object> config = new HashMap<>();
//    config.put("db.password", originalValue);
//
//    TestFactoryFn factoryWithMockedSecret =
//        new TestFactoryFn() {
//          @Override
//          public byte[] getSecret(String secretIdentifier) {
//            // Assert that the correct identifier is passed
//            Assert.assertEquals(secretVersionParsed, secretIdentifier);
//            // Return a predictable, hardcoded value for the test
//            return secretValue.getBytes(StandardCharsets.UTF_8);
//          }
//        };
//
//    // Act
//    @SuppressWarnings("unchecked")
//    Map<String, Object> processedConfig =
//        (Map<String, Object>) factoryWithMockedSecret.apply(config);
//
//    // Assert
//    Assert.assertEquals(expectedProcessedValue, processedConfig.get("db.password"));
//  }
//
//  @Test
//  public void testApplyFailurePathThrowsExceptionForInvalidSecretFormat() {
//    // Arrange
//    String invalidSecret = "secretValue:not-a-valid-secret-path";
//    Map<String, Object> config = new HashMap<>();
//    config.put("db.password", "password=" + invalidSecret);
//
//    // Act & Assert
//    RuntimeException ex = Assert.assertThrows(RuntimeException.class, () ->
// factory.apply(config));
//    Assert.assertEquals(IllegalArgumentException.class, ex.getCause().getClass());
//  }
//
//  // Helper method to mock a successful call to downloadGcsFile
//  private void mockSuccessfulDownload(String gcsPath, String expectedLocalPath) throws IOException
// {
//    Mockito.doAnswer(
//        invocation -> {
//          String localPathStr = invocation.getArgument(1);
//          Path path = Paths.get(localPathStr);
//          // Simulate the behavior of the real method
//          Files.createDirectories(path.getParent());
//          Files.createFile(path);
//          return null; // void method
//        })
//      .when(factory)
//      .downloadGcsFile(ArgumentMatchers.eq(gcsPath), ArgumentMatchers.eq(expectedLocalPath));
//  }
// }
