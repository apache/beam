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

import static org.mockito.Mockito.spy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import javax.security.auth.login.Configuration;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.ArgumentMatchers;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

@RunWith(JUnit4.class)
public class KerberosConsumerFactoryFnTest {

  private KerberosConsumerFactoryFn factory;
  private String originalKrb5Conf;
  private static final String KRB5_GCS_PATH = "gs://sec-bucket/kerberos/krb5.conf";
  private static final String LOCAL_FACTORY_TYPE = "kerberos";

  @Before
  public void setup() {
    try {
      java.lang.reflect.Field field =
          KerberosConsumerFactoryFn.class.getDeclaredField("localKrb5ConfPath");
      field.setAccessible(true);
      field.set(null, "");
    } catch (Exception e) {
      throw new RuntimeException(e);
    }

    factory = spy(new KerberosConsumerFactoryFn(KRB5_GCS_PATH));
    originalKrb5Conf = System.getProperty("java.security.krb5.conf");
  }

  @After
  public void tearDown() throws IOException {
    // Clean up system property to avoid affecting other tests
    if (originalKrb5Conf != null) {
      System.setProperty("java.security.krb5.conf", originalKrb5Conf);
    } else {
      System.clearProperty("java.security.krb5.conf");
    }

    // Clean up the directory created outside of the JUnit TemporaryFolder rule.
    Path pathToDelete = Paths.get(FileAwareFactoryFn.DIRECTORY_PREFIX, LOCAL_FACTORY_TYPE);
    if (Files.exists(pathToDelete)) {
      try (Stream<Path> walk = Files.walk(pathToDelete)) {
        walk.sorted(Comparator.reverseOrder()).map(Path::toFile).forEach(File::delete);
      }
    }
  }

  @Test
  @SuppressWarnings("rawtypes")
  public void testHappyPath() {
    // Arrange
    String keytabGcsPath = "gs://sec-bucket/keytabs/my.keytab";
    String expectedKrb5LocalPath = "/tmp/kerberos/krb5.conf";
    String expectedKeytabLocalPath = "/tmp/kerberos/sec-bucket/keytabs/my.keytab";

    Map<String, Object> config = new HashMap<>();
    config.put(
        "sasl.jaas.config",
        "com.sun.security.auth.module.Krb5LoginModule required keyTab=\""
            + keytabGcsPath
            + "\" principal=\"user@REALM\";");

    try (MockedStatic<FileAwareFactoryFn> mockedStaticFactory =
            Mockito.mockStatic(FileAwareFactoryFn.class);
        MockedStatic<Configuration> mockedConfiguration = Mockito.mockStatic(Configuration.class);
        MockedStatic<Files> mockedFiles = Mockito.mockStatic(Files.class);
        MockedConstruction<KafkaConsumer> mockedConsumer =
            Mockito.mockConstruction(KafkaConsumer.class)) {

      Assert.assertNotNull(mockedConsumer);
      // Mock the static downloadGcsFile method to prevent any GCS interaction
      mockedStaticFactory
          .when(() -> FileAwareFactoryFn.downloadGcsFile(KRB5_GCS_PATH, expectedKrb5LocalPath))
          .thenReturn(expectedKrb5LocalPath);
      mockedStaticFactory
          .when(() -> FileAwareFactoryFn.downloadGcsFile(keytabGcsPath, expectedKeytabLocalPath))
          .thenReturn(expectedKeytabLocalPath);

      Configuration mockConf = Mockito.mock(Configuration.class);
      mockedConfiguration.when(Configuration::getConfiguration).thenReturn(mockConf);
      mockedFiles
          .when(
              () ->
                  Files.setPosixFilePermissions(
                      ArgumentMatchers.any(Path.class), ArgumentMatchers.any(Set.class)))
          .thenReturn(null);
      mockedFiles
          .when(() -> Files.createDirectories(ArgumentMatchers.any(Path.class)))
          .thenReturn(null);

      // Act
      factory.apply(config);

      // Assert
      // 1. Verify that the krb5.conf system property was set correctly.
      Assert.assertEquals(expectedKrb5LocalPath, System.getProperty("java.security.krb5.conf"));

      // 2. Capture the config passed to createObject and verify the keytab path was replaced.
      ArgumentCaptor<Map<String, Object>> configCaptor = ArgumentCaptor.forClass(Map.class);
      Mockito.verify(factory).createObject(configCaptor.capture());
      Map<String, Object> capturedConfig = configCaptor.getValue();
      String processedJaasConfig = (String) capturedConfig.get("sasl.jaas.config");
      Assert.assertTrue(processedJaasConfig.contains("keyTab=\"" + expectedKeytabLocalPath + "\""));

      // 3. Verify that the JAAS configuration was refreshed.
      Mockito.verify(mockConf).refresh();
    }
  }
}
