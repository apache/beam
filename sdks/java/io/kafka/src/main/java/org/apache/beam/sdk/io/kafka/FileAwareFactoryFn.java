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
package org.apache.beam.sdk.io.kafka;

import com.google.common.base.Preconditions;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileAwareFactoryFn<T>
    implements SerializableFunction<Map<String, Object>, T> {

  public static final String GCS_PATH_PREFIX = "gs://";

  public static final String DIRECTORY_PREFIX = "/tmp";

  private static final Pattern GCS_PATH_PATTERN = Pattern.compile("(gs://[^\"]+)");
  // private static final Map<String, String> secretCache = new ConcurrentHashMap<>();

  private final String factoryType;

  /* Logger for class.*/
  private static final Logger LOG = LoggerFactory.getLogger(FileAwareFactoryFn.class);

  public FileAwareFactoryFn(String factoryType) {
    Preconditions.checkNotNull(factoryType);
    this.factoryType = factoryType;
  }

  protected abstract T createObject(Map<String, Object> config);

  @Override
  public T apply(Map<String, Object> config) {
    if (config == null) {
      return createObject(config);
    }

    Map<String, Object> processedConfig = new HashMap<>(config);

    String key = "";
    Object value = null;
    try {
      downloadAndProcessExtraFiles();

      for (Map.Entry<String, Object> e : config.entrySet()) {
        try {
          key = e.getKey();
          value = e.getValue();
          if (value instanceof String) {
            String originalValue = (String) value;
            String processedValue = replaceGcsPathWithLocal(originalValue);
            processedConfig.put(key, processedValue);
          }
        } catch (IOException ex) {
          throw new RuntimeException(
              "Failed trying to process value " + value + " for key " + key + ".", ex);
        }
      }
    } catch (IOException e) {
      throw new RuntimeException("Failed trying to process extra files.", e);
    }

    LOG.info("ProcessedConfig: {}", processedConfig);
    return createObject(processedConfig);

    //      String key = e.getKey();
    //      String value = e.getValue() == null ? "" : e.getValue().toString();
    //      String processedValue = null;
    //
    //      try {
    //        if (value.startsWith(GCS_PATH_PREFIX)) {
    //          processedValue = downloadGcsFile(value, generateLocalFileName());
    //          processedConfig.put(key, processedValue);
    //        } else if (JAAS_CONFIG_PROPERTY.equals(key)) {
    //          Matcher matcher = KEYTAB_PATH_PATTERN.matcher(value);
    //          if (matcher.find()) {
    //            String keytabGcsPath = matcher.group(1);
    //            LOG.info("Found GCS path for keytab: {}", keytabGcsPath);
    //
    //            String localKeytabPath =
    //                downloadGcsFile(keytabGcsPath, generateLocalFileName() + ".keytab");
    //
    //            // Replace the GCS path in the JAAS string with the new local path
    //            String updatedJaasConfig = value.replace(keytabGcsPath, localKeytabPath);
    //            Path path = Paths.get(localKeytabPath);
    //            Set<PosixFilePermission> perms = new HashSet<>();
    //            perms.add(PosixFilePermission.OWNER_READ); // Add 'read' for the owner
    //            Files.setPosixFilePermissions(path, perms);
    //            File keytabFile = new File(localKeytabPath);
    //            if (keytabFile.exists() && keytabFile.canRead()) {
    //              LOG.info("Keytab file {} exists and is readable.", localKeytabPath);
    //            } else if (keytabFile.exists() && !keytabFile.canRead()) {
    //              LOG.info("Keytab file {} exists but is not readable.", localKeytabPath);
    //            } else {
    //              LOG.info("Keytab file {} does not exist.", localKeytabPath);
    //            }
    //
    //            processedConfig.put(JAAS_CONFIG_PROPERTY, updatedJaasConfig);
    //            LOG.info(
    //                "Updated '{}' to use local keytab path: {}. the full config is {}",
    //                JAAS_CONFIG_PROPERTY,
    //                localKeytabPath,
    //                updatedJaasConfig);
    //          }
    //        } else {
    //          processedConfig.put(key, value);
    //        }
    //      } catch (IOException ex) {
    //        throw new RuntimeException(
    //            "Couldn't load Kafka consumer property " + key + " = " + value, ex);
    //      }
    //    }

    // If the .conf path does not exist then we want to download the file and set the system
    // property. We don't want to
    // do this more than once, at the very beginning.

    //        String kerberosConfigFilePath = "/tmp/krb5.conf";
    //        String kerberosConfigGcsFilePath = "gs://fozzie_testing_bucket/kerberos/krb5.conf";
    //        if (!new File(kerberosConfigFilePath).exists()) {
    //          try {
    //            String kerberosConfigFile =
    //                downloadGcsFile(kerberosConfigGcsFilePath, kerberosConfigFilePath);
    //            LOG.info(
    //                "Successfully downloaded {} into {}.",
    //                kerberosConfigGcsFilePath,
    //                kerberosConfigFilePath);
    //            System.setProperty("java.security.krb5.conf", kerberosConfigFile);
    //            Configuration.getConfiguration().refresh();
    //            LOG.info(
    //                "Successfully set system property {} to {}.",
    //                "java.security.krb5.conf",
    //                System.getProperty("java.security.krb5.conf"));
    //          } catch (IOException e) {
    //            throw new RuntimeException("Could not load krb5.conf.", e);
    //          }
    //        }
    //
    //    return createObject(processedConfig);
  }

  /**
   * A function to download files from their specified gcs path and copy them to the provided local
   * filepath. The local filepath is provded by the replaceGcsPathWithLocal.
   *
   * @param gcsFilePath
   * @param outputFileString
   * @return
   * @throws IOException
   */
  protected static synchronized String downloadGcsFile(String gcsFilePath, String outputFileString)
      throws IOException {
    // create the file only if it doesn't exist
    if (!new File(outputFileString).exists()) {
      Path outputFilePath = Paths.get(outputFileString);
      Path parentDir = outputFilePath.getParent();
      if (parentDir != null) {
        Files.createDirectories(parentDir);
      }

      LOG.info("Staging GCS file [{}] to [{}]", gcsFilePath, outputFileString);
      Set<StandardOpenOption> options = new HashSet<>(2);
      options.add(StandardOpenOption.CREATE);
      options.add(StandardOpenOption.WRITE);

      // Copy the GCS file into a local file and will throw an I/O exception in case file not found.
      try (ReadableByteChannel readerChannel =
          FileSystems.open(FileSystems.matchSingleFileSpec(gcsFilePath).resourceId())) {
        try (FileChannel writeChannel = FileChannel.open(outputFilePath, options)) {
          writeChannel.transferFrom(readerChannel, 0, Long.MAX_VALUE);
        }
      }
    }
    return outputFileString;
  }

  /**
   * A helper method to create a new string with the gcs paths replaced with their local path and
   * subdirectory based on the factory type in the /tmp directory. For example, the kerberos factory
   * type will replace the file paths with /tmp/kerberos/file.path
   *
   * @param value
   * @return a string with all instances of GCS paths converted to the local paths where the files
   *     sit.
   */
  private String replaceGcsPathWithLocal(String value) throws IOException {
    Matcher matcher = GCS_PATH_PATTERN.matcher(value);
    StringBuffer sb = new StringBuffer();

    LOG.info("the current value being processed: {}", value);
    while (matcher.find()) {
      String gcsPath = matcher.group(1);
      LOG.info("THE GCS PATH IS: {}", gcsPath);
      if (gcsPath != null) {
        try {
          String tmpPath =
              DIRECTORY_PREFIX
                  + "/"
                  + factoryType
                  + "/"
                  + gcsPath.substring(GCS_PATH_PREFIX.length());
          String localPath = downloadGcsFile(gcsPath, tmpPath);
          matcher.appendReplacement(sb, Matcher.quoteReplacement(localPath));
          LOG.info("Downloaded {} to {}", gcsPath, localPath);
        } catch (IOException e) {
          throw new IOException("Failed to download file : " + gcsPath, e);
        }
      }
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  /**
   * @throws IOException A hook for subclasses to download and process specific files before the
   *     main configuration is handled. For example, the kerberos factory can use this to download a
   *     krb5.conf and set a system property.
   */
  protected void downloadAndProcessExtraFiles() throws IOException {
    // Default implementation should do nothing.
  }

  protected String getBaseDirectory() {
    return DIRECTORY_PREFIX;
  }
}
