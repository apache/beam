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

import com.google.cloud.secretmanager.v1.AccessSecretVersionResponse;
import com.google.cloud.secretmanager.v1.SecretManagerServiceClient;
import com.google.cloud.secretmanager.v1.SecretVersionName;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class FileAwareFactoryFn<T>
    implements SerializableFunction<Map<String, Object>, T> {

  public static final String SECRET_VALUE_PREFIX = "secretValue:";
  public static final String DIRECTORY_PREFIX = "/tmp";
  private static final Pattern PATH_PATTERN =
      Pattern.compile("([a-zA-Z0-9]+://[^\"]+)|(secretValue:[^\"]+)|(secretFile:[^\"]+)");

  private static final Map<String, byte[]> secretCache = new ConcurrentHashMap<>();

  private final String factoryType;
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
            Matcher matcher = PATH_PATTERN.matcher(originalValue);
            StringBuffer sb = new StringBuffer();

            while (matcher.find()) {
              String externalPath = matcher.group(1);
              String secretValue = matcher.group(2);
              String secretFile = matcher.group(3);

              if (externalPath != null) {
                try {
                  String tmpPath = replacePathWithLocal(externalPath);
                  String localPath = downloadExternalFile(externalPath, tmpPath);
                  matcher.appendReplacement(sb, Matcher.quoteReplacement(localPath));
                  LOG.info("Downloaded {} to {}", externalPath, localPath);
                } catch (IOException io) {
                  throw new IOException("Failed to download file : " + externalPath, io);
                }
              } else if (secretValue != null) {
                try {
                  String secretId = secretValue.substring(SECRET_VALUE_PREFIX.length());
                  String processedSecret =
                      processSecret(originalValue, secretId, getSecretWithCache(secretId));

                  matcher.appendReplacement(sb, Matcher.quoteReplacement(processedSecret));
                } catch (IllegalArgumentException ia) {
                  throw new IllegalArgumentException("Failed to get secret.", ia);
                }
              } else if (secretFile != null) {
                throw new UnsupportedOperationException("Not yet implemented.");
              }
            }
            matcher.appendTail(sb);
            String processedValue = sb.toString();
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
  }

  /**
   * A function to download files from their specified external storage path and copy them to the
   * provided local filepath. The local filepath is provided by the replacePathWithLocal.
   *
   * @param externalFilePath
   * @param outputFileString
   * @return
   * @throws IOException
   */
  protected static synchronized String downloadExternalFile(
      String externalFilePath, String outputFileString) throws IOException {
    // create the file only if it doesn't exist
    if (new File(outputFileString).exists()) {
      return outputFileString;
    }
    Path outputFilePath = Paths.get(outputFileString);
    Path parentDir = outputFilePath.getParent();
    if (parentDir != null) {
      Files.createDirectories(parentDir);
    }
    LOG.info("Staging external file [{}] to [{}]", externalFilePath, outputFileString);
    Set<StandardOpenOption> options = new HashSet<>(2);
    options.add(StandardOpenOption.CREATE);
    options.add(StandardOpenOption.WRITE);

    // Copy the external file into a local file and will throw an I/O exception in case file not
    // found.
    try (ReadableByteChannel readerChannel =
        FileSystems.open(FileSystems.matchSingleFileSpec(externalFilePath).resourceId())) {
      try (FileChannel writeChannel = FileChannel.open(outputFilePath, options)) {
        writeChannel.transferFrom(readerChannel, 0, Long.MAX_VALUE);
      }
    }
    return outputFileString;
  }

  protected byte[] getSecretWithCache(String secretId) {
    return secretCache.computeIfAbsent(secretId, this::getSecret);
  }

  /**
   * A helper method to create a new string with the external paths replaced with their local path
   * and subdirectory based on the factory type in the /tmp directory. For example, the kerberos
   * factory type will replace the file paths with /tmp/kerberos/file.path
   *
   * @param externalPath
   * @return a string with all instances of external paths converted to the local paths where the
   *     files sit.
   */
  private String replacePathWithLocal(String externalPath) throws IOException {
    String externalBucketPrefixIdentifier = "://";
    int externalBucketPrefixIndex = externalPath.lastIndexOf(externalBucketPrefixIdentifier);
    if (externalBucketPrefixIndex == -1) {
      // if we don't find a known bucket prefix then we will error early.
      throw new RuntimeException(
          "The provided external bucket could not be matched to a known source.");
    }

    int prefixLength = externalBucketPrefixIndex + externalBucketPrefixIdentifier.length();
    return DIRECTORY_PREFIX + "/" + factoryType + "/" + externalPath.substring(prefixLength);
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

  protected byte[] getSecret(String secretVersion) {
    SecretVersionName secretVersionName;
    if (SecretVersionName.isParsableFrom(secretVersion)) {
      secretVersionName = SecretVersionName.parse(secretVersion);
    } else {
      throw new IllegalArgumentException(
          "Provided Secret must be in the form"
              + " projects/{project}/secrets/{secret}/versions/{secret_version}");
    }
    try (SecretManagerServiceClient client = SecretManagerServiceClient.create()) {
      AccessSecretVersionResponse response = client.accessSecretVersion(secretVersionName);
      return response.getPayload().getData().toByteArray();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected String processSecret(String originalValue, String secretId, byte[] secretValue) {
    // By Default, this will return the secret value directly. This function can be overridden by
    // derived classes.
    return new String(secretValue, StandardCharsets.UTF_8);
  }
}
