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
package org.apache.beam.sdk.io.mongodb;

import java.io.InputStream;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import org.apache.beam.sdk.util.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Utility class for registration of ssl context, and to allow all certificate requests. */
class SSLUtils {
  private static final Logger LOG = LoggerFactory.getLogger(SSLUtils.class);

  /** static class to allow all requests. */
  private static final TrustManager[] trustAllCerts =
      new TrustManager[] {
        new X509TrustManager() {
          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }

          @Override
          public void checkClientTrusted(X509Certificate[] certs, String authType) {}

          @Override
          public void checkServerTrusted(X509Certificate[] certs, String authType) {}
        }
      };

  /**
   * register ssl contects to accept all issue certificates.
   *
   * @return SSLContext
   */
  static SSLContext ignoreSSLCertificate() {
    try {
      // Install the all-trusting trust manager
      SSLContext sc = SSLContext.getInstance("TLS");
      sc.init(null, trustAllCerts, new java.security.SecureRandom());

      KeyStore ks =
          Preconditions.checkStateNotNull(KeyStore.getInstance("JKS"), "Keystore 'JKS' not found");
      ClassLoader classLoader =
          Preconditions.checkStateNotNull(
              SSLUtils.class.getClassLoader(), "SSLUtil classloader is null - boot classloader?");
      InputStream inputStream = classLoader.getResourceAsStream("resources/.keystore");
      if (inputStream != null) {
        LOG.info("Found keystore in classpath 'resources/.keystore'. Loading...");
      } else {
        LOG.info(
            "Unable to find keystore under 'resources/.keystore' in the classpath. "
                + "Continuing with an empty keystore.");
      }
      ks.load(inputStream, "changeit".toCharArray());
      KeyManagerFactory kmf =
          KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
      kmf.init(ks, "changeit".toCharArray());
      SSLContext ctx = SSLContext.getInstance("TLS");
      ctx.init(kmf.getKeyManagers(), trustAllCerts, null);
      SSLContext.setDefault(ctx);
      return ctx;
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
