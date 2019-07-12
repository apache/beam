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

import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.HttpsURLConnection;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;

/** Utility class for registration of ssl context, and to allow all certificate requests. */
class SSLUtils {

  /** static class to allow all requests. */
  private static final TrustManager[] trustAllCerts =
      new TrustManager[] {
        new X509TrustManager() {
          @Override
          public java.security.cert.X509Certificate[] getAcceptedIssuers() {
            return null;
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
      HttpsURLConnection.setDefaultSSLSocketFactory(sc.getSocketFactory());

      KeyStore ks = KeyStore.getInstance("JKS");
      ks.load(
          SSLUtils.class.getClassLoader().getResourceAsStream("resources/.keystore"),
          "changeit".toCharArray());
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
