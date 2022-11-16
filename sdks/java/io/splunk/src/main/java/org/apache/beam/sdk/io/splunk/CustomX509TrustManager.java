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
package org.apache.beam.sdk.io.splunk;

import java.io.IOException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;

/** A Custom X509TrustManager that trusts a user provided CA and default CA's. */
public class CustomX509TrustManager implements X509TrustManager {

  private final X509TrustManager defaultTrustManager;
  private final X509TrustManager userTrustManager;

  public CustomX509TrustManager(X509Certificate userCertificate)
      throws CertificateException, KeyStoreException, NoSuchAlgorithmException, IOException {
    // Get Default Trust Manager
    TrustManagerFactory trustMgrFactory =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustMgrFactory.init((KeyStore) null);
    defaultTrustManager = getX509TrustManager(trustMgrFactory.getTrustManagers());

    // Create Trust Manager with user provided certificate
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(null, null);
    trustStore.setCertificateEntry("User Provided Root CA", userCertificate);
    trustMgrFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    trustMgrFactory.init(trustStore);
    userTrustManager = getX509TrustManager(trustMgrFactory.getTrustManagers());
  }

  private X509TrustManager getX509TrustManager(TrustManager[] trustManagers) {
    for (TrustManager tm : trustManagers) {
      if (tm instanceof X509TrustManager) {
        return (X509TrustManager) tm;
      }
    }
    return null;
  }

  @Override
  public void checkClientTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    defaultTrustManager.checkClientTrusted(chain, authType);
  }

  @Override
  public void checkServerTrusted(X509Certificate[] chain, String authType)
      throws CertificateException {
    try {
      defaultTrustManager.checkServerTrusted(chain, authType);
    } catch (CertificateException ce) {
      // If the certificate chain couldn't be verified using the default trust manager,
      // try verifying the same with the user-provided root CA
      userTrustManager.checkServerTrusted(chain, authType);
    }
  }

  @Override
  public X509Certificate[] getAcceptedIssuers() {
    return defaultTrustManager.getAcceptedIssuers();
  }
}
