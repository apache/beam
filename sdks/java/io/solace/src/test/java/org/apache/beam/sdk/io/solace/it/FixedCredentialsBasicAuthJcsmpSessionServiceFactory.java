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
package org.apache.beam.sdk.io.solace.it;

import com.solacesystems.jcsmp.JCSMPProperties;
import java.util.Objects;
import org.apache.beam.sdk.io.solace.broker.JcsmpSessionService;
import org.apache.beam.sdk.io.solace.broker.SessionService;
import org.apache.beam.sdk.io.solace.broker.SessionServiceFactory;

public class FixedCredentialsBasicAuthJcsmpSessionServiceFactory extends SessionServiceFactory {
  private final String host;

  public FixedCredentialsBasicAuthJcsmpSessionServiceFactory(String host) {
    this.host = host;
  }

  @Override
  public SessionService create() {
    JCSMPProperties jcsmpProperties = new JCSMPProperties();
    jcsmpProperties.setProperty(JCSMPProperties.VPN_NAME, SolaceContainerManager.VPN_NAME);
    jcsmpProperties.setProperty(
        JCSMPProperties.AUTHENTICATION_SCHEME, JCSMPProperties.AUTHENTICATION_SCHEME_BASIC);
    jcsmpProperties.setProperty(JCSMPProperties.USERNAME, SolaceContainerManager.USERNAME);
    jcsmpProperties.setProperty(JCSMPProperties.PASSWORD, SolaceContainerManager.PASSWORD);
    jcsmpProperties.setProperty(JCSMPProperties.HOST, host);
    return JcsmpSessionService.create(jcsmpProperties, getQueue());
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    FixedCredentialsBasicAuthJcsmpSessionServiceFactory that =
        (FixedCredentialsBasicAuthJcsmpSessionServiceFactory) o;
    return Objects.equals(host, that.host);
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(host);
  }
}
