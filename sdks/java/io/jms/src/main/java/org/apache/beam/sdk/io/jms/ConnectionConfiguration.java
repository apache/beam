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
package org.apache.beam.sdk.io.jms;

import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.auto.value.AutoValue;
import java.io.Serializable;
import java.lang.reflect.Method;
import java.util.List;
import javax.jms.ConnectionFactory;
import org.apache.beam.sdk.schemas.AutoValueSchema;
import org.apache.beam.sdk.schemas.annotations.DefaultSchema;
import org.apache.beam.sdk.schemas.annotations.SchemaFieldDescription;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Splitter;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Strings;
import org.checkerframework.checker.nullness.qual.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A POJO describing a JMS connection, used by SchemaTransformProvider. */
@DefaultSchema(AutoValueSchema.class)
@AutoValue
public abstract class ConnectionConfiguration implements Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(ConnectionConfiguration.class);

  public static Builder builder() {
    return new AutoValue_ConnectionConfiguration.Builder();
  }

  public static ConnectionConfiguration create(
      String serverUri, @Nullable String connectionFactoryClassName) {
    checkArgument(serverUri != null, "serverUri can not be null");
    return builder()
        .setServerUri(serverUri)
        .setConnectionFactoryClassName(connectionFactoryClassName)
        .build();
  }

  public static ConnectionConfiguration create(String serverUri) {
    return create(serverUri, null);
  }

  @SchemaFieldDescription("The JMS broker URI.")
  public abstract String getServerUri();

  @SchemaFieldDescription("The JMS ConnectionFactory class name.")
  public abstract @Nullable String getConnectionFactoryClassName();

  @SchemaFieldDescription("The username to connect to the JMS broker.")
  public abstract @Nullable String getUsername();

  @SchemaFieldDescription("The password to connect to the JMS broker.")
  public abstract @Nullable String getPassword();

  public ConnectionConfiguration withUsername(String username) {
    return toBuilder().setUsername(username).build();
  }

  public ConnectionConfiguration withPassword(String password) {
    return toBuilder().setPassword(password).build();
  }

  public ConnectionConfiguration withConnectionFactoryClassName(String connectionFactoryClassName) {
    return toBuilder().setConnectionFactoryClassName(connectionFactoryClassName).build();
  }

  abstract Builder toBuilder();

  @AutoValue.Builder
  public abstract static class Builder {
    public abstract Builder setServerUri(String serverUri);

    public abstract Builder setConnectionFactoryClassName(
        @Nullable String connectionFactoryClassName);

    public abstract Builder setUsername(@Nullable String username);

    public abstract Builder setPassword(@Nullable String password);

    public abstract ConnectionConfiguration build();
  }

  public ConnectionFactory createConnectionFactory() {
    String className = getConnectionFactoryClassName();
    // Default to ActiveMQ
    if (className == null || className.isEmpty()) {
      className = "org.apache.activemq.ActiveMQConnectionFactory";
    }
    try {
      Class<?> clazz = Class.forName(className);
      String uri = getServerUri();
      String username = getUsername();
      String password = getPassword();

      if (className.contains("org.apache.activemq.ActiveMQConnectionFactory")) {
        return createActiveMqConnectionFactory(clazz, uri, username, password);
      } else if (className.contains("org.apache.qpid.jms")) {
        return createQpidConnectionFactory(clazz, uri, username, password);
      } else if (className.contains("com.ibm.mq")) {
        return createIbmMqConnectionFactory(clazz, uri, username, password);
      } else if (BeamGenericJmsConnectionFactory.class.isAssignableFrom(clazz)) {
        BeamGenericJmsConnectionFactory factory =
            (BeamGenericJmsConnectionFactory) clazz.getDeclaredConstructor().newInstance();
        return factory.createConnectionFactory(this);
      } else {
        try {
          return createStandardConnectionFactory(clazz, uri, username, password);
        } catch (Exception e) {
          throw new IllegalArgumentException(
              "Unable to instantiate JMS ConnectionFactory of class "
                  + className
                  + ". Must be a supported provider (ActiveMQ, Qpid, IBM MQ) or implement BeamGenericJmsConnectionFactory.",
              e);
        }
      }
    } catch (IllegalArgumentException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to instantiate JMS ConnectionFactory of class " + className, e);
    }
  }

  private ConnectionFactory createActiveMqConnectionFactory(
      Class<?> clazz, String uri, @Nullable String username, @Nullable String password)
      throws Exception {
    return createStandardConnectionFactory(clazz, uri, username, password);
  }

  private ConnectionFactory createQpidConnectionFactory(
      Class<?> clazz, String uri, @Nullable String username, @Nullable String password)
      throws Exception {
    return createStandardConnectionFactory(clazz, uri, username, password);
  }

  private ConnectionFactory createStandardConnectionFactory(
      Class<?> clazz, String uri, @Nullable String username, @Nullable String password)
      throws Exception {
    if (username != null && password != null) {
      try {
        return (ConnectionFactory)
            clazz
                .getConstructor(String.class, String.class, String.class)
                .newInstance(username, password, uri);
      } catch (NoSuchMethodException e) {
        // Fall through to 1-arg or 0-arg constructor + setters
      }
    }
    ConnectionFactory cf;
    try {
      cf = (ConnectionFactory) clazz.getConstructor(String.class).newInstance(uri);
    } catch (NoSuchMethodException e) {
      cf = (ConnectionFactory) clazz.getConstructor().newInstance();
    }

    if (username != null && password != null) {
      boolean setUsernameSuccess =
          // ActiveMQ (capital N)
          invokeMethodIfExists(cf, "setUserName", String.class, username)
              // Qpid (lowercase n)
              || invokeMethodIfExists(cf, "setUsername", String.class, username);
      boolean setPasswordSuccess = invokeMethodIfExists(cf, "setPassword", String.class, password);

      if (!setUsernameSuccess || !setPasswordSuccess) {
        LOG.warn("Unable to set username/password on JMS ConnectionFactory of class {}", clazz);
      }
    }
    return cf;
  }

  /**
   * Instantiates and configures an IBM MQ {@link ConnectionFactory} for TCP client mode.
   *
   * <p>Sets transport type to client mode ({@code WMQ_CM_CLIENT = 1}), extracts host, port,
   * channel, and queueManager parameters from the connection URI, and sets username/password if
   * provided.
   */
  private ConnectionFactory createIbmMqConnectionFactory(
      Class<?> clazz, String uri, @Nullable String username, @Nullable String password)
      throws Exception {
    ConnectionFactory cf = (ConnectionFactory) clazz.getConstructor().newInstance();
    // WMQ_CM_CLIENT = 1 (Network TCP connection mode)
    invokeMethodIfExists(cf, "setTransportType", int.class, 1);

    if (!Strings.isNullOrEmpty(uri)) {
      java.net.URI parsedUri = new java.net.URI(uri);
      String host = parsedUri.getHost();
      int port = parsedUri.getPort();
      if (host != null) {
        invokeMethodIfExists(cf, "setHostName", String.class, host);
      }
      if (port > 0) {
        invokeMethodIfExists(cf, "setPort", int.class, port);
      }
      if (parsedUri.getQuery() != null) {
        for (String param : Splitter.on('&').split(parsedUri.getQuery())) {
          List<String> pair = Splitter.on('=').splitToList(param);
          if (pair.size() == 2) {
            if ("channel".equalsIgnoreCase(pair.get(0))) {
              invokeMethodIfExists(cf, "setChannel", String.class, pair.get(1));
            } else if ("queueManager".equalsIgnoreCase(pair.get(0))) {
              invokeMethodIfExists(cf, "setQueueManager", String.class, pair.get(1));
            }
          }
        }
      }
    }

    if (username != null) {
      // IBM MQ requires USER_AUTHENTICATION_MQCSP (XMSC_USER_AUTHENTICATION_MQCSP) set to true for
      // username/password authentication
      invokeTwoArgMethodIfExists(
          cf,
          "setBooleanProperty",
          String.class,
          "XMSC_USER_AUTHENTICATION_MQCSP",
          boolean.class,
          true);

      boolean setUsernameSuccess =
          invokeTwoArgMethodIfExists(
              cf, "setStringProperty", String.class, "XMSC_USERID", String.class, username);

      boolean setPasswordSuccess = true;
      if (password != null) {
        setPasswordSuccess =
            invokeTwoArgMethodIfExists(
                cf, "setStringProperty", String.class, "XMSC_PASSWORD", String.class, password);
      }

      if (!setUsernameSuccess || !setPasswordSuccess) {
        LOG.warn("Unable to set username/password on IBM MQ ConnectionFactory of class {}", clazz);
      }
    }
    return cf;
  }

  private static boolean invokeMethodIfExists(
      Object target, String methodName, Class<?> paramType, Object arg) {
    try {
      Method m = target.getClass().getMethod(methodName, paramType);
      m.invoke(target, arg);
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private static boolean invokeTwoArgMethodIfExists(
      Object target,
      String methodName,
      Class<?> p1Type,
      Object arg1,
      Class<?> p2Type,
      Object arg2) {
    try {
      Method m = target.getClass().getMethod(methodName, p1Type, p2Type);
      m.invoke(target, arg1, arg2);
      return true;
    } catch (Exception e) {
      return false;
    }
  }
}
