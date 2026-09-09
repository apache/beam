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
import com.ibm.mq.jms.MQConnectionFactory;
import com.ibm.msg.client.wmq.WMQConstants;
import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URI;
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
    Class<?> clazz;
    Class<? extends BeamGenericJmsConnectionFactory> factoryClass;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(
          String.format(
              "ConnectionFactory %s does not exist. If using expansion service, attach the connection factory jar as part of its invocation classpath.",
              className),
          e);
    }
    if (BeamGenericJmsConnectionFactory.class.isAssignableFrom(clazz)) {
      factoryClass = (Class<? extends BeamGenericJmsConnectionFactory>) clazz;
    } else if (className.contains("org.apache.activemq.ActiveMQConnectionFactory")
        || className.contains("org.apache.qpid.jms")) {
      // Connectors supported by StandardJmsConnectionFactory
      factoryClass = StandardJmsConnectionFactory.class;
    } else if (className.contains("com.ibm.mq")) {
      factoryClass = IbmMqJmsConnectionFactory.class;
    } else {
      // Attempt to use StandardJmsConnectionFactory.class;
      factoryClass = StandardJmsConnectionFactory.class;
    }
    try {
      BeamGenericJmsConnectionFactory factory = factoryClass.getDeclaredConstructor().newInstance();
      return factory.createConnectionFactory(this);
    } catch (Exception e) {
      throw new IllegalArgumentException(
          "Unable to instantiate JMS ConnectionFactory of class "
              + className
              + ". Must be a supported provider (ActiveMQ, Qpid, IBM MQ) or implement BeamGenericJmsConnectionFactory.",
          e);
    }
  }

  /**
   * A {@link BeamGenericJmsConnectionFactory} implementation for standard JMS connection factories.
   */
  public static class StandardJmsConnectionFactory implements BeamGenericJmsConnectionFactory {

    @Override
    public ConnectionFactory createConnectionFactory(ConnectionConfiguration config)
        throws Exception {
      String className = config.getConnectionFactoryClassName();
      if (className == null || className.isEmpty()) {
        className = "org.apache.activemq.ActiveMQConnectionFactory";
      }
      Class<?> clazz = Class.forName(className);
      String uri = config.getServerUri();
      String username = config.getUsername();
      String password = config.getPassword();

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
        boolean setPasswordSuccess =
            invokeMethodIfExists(cf, "setPassword", String.class, password);

        if (!setUsernameSuccess || !setPasswordSuccess) {
          LOG.warn("Unable to set username/password on JMS ConnectionFactory of class {}", clazz);
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
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        return false;
      }
    }
  }

  /** A {@link BeamGenericJmsConnectionFactory} implementation for IBM MQ. */
  public static class IbmMqJmsConnectionFactory implements BeamGenericJmsConnectionFactory {

    @Override
    public ConnectionFactory createConnectionFactory(ConnectionConfiguration config)
        throws Exception {
      MQConnectionFactory cf = new MQConnectionFactory();
      cf.setTransportType(WMQConstants.WMQ_CM_CLIENT);

      String uri = config.getServerUri();
      if (!Strings.isNullOrEmpty(uri)) {
        URI parsedUri = new URI(uri);
        String host = parsedUri.getHost();
        int port = parsedUri.getPort();
        if (host != null) {
          cf.setHostName(host);
        }
        if (port > 0) {
          cf.setPort(port);
        }
        if (parsedUri.getQuery() != null) {
          for (String param : Splitter.on('&').split(parsedUri.getQuery())) {
            List<String> pair = Splitter.on('=').splitToList(param);
            if (pair.size() == 2) {
              if ("channel".equalsIgnoreCase(pair.get(0))) {
                cf.setChannel(pair.get(1));
              } else if ("queueManager".equalsIgnoreCase(pair.get(0))) {
                cf.setQueueManager(pair.get(1));
              }
            }
          }
        }
      }

      String username = config.getUsername();
      if (username != null) {
        cf.setBooleanProperty(WMQConstants.USER_AUTHENTICATION_MQCSP, true);
        cf.setStringProperty(WMQConstants.USERID, username);
        String password = config.getPassword();
        if (password != null) {
          cf.setStringProperty(WMQConstants.PASSWORD, password);
        }
      }
      return cf;
    }
  }
}
