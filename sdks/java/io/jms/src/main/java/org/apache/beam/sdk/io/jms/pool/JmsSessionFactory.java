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
package org.apache.beam.sdk.io.jms.pool;

import static org.apache.beam.sdk.io.jms.JmsIO.Writer.JMS_IO_PRODUCER_METRIC_NAME;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Session;
import org.apache.beam.sdk.io.jms.JmsIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.commons.pool2.BasePooledObjectFactory;
import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings({
  "nullness" // TODO(https://github.com/apache/beam/issues/20497)
})
public class JmsSessionFactory<T> extends BasePooledObjectFactory<Session> {

  private static final Logger LOG = LoggerFactory.getLogger(JmsSessionFactory.class);
  public static final String CONNECTION_ERRORS_METRIC_NAME = "connectionErrors";

  private final Counter connectionErrors =
      Metrics.counter(JMS_IO_PRODUCER_METRIC_NAME, CONNECTION_ERRORS_METRIC_NAME);

  private final JmsIO.Write<T> spec;
  private boolean isConnectionClosed;

  public JmsSessionFactory(JmsIO.Write<T> spec) {
    this.spec = spec;
  }

  @Override
  public Session create() throws JMSException {
    Connection connection;
    // reset the connection flag
    this.isConnectionClosed = false;
    ConnectionFactory connectionFactory = spec.getConnectionFactory();
    if (spec.getUsername() != null) {
      connection = connectionFactory.createConnection(spec.getUsername(), spec.getPassword());
    } else {
      connection = connectionFactory.createConnection();
    }
    connection.setExceptionListener(
        exception -> {
          this.isConnectionClosed = true;
          this.connectionErrors.inc();
        });
    LOG.debug("creating new connection: {}", connection);
    connection.start();
    // false means we don't use JMS transaction.
    return connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
  }

  @Override
  public boolean validateObject(PooledObject<Session> pooledObject) {
    return !isConnectionClosed && !callSessionMethod(pooledObject.getObject(), "isClosed", true);
  }

  @Override
  public void destroyObject(PooledObject<Session> pooledObject) throws JMSException {
    Session session = pooledObject.getObject();
    session.close();
    Connection connection = callSessionMethod(session, "getConnection", null);
    if (connection != null) {
      connection.close();
    }
  }

  @Override
  public PooledObject<Session> wrap(Session session) {
    return new DefaultPooledObject<>(session);
  }

  private <U> U callSessionMethod(Session session, String methodName, U defaultValue) {
    Method isClosed = getSessionMethod(session, methodName);
    if (isClosed != null) {
      try {
        return (U) isClosed.invoke(session);
      } catch (IllegalAccessException | InvocationTargetException exception) {
        LOG.debug(
            "The class {} couldn't allow access to the function 'isClosed' with the following error {}",
            session.getClass(),
            exception.getMessage());
      }
    }
    return defaultValue;
  }

  private Method getSessionMethod(Object session, String methodName) {
    try {
      return session.getClass().getDeclaredMethod(methodName);
    } catch (NoSuchMethodException e) {
      LOG.debug(
          "The class {} implemented JMS Session doesn't contain a function to check if session is closed",
          session.getClass());
    }
    return null;
  }
}
