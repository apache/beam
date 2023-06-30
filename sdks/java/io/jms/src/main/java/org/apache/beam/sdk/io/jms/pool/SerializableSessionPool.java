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

import java.io.Closeable;
import java.io.Serializable;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.commons.pool2.ObjectPool;

public abstract class SerializableSessionPool<T> implements ObjectPool<T>, Serializable, Closeable {

  private final int maxConnections;
  private final AtomicReference<ObjectPool<T>> delegate = new AtomicReference<>();

  protected SerializableSessionPool(int maxConnections) {
    this.maxConnections = maxConnections;
  }

  protected abstract ObjectPool<T> createDelegate();

  public ObjectPool<T> getDelegate() {
    ObjectPool<T> value = this.delegate.get();
    if (value == null) {
      synchronized (this.delegate) {
        value = this.delegate.get();
        if (value == null) {
          final ObjectPool<T> actualValue = createDelegate();
          value = actualValue;
          this.delegate.set(actualValue);
        }
      }
    }
    return value;
  }

  @Override
  public T borrowObject() throws Exception {
    ObjectPool<T> pool = getDelegate();
    if (pool.getNumIdle() == 0 && pool.getNumActive() < maxConnections) {
      pool.addObject();
    }
    return pool.borrowObject();
  }

  @Override
  public void returnObject(T obj) throws Exception {
    getDelegate().returnObject(obj);
  }

  @Override
  public void invalidateObject(T obj) throws Exception {
    getDelegate().invalidateObject(obj);
  }

  @Override
  public void addObject() throws Exception {
    getDelegate().addObject();
  }

  @Override
  public int getNumIdle() {
    return getDelegate().getNumIdle();
  }

  @Override
  public int getNumActive() {
    return getDelegate().getNumActive();
  }

  @Override
  public void clear() throws Exception {
    getDelegate().clear();
  }

  @Override
  public void close() {
    getDelegate().close();
  }
}
