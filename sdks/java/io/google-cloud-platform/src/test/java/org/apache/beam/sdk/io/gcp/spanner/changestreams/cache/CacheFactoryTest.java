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
package org.apache.beam.sdk.io.gcp.spanner.changestreams.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.withSettings;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.apache.beam.sdk.io.gcp.spanner.changestreams.dao.DaoFactory;
import org.joda.time.Duration;
import org.junit.Before;
import org.junit.Test;

public class CacheFactoryTest {

  private DaoFactory daoFactory;

  @Before
  public void setUp() throws Exception {
    daoFactory = mock(DaoFactory.class, withSettings().serializable());
  }

  @Test
  public void testReturnsNoOpWatermarkCacheWhenDurationIsZero() {
    CacheFactory cacheFactory = new CacheFactory(daoFactory, Duration.ZERO);

    assertEquals(NoOpWatermarkCache.class, cacheFactory.getWatermarkCache().getClass());
  }

  @Test
  public void testReturnsAsyncWatermarkCacheWhenDurationIsNonZero() {
    CacheFactory cacheFactory = new CacheFactory(daoFactory, Duration.standardSeconds(1));

    assertEquals(AsyncWatermarkCache.class, cacheFactory.getWatermarkCache().getClass());
  }

  @Test
  public void testSerializeDeserialize() throws Exception {
    CacheFactory cacheFactory = new CacheFactory(daoFactory, Duration.standardSeconds(5));
    long cacheId = cacheFactory.getCacheId();
    WatermarkCache watermarkCache = cacheFactory.getWatermarkCache();

    CacheFactory deserializedCacheFactory = deserialize(serialize(cacheFactory));

    assertEquals(cacheId, deserializedCacheFactory.getCacheId());
    assertEquals(watermarkCache, deserializedCacheFactory.getWatermarkCache());
  }

  @Test
  public void testMultipleDeserializations() throws Exception {
    CacheFactory cacheFactory = new CacheFactory(daoFactory, Duration.standardSeconds(5));
    long cacheId = cacheFactory.getCacheId();
    WatermarkCache watermarkCache = cacheFactory.getWatermarkCache();
    byte[] serializedCacheFactory = serialize(cacheFactory);

    for (int i = 0; i < 10; i++) {
      CacheFactory deserializedCacheFactory = deserialize(serializedCacheFactory);

      assertEquals(cacheId, deserializedCacheFactory.getCacheId());
      assertEquals(watermarkCache, deserializedCacheFactory.getWatermarkCache());
    }
  }

  @Test
  public void testMultipleInstancesHaveDifferentCacheIds() throws Exception {
    CacheFactory cacheFactory1 = new CacheFactory(daoFactory, Duration.standardSeconds(5));
    long cacheId1 = cacheFactory1.getCacheId();
    CacheFactory cacheFactory2 = new CacheFactory(daoFactory, Duration.standardSeconds(5));
    long cacheId2 = cacheFactory2.getCacheId();

    CacheFactory deserializedCacheFactory1 = deserialize(serialize(cacheFactory1));
    CacheFactory deserializedCacheFactory2 = deserialize(serialize(cacheFactory2));

    assertNotEquals(cacheId1, cacheId2);
    assertEquals(cacheId1, deserializedCacheFactory1.getCacheId());
    assertEquals(cacheId2, deserializedCacheFactory2.getCacheId());
  }

  private byte[] serialize(CacheFactory cacheFactory) throws IOException {
    try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(baos)) {
      oos.writeObject(cacheFactory);
      return baos.toByteArray();
    }
  }

  private CacheFactory deserialize(byte[] bytes) throws IOException, ClassNotFoundException {
    try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais)) {
      return (CacheFactory) ois.readObject();
    }
  }
}
