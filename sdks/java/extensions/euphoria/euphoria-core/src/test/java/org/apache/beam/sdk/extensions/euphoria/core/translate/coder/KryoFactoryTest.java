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
package org.apache.beam.sdk.extensions.euphoria.core.translate.coder;

import com.esotericsoftware.kryo.Kryo;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Assert;
import org.junit.Test;

/** A set of unit {@link KryoFactory} tests. */
public class KryoFactoryTest {

  @Test
  public void testGiveTheSameKrioAfterKryoRegistrarDeserialized()
      throws IOException, ClassNotFoundException {

    IdentifiedRegistrar registrar = IdentifiedRegistrar.of((k) -> k.register(TestClass.class));

    Kryo firstKryo = KryoFactory.getOrCreateKryo(registrar);

    ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    ObjectOutputStream oss = new ObjectOutputStream(outStr);

    oss.writeObject(registrar);
    oss.flush();
    oss.close();

    ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(outStr.toByteArray()));

    @SuppressWarnings("unchecked")
    IdentifiedRegistrar deserializedRegistrar = (IdentifiedRegistrar) ois.readObject();

    Kryo secondKryo = KryoFactory.getOrCreateKryo(deserializedRegistrar);

    Assert.assertSame(firstKryo, secondKryo);
  }

  private static class TestClass {}
}
