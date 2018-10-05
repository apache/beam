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
package org.apache.beam.sdk.extensions.kryo;

import static org.junit.Assert.assertSame;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import org.junit.Test;

/** A set of unit {@link KryoState} tests. */
public class KryoStateTest {

  @Test
  public void testSameKryoAfterDeserialization() throws IOException, ClassNotFoundException {
    final KryoCoder<?> coder = KryoCoder.of(k -> k.register(TestClass.class));
    final KryoState firstKryo = KryoState.get(coder);

    final ByteArrayOutputStream outStr = new ByteArrayOutputStream();
    final ObjectOutputStream oss = new ObjectOutputStream(outStr);
    oss.writeObject(coder);
    oss.flush();
    oss.close();

    final ObjectInputStream ois =
        new ObjectInputStream(new ByteArrayInputStream(outStr.toByteArray()));
    @SuppressWarnings("unchecked")
    final KryoCoder<?> deserializedCoder = (KryoCoder) ois.readObject();
    final KryoState secondKryo = KryoState.get(deserializedCoder);
    assertSame(firstKryo, secondKryo);
  }

  private static class TestClass {}
}
