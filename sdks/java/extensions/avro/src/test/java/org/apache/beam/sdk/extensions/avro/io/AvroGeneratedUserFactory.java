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
package org.apache.beam.sdk.extensions.avro.io;

import java.lang.reflect.Constructor;
import org.apache.avro.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Create a {@link AvroGeneratedUser} instance with different constructors. */
public class AvroGeneratedUserFactory {
  private static final Logger LOG = LoggerFactory.getLogger(AvroGeneratedUserFactory.class);
  private static final String VERSION_AVRO = Schema.class.getPackage().getImplementationVersion();

  public static AvroGeneratedUser newInstance(
      String name, Integer favoriteNumber, String favoriteColor) {

    if (VERSION_AVRO.equals("1.8.2")) {
      return new AvroGeneratedUser(name, favoriteNumber, favoriteColor);
    } else {
      try {
        Constructor<?> constructor;
        constructor =
            AvroGeneratedUser.class.getDeclaredConstructor(
                CharSequence.class, Integer.class, CharSequence.class);

        return (AvroGeneratedUser) constructor.newInstance(name, favoriteNumber, favoriteColor);
      } catch (ReflectiveOperationException e) {
        LOG.error(String.format("Fail to create a AvroGeneratedUser instance: %s", e.getMessage()));
        return new AvroGeneratedUser(); // return an empty instance to fail the tests
      }
    }
  }
}
