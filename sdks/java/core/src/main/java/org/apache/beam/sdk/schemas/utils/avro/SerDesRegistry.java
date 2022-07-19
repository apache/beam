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

package org.apache.beam.sdk.schemas.utils.avro;

import static org.apache.beam.sdk.schemas.utils.avro.utils.SerDesUtils.getClassName;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.apache.avro.Schema;
import org.apache.beam.sdk.values.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Serializer and Deserializer Registry.
 */

public class SerDesRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(SerDesRegistry.class);

  private static volatile SerDesRegistry _INSTANCE;

  private final Optional<String> compileClassPath;
  private URLClassLoader classLoader;
  private File classesDir;
  private Path classesPath;

  /**
   * Registry ser/des generated code which can be used in serializers/deserializers.
   *
   * @param executorService Custom {@link Executor}
   */
  public SerDesRegistry(Executor executorService) {
    try {
      classesPath = Files.createTempDirectory("generated");
      classesDir = classesPath.toFile();

      classLoader = URLClassLoader.newInstance(new URL[]{classesDir.toURI().toURL()},
          SerDesRegistry.class.getClassLoader());

    } catch (Exception e) {
      LOGGER.warn("Got Error while constructing SerDesRegistry : "
          + Thread.currentThread().getName(), e);
      throw new RuntimeException(e);
    }

    this.compileClassPath = Optional.empty();
  }

  /**
   * Registry ser/des generated code which can be used in serializers/deserializers.
   */
  public SerDesRegistry() {
    this(null);
  }


  public RowDeserializer buildRowDeserializer(Schema writerSchema, Schema readerSchema)
      throws ClassNotFoundException, NoSuchMethodException, IOException, InvocationTargetException, InstantiationException,
      IllegalAccessException {
      String className = getClassName(writerSchema, readerSchema, "RowDeserializer");
      LOGGER.warn("Lets check is there previously generated java code by someone. className: "
          + className);

      Optional<Path> clazzFile = Optional.empty();
      try(Stream<Path> clazzFiles = Files.walk(classesDir.toPath()).filter(p -> p.getFileName().startsWith(className + ".class"))){
        clazzFile = clazzFiles.findFirst();
      }
      if (clazzFile.isPresent()) {
        LOGGER.warn("Loading className: " + SerDesBase.GENERATED_PACKAGE_NAME_PREFIX + className);
        Class<RowDeserializer<?>> rowDeserializerClass = (Class<RowDeserializer<?>>) classLoader
            .loadClass(
                SerDesBase.GENERATED_PACKAGE_NAME_PREFIX + className);
        return rowDeserializerClass.getConstructor(Schema.class).newInstance(readerSchema);
      }
        //We could not find anything. We need to the generate code of deserializer
        LOGGER.warn("We could not find anything. We need to the generate code of deserializer "
            + Thread.currentThread().getName());
        RowDeserializerCodeGenerator generator = new RowDeserializerCodeGenerator(writerSchema,
            readerSchema, classesDir, classLoader, compileClassPath.orElseGet(() -> null));
        RowDeserializer generatedDeserializer = generator.generateDeserializer();
        LOGGER.warn("We generated a deserializer " + Thread.currentThread().getName());
        return generatedDeserializer;
  }

  /**
   * Get a new {@link SerDesRegistry} object.
   *
   * @return {@link SerDesRegistry}
   */
  public static SerDesRegistry getDefaultInstance() {
    if (_INSTANCE == null) {
      synchronized (SerDesRegistry.class) {
        if (_INSTANCE == null) {
          LOGGER.warn("Creating SerDesRegistry" + Thread.currentThread().getName());
          _INSTANCE = new SerDesRegistry();
        }
      }
    }
    return _INSTANCE;
  }

}
