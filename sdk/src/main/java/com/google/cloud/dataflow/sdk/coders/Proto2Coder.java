/*
 * Copyright (C) 2015 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.google.cloud.dataflow.sdk.coders;

import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

/**
 * An encoder using Google Protocol Buffers 2 binary format.
 * <p>
 * To learn more about Protocol Buffers, visit:
 * <a href="https://developers.google.com/protocol-buffers">https://developers.google.com/protocol-buffers</a>
 * <p>
 * To use, specify the {@code Coder} type on a PCollection:
 * <pre>
 * {@code
 * PCollection<MyProto.Message> records =
 *     input.apply(...)
 *          .setCoder(Proto2Coder.of(MyProto.Message.class));
 * }
 * </pre>
 * <p>
 * Custom message extensions are also supported, but the coder must be made
 * aware of them explicitly:
 * <pre>
 * {@code
 * PCollection<MyProto.Message> records =
 *     input.apply(...)
 *          .setCoder(Proto2Coder.of(MyProto.Message.class)
 *              .addExtensionsFrom(MyProto.class));
 * }
 * </pre>
 *
 * @param <T> the type of elements handled by this coder, must extend {@code Message}
 */
public class Proto2Coder<T extends Message> extends CustomCoder<T> {
  private static final long serialVersionUID = 0;

  /**
   * Produces a new Proto2Coder instance, for a given Protobuf message class.
   */
  public static <T extends Message> Proto2Coder<T> of(Class<T> protoMessageClass) {
    return new Proto2Coder<>(protoMessageClass);
  }

  private final Class<?> protoMessageClass;
  private final List<Class<?>> extensionClassList = new ArrayList<>();
  private transient Parser<T> parser;
  private transient ExtensionRegistry extensionRegistry;

  Proto2Coder(Class<T> protoMessageClass) {
    this.protoMessageClass = protoMessageClass;
  }

  /**
   * Adds custom Protobuf extensions to the coder. Returns {@code this}
   * for method chaining.
   *
   * @param extensionHosts must be a class that defines a static
   *      method name {@code registerAllExtensions}
   */
  public Proto2Coder<T> addExtensionsFrom(Class<?>... extensionHosts) {
    for (Class<?> extensionHost : extensionHosts) {
      try {
        // Attempt to access the declared method, to make sure it's present.
        extensionHost
            .getDeclaredMethod("registerAllExtensions", ExtensionRegistry.class);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(e);
      }
      extensionClassList.add(extensionHost);
    }
    return this;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context) throws IOException {
    if (context.isWholeStream) {
      value.writeTo(outStream);
    } else {
      value.writeDelimitedTo(outStream);
    }
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    if (context.isWholeStream) {
      return (T) getParser().parseFrom(inStream, getExtensionRegistry());
    } else {
      return (T) getParser().parseDelimitedFrom(inStream, getExtensionRegistry());
    }
  }

  private Parser<T> getParser() {
    if (parser != null) {
      return parser;
    }
    try {
      @SuppressWarnings("unchecked")
      T protoMessageInstance = (T) protoMessageClass
          .getMethod("getDefaultInstance").invoke(null);
      @SuppressWarnings("unchecked")
      Parser<T> tParser = (Parser<T>) protoMessageInstance.getParserForType();
      parser = tParser;
    } catch (IllegalAccessException
        | InvocationTargetException
        | NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    }
    return parser;
  }

  private ExtensionRegistry getExtensionRegistry() {
    if (extensionRegistry != null) {
      return extensionRegistry;
    }
    extensionRegistry = ExtensionRegistry.newInstance();
    for (Class<?> extensionHost : extensionClassList) {
      try {
        extensionHost
            .getDeclaredMethod("registerAllExtensions", ExtensionRegistry.class)
            .invoke(null, extensionRegistry);
      } catch (IllegalAccessException
          | InvocationTargetException
          | NoSuchMethodException e) {
        throw new IllegalStateException(e);
      }
    }
    return extensionRegistry;
  }
}
