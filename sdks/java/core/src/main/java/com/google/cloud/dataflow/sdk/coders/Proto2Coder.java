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
package com.google.cloud.dataflow.sdk.coders;

import static com.google.common.base.Preconditions.checkArgument;

import com.google.cloud.dataflow.sdk.coders.protobuf.ProtoCoder;
import com.google.cloud.dataflow.sdk.util.CloudObject;
import com.google.cloud.dataflow.sdk.util.Structs;
import com.google.cloud.dataflow.sdk.values.TypeDescriptor;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import javax.annotation.Nullable;

/**
 * A {@link Coder} using Google Protocol Buffers 2 binary format.
 *
 * <p>To learn more about Protocol Buffers, visit:
 * <a href="https://developers.google.com/protocol-buffers">https://developers.google.com/protocol-buffers</a>
 *
 * <p>To use, specify the {@link Coder} type on a PCollection containing Protocol Buffers messages.
 *
 * <pre>
 * {@code
 * PCollection<MyProto.Message> records =
 *     input.apply(...)
 *          .setCoder(Proto2Coder.of(MyProto.Message.class));
 * }
 * </pre>
 *
 * <p>Custom message extensions are also supported, but the coder must be made
 * aware of them explicitly:
 *
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
 * @deprecated Use {@link ProtoCoder}.
 */
@Deprecated
public class Proto2Coder<T extends Message> extends AtomicCoder<T> {

  /** The class of Protobuf message to be encoded. */
  private final Class<T> protoMessageClass;

  /**
   * All extension host classes included in this Proto2Coder. The extensions from
   * these classes will be included in the {@link ExtensionRegistry} used during
   * encoding and decoding.
   */
  private final List<Class<?>> extensionHostClasses;

  private Proto2Coder(Class<T> protoMessageClass, List<Class<?>> extensionHostClasses) {
    this.protoMessageClass = protoMessageClass;
    this.extensionHostClasses = extensionHostClasses;
  }

  private static final CoderProvider PROVIDER =
      new CoderProvider() {
        @Override
        public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
          if (type.isSubtypeOf(new TypeDescriptor<Message>() {})) {
            @SuppressWarnings("unchecked")
            TypeDescriptor<? extends Message> messageType =
                (TypeDescriptor<? extends Message>) type;
            @SuppressWarnings("unchecked")
            Coder<T> coder = (Coder<T>) Proto2Coder.of(messageType);
            return coder;
          } else {
            throw new CannotProvideCoderException(
                String.format(
                    "Cannot provide Proto2Coder because %s "
                        + "is not a subclass of protocol buffer Messsage",
                    type));
          }
        }
      };

  public static CoderProvider coderProvider() {
    return PROVIDER;
  }

  /**
   * Returns a {@code Proto2Coder} for the given Protobuf message class.
   */
  public static <T extends Message> Proto2Coder<T> of(Class<T> protoMessageClass) {
    return new Proto2Coder<T>(protoMessageClass, Collections.<Class<?>>emptyList());
  }

  /**
   * Returns a {@code Proto2Coder} for the given Protobuf message class.
   */
  public static <T extends Message> Proto2Coder<T> of(TypeDescriptor<T> protoMessageType) {
    @SuppressWarnings("unchecked")
    Class<T> protoMessageClass = (Class<T>) protoMessageType.getRawType();
    return of(protoMessageClass);
  }

  /**
   * Produces a {@code Proto2Coder} like this one, but with the extensions from
   * the given classes registered.
   *
   * @param moreExtensionHosts an iterable of classes that define a static
   *      method {@code registerAllExtensions(ExtensionRegistry)}
   */
  public Proto2Coder<T> withExtensionsFrom(Iterable<Class<?>> moreExtensionHosts) {
    for (Class<?> extensionHost : moreExtensionHosts) {
      // Attempt to access the required method, to make sure it's present.
      try {
        Method registerAllExtensions =
            extensionHost.getDeclaredMethod("registerAllExtensions", ExtensionRegistry.class);
        checkArgument(
            Modifier.isStatic(registerAllExtensions.getModifiers()),
            "Method registerAllExtensions() must be static for use with Proto2Coder");
      } catch (NoSuchMethodException | SecurityException e) {
        throw new IllegalArgumentException(e);
      }
    }

    return new Proto2Coder<T>(
        protoMessageClass,
        new ImmutableList.Builder<Class<?>>()
            .addAll(extensionHostClasses)
            .addAll(moreExtensionHosts)
            .build());
  }

  /**
   * See {@link #withExtensionsFrom(Iterable)}.
   */
  public Proto2Coder<T> withExtensionsFrom(Class<?>... extensionHosts) {
    return withExtensionsFrom(ImmutableList.copyOf(extensionHosts));
  }

  /**
   * Adds custom Protobuf extensions to the coder. Returns {@code this}
   * for method chaining.
   *
   * @param extensionHosts must be a class that defines a static
   *      method name {@code registerAllExtensions}
   * @deprecated use {@link #withExtensionsFrom}
   */
  @Deprecated
  public Proto2Coder<T> addExtensionsFrom(Class<?>... extensionHosts) {
    return addExtensionsFrom(ImmutableList.copyOf(extensionHosts));
  }

  /**
   * Adds custom Protobuf extensions to the coder. Returns {@code this}
   * for method chaining.
   *
   * @param extensionHosts must be a class that defines a static
   *      method name {@code registerAllExtensions}
   * @deprecated use {@link #withExtensionsFrom}
   */
  @Deprecated
  public Proto2Coder<T> addExtensionsFrom(Iterable<Class<?>> extensionHosts) {
    for (Class<?> extensionHost : extensionHosts) {
      try {
        // Attempt to access the declared method, to make sure it's present.
        extensionHost.getDeclaredMethod("registerAllExtensions", ExtensionRegistry.class);
      } catch (NoSuchMethodException e) {
        throw new IllegalArgumentException(e);
      }
      extensionHostClasses.add(extensionHost);
    }
    // The memoized extension registry needs to be recomputed because we have mutated this object.
    synchronized (this) {
      memoizedExtensionRegistry = null;
      getExtensionRegistry();
    }
    return this;
  }

  @Override
  public void encode(T value, OutputStream outStream, Context context) throws IOException {
    if (value == null) {
      throw new CoderException("cannot encode a null " + protoMessageClass.getSimpleName());
    }
    if (context.isWholeStream) {
      value.writeTo(outStream);
    } else {
      value.writeDelimitedTo(outStream);
    }
  }

  @Override
  public T decode(InputStream inStream, Context context) throws IOException {
    if (context.isWholeStream) {
      return getParser().parseFrom(inStream, getExtensionRegistry());
    } else {
      return getParser().parseDelimitedFrom(inStream, getExtensionRegistry());
    }
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    if (!(other instanceof Proto2Coder)) {
      return false;
    }
    Proto2Coder<?> otherCoder = (Proto2Coder<?>) other;
    return protoMessageClass.equals(otherCoder.protoMessageClass)
        && Sets.newHashSet(extensionHostClasses)
            .equals(Sets.newHashSet(otherCoder.extensionHostClasses));
  }

  @Override
  public int hashCode() {
    return Objects.hash(protoMessageClass, extensionHostClasses);
  }

  /**
   * The encoding identifier is designed to support evolution as per the design of Protocol
   * Buffers. In order to use this class effectively, carefully follow the advice in the Protocol
   * Buffers documentation at
   * <a href="https://developers.google.com/protocol-buffers/docs/proto#updating">Updating
   * A Message Type</a>.
   *
   * <p>In particular, the encoding identifier is guaranteed to be the same for {@code Proto2Coder}
   * instances of the same principal message class, and otherwise distinct. Loaded extensions do not
   * affect the id, nor does it encode the full schema.
   *
   * <p>When modifying a message class, here are the broadest guidelines; see the above link
   * for greater detail.
   *
   * <ul>
   * <li>Do not change the numeric tags for any fields.
   * <li>Never remove a <code>required</code> field.
   * <li>Only add <code>optional</code> or <code>repeated</code> fields, with sensible defaults.
   * <li>When changing the type of a field, consult the Protocol Buffers documentation to ensure
   * the new and old types are interchangeable.
   * </ul>
   *
   * <p>Code consuming this message class should be prepared to support <i>all</i> versions of
   * the class until it is certain that no remaining serialized instances exist.
   *
   * <p>If backwards incompatible changes must be made, the best recourse is to change the name
   * of your Protocol Buffers message class.
   */
  @Override
  public String getEncodingId() {
    return protoMessageClass.getName();
  }

  private transient Parser<T> memoizedParser;

  private Parser<T> getParser() {
    if (memoizedParser == null) {
      try {
        @SuppressWarnings("unchecked")
        T protoMessageInstance = (T) protoMessageClass.getMethod("getDefaultInstance").invoke(null);
        @SuppressWarnings("unchecked")
        Parser<T> tParser = (Parser<T>) protoMessageInstance.getParserForType();
        memoizedParser = tParser;
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return memoizedParser;
  }

  private transient ExtensionRegistry memoizedExtensionRegistry;

  private synchronized ExtensionRegistry getExtensionRegistry() {
    if (memoizedExtensionRegistry == null) {
      ExtensionRegistry registry = ExtensionRegistry.newInstance();
      for (Class<?> extensionHost : extensionHostClasses) {
        try {
          extensionHost
              .getDeclaredMethod("registerAllExtensions", ExtensionRegistry.class)
              .invoke(null, registry);
        } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
          throw new IllegalStateException(e);
        }
      }
      memoizedExtensionRegistry = registry.getUnmodifiable();
    }
    return memoizedExtensionRegistry;
  }

  ////////////////////////////////////////////////////////////////////////////////////
  // JSON Serialization details below

  private static final String PROTO_MESSAGE_CLASS = "proto_message_class";
  private static final String PROTO_EXTENSION_HOSTS = "proto_extension_hosts";

  /**
   * Constructor for JSON deserialization only.
   */
  @JsonCreator
  public static <T extends Message> Proto2Coder<T> of(
      @JsonProperty(PROTO_MESSAGE_CLASS) String protoMessageClassName,
      @Nullable @JsonProperty(PROTO_EXTENSION_HOSTS) List<String> extensionHostClassNames) {

    try {
      @SuppressWarnings("unchecked")
      Class<T> protoMessageClass = (Class<T>) Class.forName(protoMessageClassName);
      List<Class<?>> extensionHostClasses = Lists.newArrayList();
      if (extensionHostClassNames != null) {
        for (String extensionHostClassName : extensionHostClassNames) {
          extensionHostClasses.add(Class.forName(extensionHostClassName));
        }
      }
      return of(protoMessageClass).withExtensionsFrom(extensionHostClasses);
    } catch (ClassNotFoundException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public CloudObject asCloudObject() {
    CloudObject result = super.asCloudObject();
    Structs.addString(result, PROTO_MESSAGE_CLASS, protoMessageClass.getName());
    List<CloudObject> extensionHostClassNames = Lists.newArrayList();
    for (Class<?> clazz : extensionHostClasses) {
      extensionHostClassNames.add(CloudObject.forString(clazz.getName()));
    }
    Structs.addList(result, PROTO_EXTENSION_HOSTS, extensionHostClassNames);
    return result;
  }
}
