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
package org.apache.beam.sdk.coders.protobuf;

import static com.google.common.base.Preconditions.checkArgument;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.AtomicCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.util.CloudObject;
import org.apache.beam.sdk.util.Structs;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A {@link Coder} using Google Protocol Buffers binary format. {@link ProtoCoder} supports both
 * Protocol Buffers syntax versions 2 and 3.
 *
 * <p>To learn more about Protocol Buffers, visit:
 * <a href="https://developers.google.com/protocol-buffers">https://developers.google.com/protocol-buffers</a>
 *
 * <p>{@link ProtoCoder} is registered in the global {@link CoderRegistry} as the default
 * {@link Coder} for any {@link Message} object. Custom message extensions are also supported, but
 * these extensions must be registered for a particular {@link ProtoCoder} instance and that
 * instance must be registered on the {@link PCollection} that needs the extensions:
 *
 * <pre>{@code
 * import MyProtoFile;
 * import MyProtoFile.MyMessage;
 *
 * Coder<MyMessage> coder = ProtoCoder.of(MyMessage.class).withExtensionsFrom(MyProtoFile.class);
 * PCollection<MyMessage> records =  input.apply(...).setCoder(coder);
 * }</pre>
 *
 * <h3>Versioning</h3>
 *
 * <p>{@link ProtoCoder} supports both versions 2 and 3 of the Protocol Buffers syntax. However,
 * the Java runtime version of the <code>google.com.protobuf</code> library must match exactly the
 * version of <code>protoc</code> that was used to produce the JAR files containing the compiled
 * <code>.proto</code> messages.
 *
 * <p>For more information, see the
 * <a href="https://developers.google.com/protocol-buffers/docs/proto3#using-proto2-message-types">Protocol Buffers documentation</a>.
 *
 * <h3>{@link ProtoCoder} and Determinism</h3>
 *
 * <p>In general, Protocol Buffers messages can be encoded deterministically within a single
 * pipeline as long as:
 *
 * <ul>
 * <li>The encoded messages (and any transitively linked messages) do not use <code>map</code>
 *     fields.</li>
 * <li>Every Java VM that encodes or decodes the messages use the same runtime version of the
 *     Protocol Buffers library and the same compiled <code>.proto</code> file JAR.</li>
 * </ul>
 *
 * <h3>{@link ProtoCoder} and Encoding Stability</h3>
 *
 * <p>When changing Protocol Buffers messages, follow the rules in the Protocol Buffers language
 * guides for
 * <a href="https://developers.google.com/protocol-buffers/docs/proto#updating">{@code proto2}</a>
 * and
 * <a href="https://developers.google.com/protocol-buffers/docs/proto3#updating">{@code proto3}</a>
 * syntaxes, depending on your message type. Following these guidelines will ensure that the
 * old encoded data can be read by new versions of the code.
 *
 * <p>Generally, any change to the message type, registered extensions, runtime library, or
 * compiled proto JARs may change the encoding. Thus even if both the original and updated messages
 * can be encoded deterministically within a single job, these deterministic encodings may not be
 * the same across jobs.
 *
 * @param <T> the Protocol Buffers {@link Message} handled by this {@link Coder}.
 */
public class ProtoCoder<T extends Message> extends AtomicCoder<T> {

  /**
   * A {@link CoderProvider} that returns a {@link ProtoCoder} with an empty
   * {@link ExtensionRegistry}.
   */
  public static CoderProvider coderProvider() {
    return PROVIDER;
  }

  /**
   * Returns a {@link ProtoCoder} for the given Protocol Buffers {@link Message}.
   */
  public static <T extends Message> ProtoCoder<T> of(Class<T> protoMessageClass) {
    return new ProtoCoder<>(protoMessageClass, ImmutableSet.<Class<?>>of());
  }

  /**
   * Returns a {@link ProtoCoder} for the Protocol Buffers {@link Message} indicated by the given
   * {@link TypeDescriptor}.
   */
  public static <T extends Message> ProtoCoder<T> of(TypeDescriptor<T> protoMessageType) {
    @SuppressWarnings("unchecked")
    Class<T> protoMessageClass = (Class<T>) protoMessageType.getRawType();
    return of(protoMessageClass);
  }

  /**
   * Returns a {@link ProtoCoder} like this one, but with the extensions from the given classes
   * registered.
   *
   * <p>Each of the extension host classes must be an class automatically generated by the
   * Protocol Buffers compiler, {@code protoc}, that contains messages.
   *
   * <p>Does not modify this object.
   */
  public ProtoCoder<T> withExtensionsFrom(Iterable<Class<?>> moreExtensionHosts) {
    for (Class<?> extensionHost : moreExtensionHosts) {
      // Attempt to access the required method, to make sure it's present.
      try {
        Method registerAllExtensions =
            extensionHost.getDeclaredMethod("registerAllExtensions", ExtensionRegistry.class);
        checkArgument(
            Modifier.isStatic(registerAllExtensions.getModifiers()),
            "Method registerAllExtensions() must be static");
      } catch (NoSuchMethodException | SecurityException e) {
        throw new IllegalArgumentException(
            String.format("Unable to register extensions for %s", extensionHost.getCanonicalName()),
            e);
      }
    }

    return new ProtoCoder<>(
        protoMessageClass,
        new ImmutableSet.Builder<Class<?>>()
            .addAll(extensionHostClasses)
            .addAll(moreExtensionHosts)
            .build());
  }

  /**
   * See {@link #withExtensionsFrom(Iterable)}.
   *
   * <p>Does not modify this object.
   */
  public ProtoCoder<T> withExtensionsFrom(Class<?>... moreExtensionHosts) {
    return withExtensionsFrom(Arrays.asList(moreExtensionHosts));
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
    if (!(other instanceof ProtoCoder)) {
      return false;
    }
    ProtoCoder<?> otherCoder = (ProtoCoder<?>) other;
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
   * <p>In particular, the encoding identifier is guaranteed to be the same for {@link ProtoCoder}
   * instances of the same principal message class, with the same registered extension host classes,
   * and otherwise distinct. Note that the encoding ID does not encode any version of the message
   * or extensions, nor does it include the message schema.
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
    return protoMessageClass.getName() + getSortedExtensionClasses().toString();
  }

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    ProtobufUtil.verifyDeterministic(this);
  }

  /**
   * Returns the Protocol Buffers {@link Message} type this {@link ProtoCoder} supports.
   */
  public Class<T> getMessageType() {
    return protoMessageClass;
  }

  /**
   * Returns the {@link ExtensionRegistry} listing all known Protocol Buffers extension messages
   * to {@code T} registered with this {@link ProtoCoder}.
   */
  public ExtensionRegistry getExtensionRegistry() {
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
  // Private implementation details below.

  /** The {@link Message} type to be coded. */
  private final Class<T> protoMessageClass;

  /**
   * All extension host classes included in this {@link ProtoCoder}. The extensions from these
   * classes will be included in the {@link ExtensionRegistry} used during encoding and decoding.
   */
  private final Set<Class<?>> extensionHostClasses;

  // Constants used to serialize and deserialize
  private static final String PROTO_MESSAGE_CLASS = "proto_message_class";
  private static final String PROTO_EXTENSION_HOSTS = "proto_extension_hosts";

  // Transient fields that are lazy initialized and then memoized.
  private transient ExtensionRegistry memoizedExtensionRegistry;
  private transient Parser<T> memoizedParser;

  /** Private constructor. */
  private ProtoCoder(Class<T> protoMessageClass, Set<Class<?>> extensionHostClasses) {
    this.protoMessageClass = protoMessageClass;
    this.extensionHostClasses = extensionHostClasses;
  }

  /**
   * @deprecated For JSON deserialization only.
   */
  @JsonCreator
  @Deprecated
  public static <T extends Message> ProtoCoder<T> of(
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
  public CloudObject initializeCloudObject() {
    CloudObject result = CloudObject.forClass(getClass());
    Structs.addString(result, PROTO_MESSAGE_CLASS, protoMessageClass.getName());
    List<CloudObject> extensionHostClassNames = Lists.newArrayList();
    for (String className : getSortedExtensionClasses()) {
      extensionHostClassNames.add(CloudObject.forString(className));
    }
    Structs.addList(result, PROTO_EXTENSION_HOSTS, extensionHostClassNames);
    return result;
  }

  /** Get the memoized {@link Parser}, possibly initializing it lazily. */
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

  static final TypeDescriptor<Message> CHECK = new TypeDescriptor<Message>() {};

  /**
   * The implementation of the {@link CoderProvider} for this {@link ProtoCoder} returned by
   * {@link #coderProvider()}.
   */
  private static final CoderProvider PROVIDER =
      new CoderProvider() {
        @Override
        public <T> Coder<T> getCoder(TypeDescriptor<T> type) throws CannotProvideCoderException {
          if (!type.isSubtypeOf(CHECK)) {
            throw new CannotProvideCoderException(
                String.format(
                    "Cannot provide %s because %s is not a subclass of %s",
                    ProtoCoder.class.getSimpleName(),
                    type,
                    Message.class.getName()));
          }

          @SuppressWarnings("unchecked")
          TypeDescriptor<? extends Message> messageType = (TypeDescriptor<? extends Message>) type;
          try {
            @SuppressWarnings("unchecked")
            Coder<T> coder = (Coder<T>) ProtoCoder.of(messageType);
            return coder;
          } catch (IllegalArgumentException e) {
            throw new CannotProvideCoderException(e);
          }
        }
      };

  private SortedSet<String> getSortedExtensionClasses() {
    SortedSet<String> ret = new TreeSet<>();
    for (Class<?> clazz : extensionHostClasses) {
      ret.add(clazz.getName());
    }
    return ret;
  }
}
