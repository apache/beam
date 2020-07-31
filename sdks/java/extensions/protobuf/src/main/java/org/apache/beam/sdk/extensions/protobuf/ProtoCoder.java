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
package org.apache.beam.sdk.extensions.protobuf;

import static org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.DynamicMessage;
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
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.CoderProvider;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.coders.CustomCoder;
import org.apache.beam.sdk.coders.DefaultCoder;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.ImmutableSet;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Sets;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * A {@link Coder} using Google Protocol Buffers binary format. {@link ProtoCoder} supports both
 * Protocol Buffers syntax versions 2 and 3.
 *
 * <p>To learn more about Protocol Buffers, visit: <a
 * href="https://developers.google.com/protocol-buffers">https://developers.google.com/protocol-buffers</a>
 *
 * <p>{@link ProtoCoder} is registered in the global {@link CoderRegistry} as the default {@link
 * Coder} for any {@link Message} object. Custom message extensions are also supported, but these
 * extensions must be registered for a particular {@link ProtoCoder} instance and that instance must
 * be registered on the {@link PCollection} that needs the extensions:
 *
 * <pre>{@code
 * import MyProtoFile;
 * import MyProtoFile.MyMessage;
 *
 * Coder<MyMessage> coder = ProtoCoder.of(MyMessage.class).withExtensionsFrom(MyProtoFile.class);
 * PCollection<MyMessage> records = input.apply(...).setCoder(coder);
 * }</pre>
 *
 * <h3>Versioning</h3>
 *
 * <p>{@link ProtoCoder} supports both versions 2 and 3 of the Protocol Buffers syntax. However, the
 * Java runtime version of the <code>google.com.protobuf</code> library must match exactly the
 * version of <code>protoc</code> that was used to produce the JAR files containing the compiled
 * <code>.proto</code> messages.
 *
 * <p>For more information, see the <a
 * href="https://developers.google.com/protocol-buffers/docs/proto3#using-proto2-message-types">Protocol
 * Buffers documentation</a>.
 *
 * <h3>{@link ProtoCoder} and Determinism</h3>
 *
 * <p>In general, Protocol Buffers messages can be encoded deterministically within a single
 * pipeline as long as:
 *
 * <ul>
 *   <li>The encoded messages (and any transitively linked messages) do not use <code>map</code>
 *       fields.
 *   <li>Every Java VM that encodes or decodes the messages use the same runtime version of the
 *       Protocol Buffers library and the same compiled <code>.proto</code> file JAR.
 * </ul>
 *
 * <h3>{@link ProtoCoder} and Encoding Stability</h3>
 *
 * <p>When changing Protocol Buffers messages, follow the rules in the Protocol Buffers language
 * guides for <a href="https://developers.google.com/protocol-buffers/docs/proto#updating">{@code
 * proto2}</a> and <a
 * href="https://developers.google.com/protocol-buffers/docs/proto3#updating">{@code proto3}</a>
 * syntaxes, depending on your message type. Following these guidelines will ensure that the old
 * encoded data can be read by new versions of the code.
 *
 * <p>Generally, any change to the message type, registered extensions, runtime library, or compiled
 * proto JARs may change the encoding. Thus even if both the original and updated messages can be
 * encoded deterministically within a single job, these deterministic encodings may not be the same
 * across jobs.
 *
 * @param <T> the Protocol Buffers {@link Message} handled by this {@link Coder}.
 */
public class ProtoCoder<T extends Message> extends CustomCoder<T> {

  public static final long serialVersionUID = -5043999806040629525L;

  /** Returns a {@link ProtoCoder} for the given Protocol Buffers {@link Message}. */
  public static <T extends Message> ProtoCoder<T> of(Class<T> protoMessageClass) {
    return new ProtoCoder<>(protoMessageClass, ImmutableSet.of());
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
   * Validate that all extensionHosts are able to be registered.
   *
   * @param moreExtensionHosts
   */
  void validateExtensions(Iterable<Class<?>> moreExtensionHosts) {
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
  }

  /**
   * Returns a {@link ProtoCoder} like this one, but with the extensions from the given classes
   * registered.
   *
   * <p>Each of the extension host classes must be an class automatically generated by the Protocol
   * Buffers compiler, {@code protoc}, that contains messages.
   *
   * <p>Does not modify this object.
   */
  public ProtoCoder<T> withExtensionsFrom(Iterable<Class<?>> moreExtensionHosts) {
    validateExtensions(moreExtensionHosts);
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
  public void encode(T value, OutputStream outStream) throws IOException {
    encode(value, outStream, Context.NESTED);
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
  public T decode(InputStream inStream) throws IOException {
    return decode(inStream, Context.NESTED);
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
  public boolean equals(@Nullable Object other) {
    if (this == other) {
      return true;
    }
    if (other == null || getClass() != other.getClass()) {
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

  @Override
  public void verifyDeterministic() throws NonDeterministicException {
    ProtobufUtil.verifyDeterministic(this);
  }

  /** Returns the Protocol Buffers {@link Message} type this {@link ProtoCoder} supports. */
  public Class<T> getMessageType() {
    return protoMessageClass;
  }

  public Set<Class<?>> getExtensionHosts() {
    return extensionHostClasses;
  }

  /**
   * Returns the {@link ExtensionRegistry} listing all known Protocol Buffers extension messages to
   * {@code T} registered with this {@link ProtoCoder}.
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
  final Class<T> protoMessageClass;

  /**
   * All extension host classes included in this {@link ProtoCoder}. The extensions from these
   * classes will be included in the {@link ExtensionRegistry} used during encoding and decoding.
   */
  final Set<Class<?>> extensionHostClasses;

  // Constants used to serialize and deserialize
  private static final String PROTO_MESSAGE_CLASS = "proto_message_class";
  private static final String PROTO_EXTENSION_HOSTS = "proto_extension_hosts";

  // Transient fields that are lazy initialized and then memoized.
  private transient ExtensionRegistry memoizedExtensionRegistry;
  transient Parser<T> memoizedParser;

  /** Private constructor. */
  protected ProtoCoder(Class<T> protoMessageClass, Set<Class<?>> extensionHostClasses) {
    this.protoMessageClass = protoMessageClass;
    this.extensionHostClasses = extensionHostClasses;
  }

  /** Get the memoized {@link Parser}, possibly initializing it lazily. */
  protected Parser<T> getParser() {
    if (memoizedParser == null) {
      try {
        if (DynamicMessage.class.equals(protoMessageClass)) {
          throw new IllegalArgumentException(
              "DynamicMessage is not supported by the ProtoCoder, use the DynamicProtoCoder.");
        } else {
          @SuppressWarnings("unchecked")
          T protoMessageInstance =
              (T) protoMessageClass.getMethod("getDefaultInstance").invoke(null);
          @SuppressWarnings("unchecked")
          Parser<T> tParser = (Parser<T>) protoMessageInstance.getParserForType();
          memoizedParser = tParser;
        }
      } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
        throw new IllegalArgumentException(e);
      }
    }
    return memoizedParser;
  }

  /**
   * Returns a {@link CoderProvider} which uses the {@link ProtoCoder} for {@link Message proto
   * messages}.
   *
   * <p>This method is invoked reflectively from {@link DefaultCoder}.
   */
  public static CoderProvider getCoderProvider() {
    return new ProtoCoderProvider();
  }

  static final TypeDescriptor<Message> MESSAGE_TYPE = new TypeDescriptor<Message>() {};

  /** A {@link CoderProvider} for {@link Message proto messages}. */
  private static class ProtoCoderProvider extends CoderProvider {

    @Override
    public <T> Coder<T> coderFor(
        TypeDescriptor<T> typeDescriptor, List<? extends Coder<?>> componentCoders)
        throws CannotProvideCoderException {
      if (!typeDescriptor.isSubtypeOf(MESSAGE_TYPE)) {
        throw new CannotProvideCoderException(
            String.format(
                "Cannot provide %s because %s is not a subclass of %s",
                ProtoCoder.class.getSimpleName(), typeDescriptor, Message.class.getName()));
      }

      @SuppressWarnings("unchecked")
      TypeDescriptor<? extends Message> messageType =
          (TypeDescriptor<? extends Message>) typeDescriptor;
      try {
        @SuppressWarnings("unchecked")
        Coder<T> coder = (Coder<T>) ProtoCoder.of(messageType);
        return coder;
      } catch (IllegalArgumentException e) {
        throw new CannotProvideCoderException(e);
      }
    }
  }
}
