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

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor.Syntax;
import com.google.protobuf.Descriptors.GenericDescriptor;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.ExtensionRegistry.ExtensionInfo;
import com.google.protobuf.Message;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.Set;
import org.apache.beam.sdk.coders.Coder.NonDeterministicException;

/**
 * Utility functions for reflecting and analyzing Protocol Buffers classes.
 *
 * <p>Used by {@link ProtoCoder}, but in a separate file for testing and isolation.
 */
class ProtobufUtil {
  /**
   * Returns the {@link Descriptor} for the given Protocol Buffers {@link Message}.
   *
   * @throws IllegalArgumentException if there is an error in Java reflection.
   */
  static Descriptor getDescriptorForClass(Class<? extends Message> clazz) {
    try {
      return (Descriptor) clazz.getMethod("getDescriptor").invoke(null);
    } catch (IllegalAccessException | InvocationTargetException | NoSuchMethodException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Returns the {@link Descriptor} for the given Protocol Buffers {@link Message} as well as
   * every class it can include transitively.
   *
   * @throws IllegalArgumentException if there is an error in Java reflection.
   */
  static Set<Descriptor> getRecursiveDescriptorsForClass(
      Class<? extends Message> clazz, ExtensionRegistry registry) {
    Descriptor root = getDescriptorForClass(clazz);
    Set<Descriptor> descriptors = new HashSet<>();
    recursivelyAddDescriptors(root, descriptors, registry);
    return descriptors;
  }

  /**
   * Recursively walks the given {@link Message} class and verifies that every field or message
   * linked in uses the Protocol Buffers proto2 syntax.
   */
  static void checkProto2Syntax(Class<? extends Message> clazz, ExtensionRegistry registry) {
    for (GenericDescriptor d : getRecursiveDescriptorsForClass(clazz, registry)) {
      Syntax s = d.getFile().getSyntax();
      checkArgument(
          s == Syntax.PROTO2,
          "Message %s or one of its dependencies does not use proto2 syntax: %s in file %s",
          clazz.getName(),
          d.getFullName(),
          d.getFile().getName());
    }
  }

  /**
   * Recursively checks whether the specified class uses any Protocol Buffers fields that cannot
   * be deterministically encoded.
   *
   * @throws NonDeterministicException if the object cannot be encoded deterministically.
   */
  static void verifyDeterministic(ProtoCoder<?> coder) throws NonDeterministicException {
    Class<? extends Message> message = coder.getMessageType();
    ExtensionRegistry registry = coder.getExtensionRegistry();
    Set<Descriptor> descriptors = getRecursiveDescriptorsForClass(message, registry);
    for (Descriptor d : descriptors) {
      for (FieldDescriptor fd : d.getFields()) {
        // If there is a transitively reachable Protocol Buffers map field, then this object cannot
        // be encoded deterministically.
        if (fd.isMapField()) {
          String reason =
              String.format(
                  "Protocol Buffers message %s transitively includes Map field %s (from file %s)."
                      + " Maps cannot be deterministically encoded.",
                  message.getName(),
                  fd.getFullName(),
                  fd.getFile().getFullName());
          throw new NonDeterministicException(coder, reason);
        }
      }
    }
  }

  ////////////////////////////////////////////////////////////////////////////////////////////////
  // Disable construction of utility class
  private ProtobufUtil() {}

  private static void recursivelyAddDescriptors(
      Descriptor message, Set<Descriptor> descriptors, ExtensionRegistry registry) {
    if (descriptors.contains(message)) {
      return;
    }
    descriptors.add(message);

    for (FieldDescriptor f : message.getFields()) {
      recursivelyAddDescriptors(f, descriptors, registry);
    }
    for (FieldDescriptor f : message.getExtensions()) {
      recursivelyAddDescriptors(f, descriptors, registry);
    }
    for (ExtensionInfo info :
        registry.getAllImmutableExtensionsByExtendedType(message.getFullName())) {
      recursivelyAddDescriptors(info.descriptor, descriptors, registry);
    }
    for (ExtensionInfo info :
        registry.getAllMutableExtensionsByExtendedType(message.getFullName())) {
      recursivelyAddDescriptors(info.descriptor, descriptors, registry);
    }
  }

  private static void recursivelyAddDescriptors(
      FieldDescriptor field, Set<Descriptor> descriptors, ExtensionRegistry registry) {
    switch (field.getType()) {
      case BOOL:
      case BYTES:
      case DOUBLE:
      case ENUM:
      case FIXED32:
      case FIXED64:
      case FLOAT:
      case INT32:
      case INT64:
      case SFIXED32:
      case SFIXED64:
      case SINT32:
      case SINT64:
      case STRING:
      case UINT32:
      case UINT64:
        // Primitive types do not transitively access anything else.
        break;

      case GROUP:
      case MESSAGE:
        // Recursively adds all the fields from this nested Message.
        recursivelyAddDescriptors(field.getMessageType(), descriptors, registry);
        break;

      default:
        throw new UnsupportedOperationException(
            "Unexpected Protocol Buffers field type: " + field.getType());
    }
  }
}
