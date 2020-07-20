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

import com.google.protobuf.Any;
import com.google.protobuf.Api;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.Duration;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.Empty;
import com.google.protobuf.ExtensionRegistry;
import com.google.protobuf.FieldMask;
import com.google.protobuf.Int32Value;
import com.google.protobuf.SourceContext;
import com.google.protobuf.Struct;
import com.google.protobuf.Timestamp;
import com.google.protobuf.Type;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * ProtoDomain is a container class for Protobuf descriptors. By using a domain for all descriptors
 * that are related to each other the FileDescriptorSet needs to be serialized only once in the
 * graph.
 *
 * <p>Using a domain also grantees that all Descriptors have object equality, just like statically
 * compiled Proto classes Descriptors. A lot of Java code isn't used to the new DynamicMessages an
 * assume always Object equality. Because of this the domain class is immutable.
 *
 * <p>ProtoDomains aren't assumed to be used on with normal Message descriptors, only with
 * DynamicMessage descriptors.
 */
public final class ProtoDomain implements Serializable {
  public static final long serialVersionUID = 1L;
  private transient DescriptorProtos.FileDescriptorSet fileDescriptorSet;
  private transient int hashCode;

  private transient Map<String, Descriptors.FileDescriptor> fileDescriptorMap;
  private transient Map<String, Descriptors.Descriptor> descriptorMap;

  private transient Map<Integer, Descriptors.FieldDescriptor> fileOptionMap;
  private transient Map<Integer, Descriptors.FieldDescriptor> messageOptionMap;
  private transient Map<Integer, Descriptors.FieldDescriptor> fieldOptionMap;

  ProtoDomain() {
    this(DescriptorProtos.FileDescriptorSet.newBuilder().build());
  }

  private ProtoDomain(DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    this.fileDescriptorSet = fileDescriptorSet;
    hashCode = java.util.Arrays.hashCode(this.fileDescriptorSet.toByteArray());
    crosswire();
  }

  private static Map<String, DescriptorProtos.FileDescriptorProto> extractProtoMap(
      DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    HashMap<String, DescriptorProtos.FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().forEach(fdp -> map.put(fdp.getName(), fdp));
    return map;
  }

  private static Descriptors.@Nullable FileDescriptor convertToFileDescriptorMap(
      String name,
      Map<String, DescriptorProtos.FileDescriptorProto> inMap,
      Map<String, Descriptors.FileDescriptor> outMap,
      ExtensionRegistry registry) {
    if (outMap.containsKey(name)) {
      return outMap.get(name);
    }
    DescriptorProtos.FileDescriptorProto fileDescriptorProto = inMap.get(name);
    if (fileDescriptorProto == null) {
      Descriptors.FileDescriptor fd;
      switch (name) {
        case "google/protobuf/descriptor.proto":
          fd = DescriptorProtos.FieldOptions.getDescriptor().getFile();
          break;
        case "google/protobuf/wrappers.proto":
          fd = Int32Value.getDescriptor().getFile();
          break;
        case "google/protobuf/timestamp.proto":
          fd = Timestamp.getDescriptor().getFile();
          break;
        case "google/protobuf/duration.proto":
          fd = Duration.getDescriptor().getFile();
          break;
        case "google/protobuf/any.proto":
          fd = Any.getDescriptor().getFile();
          break;
        case "google/protobuf/api.proto":
          fd = Api.getDescriptor().getFile();
          break;
        case "google/protobuf/empty.proto":
          fd = Empty.getDescriptor().getFile();
          break;
        case "google/protobuf/field_mask.proto":
          fd = FieldMask.getDescriptor().getFile();
          break;
        case "google/protobuf/source_context.proto":
          fd = SourceContext.getDescriptor().getFile();
          break;
        case "google/protobuf/struct.proto":
          fd = Struct.getDescriptor().getFile();
          break;
        case "google/protobuf/type.proto":
          fd = Type.getDescriptor().getFile();
          break;
        default:
          return null;
      }
      outMap.put(name, fd);
      return fd;
    } else {
      List<Descriptors.FileDescriptor> dependencies = new ArrayList<>();
      if (fileDescriptorProto.getDependencyCount() > 0) {
        fileDescriptorProto
            .getDependencyList()
            .forEach(
                dependencyName -> {
                  Descriptors.FileDescriptor fileDescriptor =
                      convertToFileDescriptorMap(dependencyName, inMap, outMap, registry);
                  if (fileDescriptor != null) {
                    dependencies.add(fileDescriptor);
                  }
                });
      }
      try {
        Descriptors.FileDescriptor fileDescriptor =
            Descriptors.FileDescriptor.buildFrom(
                fileDescriptorProto, dependencies.toArray(new Descriptors.FileDescriptor[0]));
        fileDescriptor
            .getExtensions()
            .forEach(
                extension -> {
                  if (extension.getType() == Descriptors.FieldDescriptor.Type.MESSAGE) {
                    registry.add(
                        extension, DynamicMessage.newBuilder(extension.getMessageType()).build());
                  } else {
                    registry.add(extension);
                  }
                });
        Descriptors.FileDescriptor.internalUpdateFileDescriptor(fileDescriptor, registry);
        outMap.put(name, fileDescriptor);
        return fileDescriptor;
      } catch (Descriptors.DescriptorValidationException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private static void visitFileDescriptorTree(Map map, Descriptors.FileDescriptor fileDescriptor) {
    if (!map.containsKey(fileDescriptor.getName())) {
      map.put(fileDescriptor.getName(), fileDescriptor);
      List<Descriptors.FileDescriptor> dependencies = fileDescriptor.getDependencies();
      dependencies.forEach(fd -> visitFileDescriptorTree(map, fd));
    }
  }

  public static ProtoDomain buildFrom(Descriptors.Descriptor descriptor) {
    return buildFrom(descriptor.getFile());
  }

  public static ProtoDomain buildFrom(DescriptorProtos.FileDescriptorSet fileDescriptorSet) {
    return new ProtoDomain(fileDescriptorSet);
  }

  public static ProtoDomain buildFrom(Descriptors.FileDescriptor fileDescriptor) {
    HashMap<String, Descriptors.FileDescriptor> fileDescriptorMap = new HashMap<>();
    visitFileDescriptorTree(fileDescriptorMap, fileDescriptor);
    DescriptorProtos.FileDescriptorSet.Builder builder =
        DescriptorProtos.FileDescriptorSet.newBuilder();
    fileDescriptorMap.values().forEach(fd -> builder.addFile(fd.toProto()));
    return new ProtoDomain(builder.build());
  }

  public static ProtoDomain buildFrom(InputStream inputStream) throws IOException {
    return buildFrom(DescriptorProtos.FileDescriptorSet.parseFrom(inputStream));
  }

  private void crosswire() {
    HashMap<String, DescriptorProtos.FileDescriptorProto> map = new HashMap<>();
    fileDescriptorSet.getFileList().stream()
        .filter(fdp -> !fdp.getName().startsWith("google/protobuf"))
        .forEach(fdp -> map.put(fdp.getName(), fdp));

    ExtensionRegistry extensionRegistry = ExtensionRegistry.newInstance();
    Map<String, Descriptors.FileDescriptor> outMap = new HashMap<>();
    map.forEach(
        (fileName, proto) -> convertToFileDescriptorMap(fileName, map, outMap, extensionRegistry));
    fileDescriptorMap = outMap;

    indexOptionsByNumber(fileDescriptorMap.values());
    indexDescriptorByName();
  }

  private void indexDescriptorByName() {
    descriptorMap = new HashMap<>();
    fileDescriptorMap
        .values()
        .forEach(
            fileDescriptor -> {
              fileDescriptor.getMessageTypes().forEach(this::indexDescriptor);
            });
  }

  private void indexDescriptor(Descriptors.Descriptor descriptor) {
    descriptorMap.put(descriptor.getFullName(), descriptor);
    descriptor.getNestedTypes().forEach(this::indexDescriptor);
  }

  private void indexOptionsByNumber(Collection<Descriptors.FileDescriptor> fileDescriptors) {
    fieldOptionMap = new HashMap<>();
    fileOptionMap = new HashMap<>();
    messageOptionMap = new HashMap<>();
    fileDescriptors.forEach(
        (fileDescriptor) -> {
          fileDescriptor
              .getExtensions()
              .forEach(
                  extension -> {
                    switch (extension.toProto().getExtendee()) {
                      case ".google.protobuf.FileOptions":
                        fileOptionMap.put(extension.getNumber(), extension);
                        break;
                      case ".google.protobuf.MessageOptions":
                        messageOptionMap.put(extension.getNumber(), extension);
                        break;
                      case ".google.protobuf.FieldOptions":
                        fieldOptionMap.put(extension.getNumber(), extension);
                        break;
                      default:
                        break;
                    }
                  });
        });
  }

  private void writeObject(ObjectOutputStream oos) throws IOException {
    byte[] buffer = fileDescriptorSet.toByteArray();
    oos.writeInt(buffer.length);
    oos.write(buffer);
  }

  private void readObject(ObjectInputStream ois) throws IOException {
    byte[] buffer = new byte[ois.readInt()];
    ois.readFully(buffer);
    fileDescriptorSet = DescriptorProtos.FileDescriptorSet.parseFrom(buffer);
    hashCode = java.util.Arrays.hashCode(buffer);
    crosswire();
  }

  public Descriptors.FileDescriptor getFileDescriptor(String name) {
    return fileDescriptorMap.get(name);
  }

  public Descriptors.Descriptor getDescriptor(String fullName) {
    return descriptorMap.get(fullName);
  }

  public Descriptors.FieldDescriptor getFieldOptionById(int id) {
    return fieldOptionMap.get(id);
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    ProtoDomain that = (ProtoDomain) o;
    return hashCode == that.hashCode;
  }

  @Override
  public int hashCode() {
    return Objects.hash(hashCode);
  }

  public boolean contains(Descriptors.Descriptor descriptor) {
    return getDescriptor(descriptor.getFullName()) != null;
  }
}
