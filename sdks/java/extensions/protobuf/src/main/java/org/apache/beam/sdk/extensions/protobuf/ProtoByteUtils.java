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

import static java.util.stream.Collectors.toList;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkArgument;

import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.List;
import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.MatchResult;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.commons.compress.utils.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for working with Protocol Buffer (Proto) data in the context of Apache Beam. This
 * class provides methods to retrieve Beam Schemas from Proto messages, convert Proto bytes to Beam
 * Rows, and vice versa. It also includes utilities for handling Protocol Buffer schemas and related
 * file operations.
 *
 * <p>Users can utilize the methods in this class to facilitate the integration of Proto data
 * processing within Apache Beam pipelines, allowing for the seamless transformation of Proto
 * messages to Beam Rows and vice versa.
 */
public class ProtoByteUtils {

  private static final Logger LOG = LoggerFactory.getLogger(ProtoByteUtils.class);

  /**
   * Retrieves a Beam Schema from a Protocol Buffer message.
   *
   * @param fileDescriptorPath The path to the File Descriptor Set file.
   * @param messageName The name of the Protocol Buffer message.
   * @return The Beam Schema representing the Protocol Buffer message.
   */
  public static Schema getBeamSchemaFromProto(String fileDescriptorPath, String messageName) {
    ProtoSchemaInfo dpd = getProtoDomain(fileDescriptorPath);
    ProtoDomain protoDomain = dpd.getProtoDomain();
    return ProtoDynamicMessageSchema.forDescriptor(protoDomain, messageName).getSchema();
  }

  public static SerializableFunction<byte[], Row> getProtoBytesToRowFunction(
      String fileDescriptorPath, String messageName) {

    ProtoSchemaInfo dynamicProtoDomain = getProtoDomain(fileDescriptorPath);
    ProtoDomain protoDomain = dynamicProtoDomain.getProtoDomain();
    @SuppressWarnings("unchecked")
    ProtoDynamicMessageSchema<DynamicMessage> protoDynamicMessageSchema =
        ProtoDynamicMessageSchema.forDescriptor(protoDomain, messageName);
    return new SimpleFunction<byte[], Row>() {
      @Override
      public Row apply(byte[] input) {
        try {
          final Descriptors.Descriptor descriptor =
              protoDomain
                  .getFileDescriptor(dynamicProtoDomain.getFileName())
                  .findMessageTypeByName(messageName);
          DynamicMessage dynamicMessage = DynamicMessage.parseFrom(descriptor, input);
          SerializableFunction<DynamicMessage, Row> res =
              protoDynamicMessageSchema.getToRowFunction();
          return res.apply(dynamicMessage);
        } catch (InvalidProtocolBufferException e) {
          LOG.error("Error parsing to DynamicMessage", e);
          throw new RuntimeException(e);
        }
      }
    };
  }

  public static SerializableFunction<Row, byte[]> getRowToProtoBytes(
      String fileDescriptorPath, String messageName) {
    ProtoSchemaInfo dynamicProtoDomain = getProtoDomain(fileDescriptorPath);
    ProtoDomain protoDomain = dynamicProtoDomain.getProtoDomain();
    @SuppressWarnings("unchecked")
    ProtoDynamicMessageSchema<DynamicMessage> protoDynamicMessageSchema =
        ProtoDynamicMessageSchema.forDescriptor(protoDomain, messageName);

    return new SimpleFunction<Row, byte[]>() {
      @Override
      public byte[] apply(Row input) {
        SerializableFunction<Row, DynamicMessage> res =
            protoDynamicMessageSchema.getFromRowFunction();
        return res.apply(input).toByteArray();
      }
    };
  }

  /**
   * Retrieves a ProtoSchemaInfo containing schema information for the specified Protocol Buffer
   * file.
   *
   * @param fileDescriptorPath The path to the File Descriptor Set file.
   * @return ProtoSchemaInfo containing the associated ProtoDomain and File Name.
   * @throws RuntimeException if an error occurs during schema retrieval.
   */
  private static ProtoSchemaInfo getProtoDomain(String fileDescriptorPath) {
    byte[] from = getFileAsBytes(fileDescriptorPath);
    try {
      DescriptorProtos.FileDescriptorSet descriptorSet =
          DescriptorProtos.FileDescriptorSet.parseFrom(from);
      return new ProtoSchemaInfo(
          descriptorSet.getFile(0).getName(), ProtoDomain.buildFrom(descriptorSet));
    } catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Reads the contents of a file specified by its path and returns them as a byte array.
   *
   * @param fileDescriptorPath The path to the file to read.
   * @return Byte array containing the file contents.
   * @throws RuntimeException if an error occurs during file reading.
   */
  private static byte[] getFileAsBytes(String fileDescriptorPath) {
    ReadableByteChannel channel = getFileByteChannel(fileDescriptorPath);
    try (InputStream inputStream = Channels.newInputStream(channel)) {
      return IOUtils.toByteArray(inputStream);
    } catch (IOException e) {
      throw new RuntimeException("Error when reading: " + fileDescriptorPath, e);
    }
  }

  /**
   * Retrieves a ReadableByteChannel for a file specified by its path.
   *
   * @param filePath The path to the file to obtain a ReadableByteChannel for.
   * @return ReadableByteChannel for the specified file.
   * @throws RuntimeException if an error occurs while finding or opening the file.
   */
  private static ReadableByteChannel getFileByteChannel(String filePath) {
    try {
      MatchResult result = FileSystems.match(filePath);
      checkArgument(
          result.status() == MatchResult.Status.OK && !result.metadata().isEmpty(),
          "Failed to match any files with the pattern: " + filePath);

      List<ResourceId> rId =
          result.metadata().stream().map(MatchResult.Metadata::resourceId).collect(toList());

      checkArgument(rId.size() == 1, "Expected exactly 1 file, but got " + rId.size() + " files.");
      return FileSystems.open(rId.get(0));
    } catch (IOException e) {
      throw new RuntimeException("Error when finding: " + filePath, e);
    }
  }

  /**
   * Represents metadata associated with a Protocol Buffer schema, including the File Name and
   * ProtoDomain.
   */
  static class ProtoSchemaInfo implements Serializable {
    private String fileName;
    private ProtoDomain protoDomain;

    /**
     * Constructs a ProtoSchemaInfo with the specified File Name and ProtoDomain.
     *
     * @param fileName The name of the associated Protocol Buffer file.
     * @param protoDomain The ProtoDomain containing schema information.
     */
    public ProtoSchemaInfo(String fileName, ProtoDomain protoDomain) {
      this.fileName = fileName;
      this.protoDomain = protoDomain;
    }

    /**
     * Sets the ProtoDomain associated with this ProtoSchemaInfo.
     *
     * @param protoDomain The ProtoDomain to set.
     */
    @SuppressWarnings("unused")
    public void setProtoDomain(ProtoDomain protoDomain) {
      this.protoDomain = protoDomain;
    }

    /**
     * Gets the ProtoDomain associated with this ProtoSchemaInfo.
     *
     * @return The ProtoDomain containing schema information.
     */
    public ProtoDomain getProtoDomain() {
      return protoDomain;
    }

    /**
     * Gets the File Name associated with this ProtoSchemaInfo.
     *
     * @return The name of the associated Protocol Buffer file.
     */
    public String getFileName() {
      return fileName;
    }

    /**
     * Sets the File Name associated with this ProtoSchemaInfo.
     *
     * @param fileName The name of the Protocol Buffer file to set.
     */
    public void setFileName(String fileName) {
      this.fileName = fileName;
    }
  }
}
