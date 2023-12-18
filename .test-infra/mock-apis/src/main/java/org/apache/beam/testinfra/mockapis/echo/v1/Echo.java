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
package org.apache.beam.testinfra.mockapis.echo.v1;

@SuppressWarnings({
  "argument",
  "assignment",
  "initialization.fields.uninitialized",
  "initialization.static.field.uninitialized",
  "override.param",
  "ClassTypeParameterName",
  "ForbidNonVendoredGuava",
  "JavadocStyle",
  "LocalVariableName",
  "MemberName",
  "NeedBraces",
  "MissingOverride",
  "RedundantModifier",
  "ReferenceEquality",
  "UnusedVariable",
})
public final class Echo {
  private Echo() {}

  public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {}

  public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
  }

  public interface EchoRequestOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.echo.v1.EchoRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The id.
     */
    java.lang.String getId();
    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The bytes for id.
     */
    com.google.protobuf.ByteString getIdBytes();

    /**
     * <code>bytes payload = 2 [json_name = "payload"];</code>
     *
     * @return The payload.
     */
    com.google.protobuf.ByteString getPayload();
  }
  /**
   *
   *
   * <pre>
   * The request to echo a payload.
   * </pre>
   *
   * Protobuf type {@code proto.echo.v1.EchoRequest}
   */
  public static final class EchoRequest extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.echo.v1.EchoRequest)
      EchoRequestOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use EchoRequest.newBuilder() to construct.
    private EchoRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private EchoRequest() {
      id_ = "";
      payload_ = com.google.protobuf.ByteString.EMPTY;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new EchoRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.mockapis.echo.v1.Echo
          .internal_static_proto_echo_v1_EchoRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.mockapis.echo.v1.Echo
          .internal_static_proto_echo_v1_EchoRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.class,
              org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.Builder.class);
    }

    public static final int ID_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private volatile java.lang.Object id_ = "";
    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The id.
     */
    @java.lang.Override
    public java.lang.String getId() {
      java.lang.Object ref = id_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        id_ = s;
        return s;
      }
    }
    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The bytes for id.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString getIdBytes() {
      java.lang.Object ref = id_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b =
            com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
        id_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int PAYLOAD_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString payload_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>bytes payload = 2 [json_name = "payload"];</code>
     *
     * @return The payload.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString getPayload() {
      return payload_;
    }

    private byte memoizedIsInitialized = -1;

    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output) throws java.io.IOException {
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(id_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, id_);
      }
      if (!payload_.isEmpty()) {
        output.writeBytes(2, payload_);
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(id_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, id_);
      }
      if (!payload_.isEmpty()) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(2, payload_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest other =
          (org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest) obj;

      if (!getId().equals(other.getId())) return false;
      if (!getPayload().equals(other.getPayload())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + ID_FIELD_NUMBER;
      hash = (53 * hash) + getId().hashCode();
      hash = (37 * hash) + PAYLOAD_FIELD_NUMBER;
      hash = (53 * hash) + getPayload().hashCode();
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        java.nio.ByteBuffer data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(byte[] data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseDelimitedFrom(
        java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseDelimitedFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     *
     *
     * <pre>
     * The request to echo a payload.
     * </pre>
     *
     * Protobuf type {@code proto.echo.v1.EchoRequest}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.echo.v1.EchoRequest)
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo
            .internal_static_proto_echo_v1_EchoRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo
            .internal_static_proto_echo_v1_EchoRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.class,
                org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.Builder.class);
      }

      // Construct using org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = "";
        payload_ = com.google.protobuf.ByteString.EMPTY;
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo
            .internal_static_proto_echo_v1_EchoRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest build() {
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest buildPartial() {
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest result =
            new org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.payload_ = payload_;
        }
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }

      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field, java.lang.Object value) {
        return super.setField(field, value);
      }

      @java.lang.Override
      public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }

      @java.lang.Override
      public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }

      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index,
          java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }

      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field, java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }

      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest) {
          return mergeFrom((org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest other) {
        if (other
            == org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest.getDefaultInstance())
          return this;
        if (!other.getId().isEmpty()) {
          id_ = other.id_;
          bitField0_ |= 0x00000001;
          onChanged();
        }
        if (other.getPayload() != com.google.protobuf.ByteString.EMPTY) {
          setPayload(other.getPayload());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new java.lang.NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10:
                {
                  id_ = input.readStringRequireUtf8();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
              case 18:
                {
                  payload_ = input.readBytes();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 18
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private java.lang.Object id_ = "";
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @return The id.
       */
      public java.lang.String getId() {
        java.lang.Object ref = id_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          id_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @return The bytes for id.
       */
      public com.google.protobuf.ByteString getIdBytes() {
        java.lang.Object ref = id_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b =
              com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
          id_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(java.lang.String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        id_ = getDefaultInstance().getId();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @param value The bytes for id to set.
       * @return This builder for chaining.
       */
      public Builder setIdBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        checkByteStringIsUtf8(value);
        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString payload_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>bytes payload = 2 [json_name = "payload"];</code>
       *
       * @return The payload.
       */
      @java.lang.Override
      public com.google.protobuf.ByteString getPayload() {
        return payload_;
      }
      /**
       * <code>bytes payload = 2 [json_name = "payload"];</code>
       *
       * @param value The payload to set.
       * @return This builder for chaining.
       */
      public Builder setPayload(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        payload_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       * <code>bytes payload = 2 [json_name = "payload"];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearPayload() {
        bitField0_ = (bitField0_ & ~0x00000002);
        payload_ = getDefaultInstance().getPayload();
        onChanged();
        return this;
      }

      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }

      // @@protoc_insertion_point(builder_scope:proto.echo.v1.EchoRequest)
    }

    // @@protoc_insertion_point(class_scope:proto.echo.v1.EchoRequest)
    private static final org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest();
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<EchoRequest> PARSER =
        new com.google.protobuf.AbstractParser<EchoRequest>() {
          @java.lang.Override
          public EchoRequest parsePartialFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws com.google.protobuf.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (com.google.protobuf.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new com.google.protobuf.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static com.google.protobuf.Parser<EchoRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<EchoRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoRequest getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface EchoResponseOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.echo.v1.EchoResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The id.
     */
    java.lang.String getId();
    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The bytes for id.
     */
    com.google.protobuf.ByteString getIdBytes();

    /**
     * <code>bytes payload = 2 [json_name = "payload"];</code>
     *
     * @return The payload.
     */
    com.google.protobuf.ByteString getPayload();
  }
  /**
   *
   *
   * <pre>
   * The response echo of a request payload.
   * </pre>
   *
   * Protobuf type {@code proto.echo.v1.EchoResponse}
   */
  public static final class EchoResponse extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.echo.v1.EchoResponse)
      EchoResponseOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use EchoResponse.newBuilder() to construct.
    private EchoResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private EchoResponse() {
      id_ = "";
      payload_ = com.google.protobuf.ByteString.EMPTY;
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new EchoResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.mockapis.echo.v1.Echo
          .internal_static_proto_echo_v1_EchoResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.mockapis.echo.v1.Echo
          .internal_static_proto_echo_v1_EchoResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.class,
              org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.Builder.class);
    }

    public static final int ID_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private volatile java.lang.Object id_ = "";
    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The id.
     */
    @java.lang.Override
    public java.lang.String getId() {
      java.lang.Object ref = id_;
      if (ref instanceof java.lang.String) {
        return (java.lang.String) ref;
      } else {
        com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
        java.lang.String s = bs.toStringUtf8();
        id_ = s;
        return s;
      }
    }
    /**
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The bytes for id.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString getIdBytes() {
      java.lang.Object ref = id_;
      if (ref instanceof java.lang.String) {
        com.google.protobuf.ByteString b =
            com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
        id_ = b;
        return b;
      } else {
        return (com.google.protobuf.ByteString) ref;
      }
    }

    public static final int PAYLOAD_FIELD_NUMBER = 2;
    private com.google.protobuf.ByteString payload_ = com.google.protobuf.ByteString.EMPTY;
    /**
     * <code>bytes payload = 2 [json_name = "payload"];</code>
     *
     * @return The payload.
     */
    @java.lang.Override
    public com.google.protobuf.ByteString getPayload() {
      return payload_;
    }

    private byte memoizedIsInitialized = -1;

    @java.lang.Override
    public final boolean isInitialized() {
      byte isInitialized = memoizedIsInitialized;
      if (isInitialized == 1) return true;
      if (isInitialized == 0) return false;

      memoizedIsInitialized = 1;
      return true;
    }

    @java.lang.Override
    public void writeTo(com.google.protobuf.CodedOutputStream output) throws java.io.IOException {
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(id_)) {
        com.google.protobuf.GeneratedMessageV3.writeString(output, 1, id_);
      }
      if (!payload_.isEmpty()) {
        output.writeBytes(2, payload_);
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (!com.google.protobuf.GeneratedMessageV3.isStringEmpty(id_)) {
        size += com.google.protobuf.GeneratedMessageV3.computeStringSize(1, id_);
      }
      if (!payload_.isEmpty()) {
        size += com.google.protobuf.CodedOutputStream.computeBytesSize(2, payload_);
      }
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj instanceof org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse other =
          (org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse) obj;

      if (!getId().equals(other.getId())) return false;
      if (!getPayload().equals(other.getPayload())) return false;
      if (!getUnknownFields().equals(other.getUnknownFields())) return false;
      return true;
    }

    @java.lang.Override
    public int hashCode() {
      if (memoizedHashCode != 0) {
        return memoizedHashCode;
      }
      int hash = 41;
      hash = (19 * hash) + getDescriptor().hashCode();
      hash = (37 * hash) + ID_FIELD_NUMBER;
      hash = (53 * hash) + getId().hashCode();
      hash = (37 * hash) + PAYLOAD_FIELD_NUMBER;
      hash = (53 * hash) + getPayload().hashCode();
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        java.nio.ByteBuffer data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        com.google.protobuf.ByteString data)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        com.google.protobuf.ByteString data,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseDelimitedFrom(
        java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseDelimitedFrom(
        java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse parseFrom(
        com.google.protobuf.CodedInputStream input,
        com.google.protobuf.ExtensionRegistryLite extensionRegistry)
        throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    @java.lang.Override
    public Builder newBuilderForType() {
      return newBuilder();
    }

    public static Builder newBuilder() {
      return DEFAULT_INSTANCE.toBuilder();
    }

    public static Builder newBuilder(
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse prototype) {
      return DEFAULT_INSTANCE.toBuilder().mergeFrom(prototype);
    }

    @java.lang.Override
    public Builder toBuilder() {
      return this == DEFAULT_INSTANCE ? new Builder() : new Builder().mergeFrom(this);
    }

    @java.lang.Override
    protected Builder newBuilderForType(
        com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
      Builder builder = new Builder(parent);
      return builder;
    }
    /**
     *
     *
     * <pre>
     * The response echo of a request payload.
     * </pre>
     *
     * Protobuf type {@code proto.echo.v1.EchoResponse}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.echo.v1.EchoResponse)
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo
            .internal_static_proto_echo_v1_EchoResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo
            .internal_static_proto_echo_v1_EchoResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.class,
                org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.Builder.class);
      }

      // Construct using org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = "";
        payload_ = com.google.protobuf.ByteString.EMPTY;
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo
            .internal_static_proto_echo_v1_EchoResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse build() {
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse result = buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse buildPartial() {
        org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse result =
            new org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.payload_ = payload_;
        }
      }

      @java.lang.Override
      public Builder clone() {
        return super.clone();
      }

      @java.lang.Override
      public Builder setField(
          com.google.protobuf.Descriptors.FieldDescriptor field, java.lang.Object value) {
        return super.setField(field, value);
      }

      @java.lang.Override
      public Builder clearField(com.google.protobuf.Descriptors.FieldDescriptor field) {
        return super.clearField(field);
      }

      @java.lang.Override
      public Builder clearOneof(com.google.protobuf.Descriptors.OneofDescriptor oneof) {
        return super.clearOneof(oneof);
      }

      @java.lang.Override
      public Builder setRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field,
          int index,
          java.lang.Object value) {
        return super.setRepeatedField(field, index, value);
      }

      @java.lang.Override
      public Builder addRepeatedField(
          com.google.protobuf.Descriptors.FieldDescriptor field, java.lang.Object value) {
        return super.addRepeatedField(field, value);
      }

      @java.lang.Override
      public Builder mergeFrom(com.google.protobuf.Message other) {
        if (other instanceof org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse) {
          return mergeFrom((org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse other) {
        if (other
            == org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse.getDefaultInstance())
          return this;
        if (!other.getId().isEmpty()) {
          id_ = other.id_;
          bitField0_ |= 0x00000001;
          onChanged();
        }
        if (other.getPayload() != com.google.protobuf.ByteString.EMPTY) {
          setPayload(other.getPayload());
        }
        this.mergeUnknownFields(other.getUnknownFields());
        onChanged();
        return this;
      }

      @java.lang.Override
      public final boolean isInitialized() {
        return true;
      }

      @java.lang.Override
      public Builder mergeFrom(
          com.google.protobuf.CodedInputStream input,
          com.google.protobuf.ExtensionRegistryLite extensionRegistry)
          throws java.io.IOException {
        if (extensionRegistry == null) {
          throw new java.lang.NullPointerException();
        }
        try {
          boolean done = false;
          while (!done) {
            int tag = input.readTag();
            switch (tag) {
              case 0:
                done = true;
                break;
              case 10:
                {
                  id_ = input.readStringRequireUtf8();
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
              case 18:
                {
                  payload_ = input.readBytes();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 18
              default:
                {
                  if (!super.parseUnknownField(input, extensionRegistry, tag)) {
                    done = true; // was an endgroup tag
                  }
                  break;
                } // default:
            } // switch (tag)
          } // while (!done)
        } catch (com.google.protobuf.InvalidProtocolBufferException e) {
          throw e.unwrapIOException();
        } finally {
          onChanged();
        } // finally
        return this;
      }

      private int bitField0_;

      private java.lang.Object id_ = "";
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @return The id.
       */
      public java.lang.String getId() {
        java.lang.Object ref = id_;
        if (!(ref instanceof java.lang.String)) {
          com.google.protobuf.ByteString bs = (com.google.protobuf.ByteString) ref;
          java.lang.String s = bs.toStringUtf8();
          id_ = s;
          return s;
        } else {
          return (java.lang.String) ref;
        }
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @return The bytes for id.
       */
      public com.google.protobuf.ByteString getIdBytes() {
        java.lang.Object ref = id_;
        if (ref instanceof String) {
          com.google.protobuf.ByteString b =
              com.google.protobuf.ByteString.copyFromUtf8((java.lang.String) ref);
          id_ = b;
          return b;
        } else {
          return (com.google.protobuf.ByteString) ref;
        }
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @param value The id to set.
       * @return This builder for chaining.
       */
      public Builder setId(java.lang.String value) {
        if (value == null) {
          throw new NullPointerException();
        }
        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearId() {
        id_ = getDefaultInstance().getId();
        bitField0_ = (bitField0_ & ~0x00000001);
        onChanged();
        return this;
      }
      /**
       * <code>string id = 1 [json_name = "id"];</code>
       *
       * @param value The bytes for id to set.
       * @return This builder for chaining.
       */
      public Builder setIdBytes(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        checkByteStringIsUtf8(value);
        id_ = value;
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }

      private com.google.protobuf.ByteString payload_ = com.google.protobuf.ByteString.EMPTY;
      /**
       * <code>bytes payload = 2 [json_name = "payload"];</code>
       *
       * @return The payload.
       */
      @java.lang.Override
      public com.google.protobuf.ByteString getPayload() {
        return payload_;
      }
      /**
       * <code>bytes payload = 2 [json_name = "payload"];</code>
       *
       * @param value The payload to set.
       * @return This builder for chaining.
       */
      public Builder setPayload(com.google.protobuf.ByteString value) {
        if (value == null) {
          throw new NullPointerException();
        }
        payload_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       * <code>bytes payload = 2 [json_name = "payload"];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearPayload() {
        bitField0_ = (bitField0_ & ~0x00000002);
        payload_ = getDefaultInstance().getPayload();
        onChanged();
        return this;
      }

      @java.lang.Override
      public final Builder setUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.setUnknownFields(unknownFields);
      }

      @java.lang.Override
      public final Builder mergeUnknownFields(
          final com.google.protobuf.UnknownFieldSet unknownFields) {
        return super.mergeUnknownFields(unknownFields);
      }

      // @@protoc_insertion_point(builder_scope:proto.echo.v1.EchoResponse)
    }

    // @@protoc_insertion_point(class_scope:proto.echo.v1.EchoResponse)
    private static final org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE = new org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse();
    }

    public static org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<EchoResponse> PARSER =
        new com.google.protobuf.AbstractParser<EchoResponse>() {
          @java.lang.Override
          public EchoResponse parsePartialFrom(
              com.google.protobuf.CodedInputStream input,
              com.google.protobuf.ExtensionRegistryLite extensionRegistry)
              throws com.google.protobuf.InvalidProtocolBufferException {
            Builder builder = newBuilder();
            try {
              builder.mergeFrom(input, extensionRegistry);
            } catch (com.google.protobuf.InvalidProtocolBufferException e) {
              throw e.setUnfinishedMessage(builder.buildPartial());
            } catch (com.google.protobuf.UninitializedMessageException e) {
              throw e.asInvalidProtocolBufferException()
                  .setUnfinishedMessage(builder.buildPartial());
            } catch (java.io.IOException e) {
              throw new com.google.protobuf.InvalidProtocolBufferException(e)
                  .setUnfinishedMessage(builder.buildPartial());
            }
            return builder.buildPartial();
          }
        };

    public static com.google.protobuf.Parser<EchoResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<EchoResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.mockapis.echo.v1.Echo.EchoResponse
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_echo_v1_EchoRequest_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_echo_v1_EchoRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_echo_v1_EchoResponse_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_echo_v1_EchoResponse_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
    return descriptor;
  }

  private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

  static {
    java.lang.String[] descriptorData = {
      "\n\030proto/echo/v1/echo.proto\022\rproto.echo.v"
          + "1\"7\n\013EchoRequest\022\016\n\002id\030\001 \001(\tR\002id\022\030\n\007payl"
          + "oad\030\002 \001(\014R\007payload\"8\n\014EchoResponse\022\016\n\002id"
          + "\030\001 \001(\tR\002id\022\030\n\007payload\030\002 \001(\014R\007payload2P\n\013"
          + "EchoService\022A\n\004Echo\022\032.proto.echo.v1.Echo"
          + "Request\032\033.proto.echo.v1.EchoResponse\"\000B;"
          + "\n*org.apache.beam.testinfra.mockapis.ech"
          + "o.v1Z\rproto/echo/v1b\006proto3"
    };
    descriptor =
        com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
            descriptorData, new com.google.protobuf.Descriptors.FileDescriptor[] {});
    internal_static_proto_echo_v1_EchoRequest_descriptor = getDescriptor().getMessageTypes().get(0);
    internal_static_proto_echo_v1_EchoRequest_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_echo_v1_EchoRequest_descriptor,
            new java.lang.String[] {
              "Id", "Payload",
            });
    internal_static_proto_echo_v1_EchoResponse_descriptor =
        getDescriptor().getMessageTypes().get(1);
    internal_static_proto_echo_v1_EchoResponse_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_echo_v1_EchoResponse_descriptor,
            new java.lang.String[] {
              "Id", "Payload",
            });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
