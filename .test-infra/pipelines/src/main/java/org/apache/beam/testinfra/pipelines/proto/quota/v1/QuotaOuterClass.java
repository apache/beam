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
package org.apache.beam.testinfra.pipelines.proto.quota.v1;

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
public final class QuotaOuterClass {
  private QuotaOuterClass() {}

  public static void registerAllExtensions(com.google.protobuf.ExtensionRegistryLite registry) {}

  public static void registerAllExtensions(com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions((com.google.protobuf.ExtensionRegistryLite) registry);
  }

  public interface CreateRequestOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.CreateRequest)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    boolean hasQuota();
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota();
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder();
  }
  /**
   *
   *
   * <pre>
   * CreateRequest defines a request for a new quota entry.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.CreateRequest}
   */
  public static final class CreateRequest extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.CreateRequest)
      CreateRequestOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use CreateRequest.newBuilder() to construct.
    private CreateRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private CreateRequest() {}

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new CreateRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_CreateRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_CreateRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
                  .class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
                  .Builder.class);
    }

    public static final int QUOTA_FIELD_NUMBER = 1;
    private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    @java.lang.Override
    public boolean hasQuota() {
      return quota_ != null;
    }
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
    }
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
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
      if (quota_ != null) {
        output.writeMessage(1, getQuota());
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (quota_ != null) {
        size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, getQuota());
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
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest) obj;

      if (hasQuota() != other.hasQuota()) return false;
      if (hasQuota()) {
        if (!getQuota().equals(other.getQuota())) return false;
      }
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
      if (hasQuota()) {
        hash = (37 * hash) + QUOTA_FIELD_NUMBER;
        hash = (53 * hash) + getQuota().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
            prototype) {
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
     * CreateRequest defines a request for a new quota entry.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.CreateRequest}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.CreateRequest)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_CreateRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_CreateRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_CreateRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest(
                this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.quota_ = quotaBuilder_ == null ? quota_ : quotaBuilder_.build();
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
                .getDefaultInstance()) return this;
        if (other.hasQuota()) {
          mergeQuota(other.getQuota());
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
                  input.readMessage(getQuotaFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
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

      private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          quotaBuilder_;
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return Whether the quota field is set.
       */
      public boolean hasQuota() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return The quota.
       */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
        if (quotaBuilder_ == null) {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        } else {
          return quotaBuilder_.getMessage();
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          quota_ = value;
        } else {
          quotaBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (quotaBuilder_ == null) {
          quota_ = builderForValue.build();
        } else {
          quotaBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder mergeQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)
              && quota_ != null
              && quota_
                  != org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                      .getDefaultInstance()) {
            getQuotaBuilder().mergeFrom(value);
          } else {
            quota_ = value;
          }
        } else {
          quotaBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder clearQuota() {
        bitField0_ = (bitField0_ & ~0x00000001);
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          getQuotaBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getQuotaFieldBuilder().getBuilder();
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
          getQuotaOrBuilder() {
        if (quotaBuilder_ != null) {
          return quotaBuilder_.getMessageOrBuilder();
        } else {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          getQuotaFieldBuilder() {
        if (quotaBuilder_ == null) {
          quotaBuilder_ =
              new com.google.protobuf.SingleFieldBuilderV3<
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                      .QuotaOrBuilder>(getQuota(), getParentForChildren(), isClean());
          quota_ = null;
        }
        return quotaBuilder_;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.CreateRequest)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.CreateRequest)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .CreateRequest
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<CreateRequest> PARSER =
        new com.google.protobuf.AbstractParser<CreateRequest>() {
          @java.lang.Override
          public CreateRequest parsePartialFrom(
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

    public static com.google.protobuf.Parser<CreateRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<CreateRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateRequest
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface CreateResponseOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.CreateResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    boolean hasQuota();
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota();
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder();
  }
  /**
   *
   *
   * <pre>
   * CreateResponse defines the response to a CreateQuota fulfillment.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.CreateResponse}
   */
  public static final class CreateResponse extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.CreateResponse)
      CreateResponseOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use CreateResponse.newBuilder() to construct.
    private CreateResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private CreateResponse() {}

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new CreateResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_CreateResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_CreateResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
                  .class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
                  .Builder.class);
    }

    public static final int QUOTA_FIELD_NUMBER = 1;
    private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    @java.lang.Override
    public boolean hasQuota() {
      return quota_ != null;
    }
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
    }
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
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
      if (quota_ != null) {
        output.writeMessage(1, getQuota());
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (quota_ != null) {
        size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, getQuota());
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
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse) obj;

      if (hasQuota() != other.hasQuota()) return false;
      if (hasQuota()) {
        if (!getQuota().equals(other.getQuota())) return false;
      }
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
      if (hasQuota()) {
        hash = (37 * hash) + QUOTA_FIELD_NUMBER;
        hash = (53 * hash) + getQuota().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
            prototype) {
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
     * CreateResponse defines the response to a CreateQuota fulfillment.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.CreateResponse}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.CreateResponse)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_CreateResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_CreateResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_CreateResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse(
                this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
              result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.quota_ = quotaBuilder_ == null ? quota_ : quotaBuilder_.build();
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
                .getDefaultInstance()) return this;
        if (other.hasQuota()) {
          mergeQuota(other.getQuota());
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
                  input.readMessage(getQuotaFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
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

      private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          quotaBuilder_;
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return Whether the quota field is set.
       */
      public boolean hasQuota() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return The quota.
       */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
        if (quotaBuilder_ == null) {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        } else {
          return quotaBuilder_.getMessage();
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          quota_ = value;
        } else {
          quotaBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (quotaBuilder_ == null) {
          quota_ = builderForValue.build();
        } else {
          quotaBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder mergeQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)
              && quota_ != null
              && quota_
                  != org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                      .getDefaultInstance()) {
            getQuotaBuilder().mergeFrom(value);
          } else {
            quota_ = value;
          }
        } else {
          quotaBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder clearQuota() {
        bitField0_ = (bitField0_ & ~0x00000001);
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          getQuotaBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getQuotaFieldBuilder().getBuilder();
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
          getQuotaOrBuilder() {
        if (quotaBuilder_ != null) {
          return quotaBuilder_.getMessageOrBuilder();
        } else {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          getQuotaFieldBuilder() {
        if (quotaBuilder_ == null) {
          quotaBuilder_ =
              new com.google.protobuf.SingleFieldBuilderV3<
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                      .QuotaOrBuilder>(getQuota(), getParentForChildren(), isClean());
          quota_ = null;
        }
        return quotaBuilder_;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.CreateResponse)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.CreateResponse)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .CreateResponse
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<CreateResponse> PARSER =
        new com.google.protobuf.AbstractParser<CreateResponse>() {
          @java.lang.Override
          public CreateResponse parsePartialFrom(
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

    public static com.google.protobuf.Parser<CreateResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<CreateResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.CreateResponse
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface ListRequestOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.ListRequest)
      com.google.protobuf.MessageOrBuilder {}
  /**
   *
   *
   * <pre>
   * ListRequest defines a request to list existing quota entries.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.ListRequest}
   */
  public static final class ListRequest extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.ListRequest)
      ListRequestOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use ListRequest.newBuilder() to construct.
    private ListRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private ListRequest() {}

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new ListRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_ListRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_ListRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest.class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest.Builder
                  .class);
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
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest) obj;

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
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest prototype) {
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
     * ListRequest defines a request to list existing quota entries.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.ListRequest}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.ListRequest)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_ListRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_ListRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_ListRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest(
                this);
        onBuilt();
        return result;
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
                .getDefaultInstance()) return this;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.ListRequest)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.ListRequest)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .ListRequest
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<ListRequest> PARSER =
        new com.google.protobuf.AbstractParser<ListRequest>() {
          @java.lang.Override
          public ListRequest parsePartialFrom(
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

    public static com.google.protobuf.Parser<ListRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<ListRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListRequest
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface ListResponseOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.ListResponse)
      com.google.protobuf.MessageOrBuilder {

    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    java.util.List<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>
        getListList();
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getList(int index);
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    int getListCount();
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    java.util.List<
            ? extends
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
        getListOrBuilderList();
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getListOrBuilder(int index);
  }
  /**
   *
   *
   * <pre>
   * ListQuotaResponse defines the response to a ListQuota fulfillment.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.ListResponse}
   */
  public static final class ListResponse extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.ListResponse)
      ListResponseOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use ListResponse.newBuilder() to construct.
    private ListResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private ListResponse() {
      list_ = java.util.Collections.emptyList();
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new ListResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_ListResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_ListResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse.class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
                  .Builder.class);
    }

    public static final int LIST_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private java.util.List<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>
        list_;
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    @java.lang.Override
    public java.util.List<org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>
        getListList() {
      return list_;
    }
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    @java.lang.Override
    public java.util.List<
            ? extends
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
        getListOrBuilderList() {
      return list_;
    }
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    @java.lang.Override
    public int getListCount() {
      return list_.size();
    }
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getList(
        int index) {
      return list_.get(index);
    }
    /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getListOrBuilder(int index) {
      return list_.get(index);
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
      for (int i = 0; i < list_.size(); i++) {
        output.writeMessage(1, list_.get(i));
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      for (int i = 0; i < list_.size(); i++) {
        size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, list_.get(i));
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
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse) obj;

      if (!getListList().equals(other.getListList())) return false;
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
      if (getListCount() > 0) {
        hash = (37 * hash) + LIST_FIELD_NUMBER;
        hash = (53 * hash) + getListList().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse prototype) {
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
     * ListQuotaResponse defines the response to a ListQuota fulfillment.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.ListResponse}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.ListResponse)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_ListResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_ListResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        if (listBuilder_ == null) {
          list_ = java.util.Collections.emptyList();
        } else {
          list_ = null;
          listBuilder_.clear();
        }
        bitField0_ = (bitField0_ & ~0x00000001);
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_ListResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse(
                this);
        buildPartialRepeatedFields(result);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartialRepeatedFields(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse result) {
        if (listBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)) {
            list_ = java.util.Collections.unmodifiableList(list_);
            bitField0_ = (bitField0_ & ~0x00000001);
          }
          result.list_ = list_;
        } else {
          result.list_ = listBuilder_.build();
        }
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse result) {
        int from_bitField0_ = bitField0_;
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
                .getDefaultInstance()) return this;
        if (listBuilder_ == null) {
          if (!other.list_.isEmpty()) {
            if (list_.isEmpty()) {
              list_ = other.list_;
              bitField0_ = (bitField0_ & ~0x00000001);
            } else {
              ensureListIsMutable();
              list_.addAll(other.list_);
            }
            onChanged();
          }
        } else {
          if (!other.list_.isEmpty()) {
            if (listBuilder_.isEmpty()) {
              listBuilder_.dispose();
              listBuilder_ = null;
              list_ = other.list_;
              bitField0_ = (bitField0_ & ~0x00000001);
              listBuilder_ =
                  com.google.protobuf.GeneratedMessageV3.alwaysUseFieldBuilders
                      ? getListFieldBuilder()
                      : null;
            } else {
              listBuilder_.addAllMessages(other.list_);
            }
          }
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
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota m =
                      input.readMessage(
                          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                              .parser(),
                          extensionRegistry);
                  if (listBuilder_ == null) {
                    ensureListIsMutable();
                    list_.add(m);
                  } else {
                    listBuilder_.addMessage(m);
                  }
                  break;
                } // case 10
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

      private java.util.List<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>
          list_ = java.util.Collections.emptyList();

      private void ensureListIsMutable() {
        if (!((bitField0_ & 0x00000001) != 0)) {
          list_ =
              new java.util.ArrayList<
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>(list_);
          bitField0_ |= 0x00000001;
        }
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          listBuilder_;

      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public java.util.List<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>
          getListList() {
        if (listBuilder_ == null) {
          return java.util.Collections.unmodifiableList(list_);
        } else {
          return listBuilder_.getMessageList();
        }
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public int getListCount() {
        if (listBuilder_ == null) {
          return list_.size();
        } else {
          return listBuilder_.getCount();
        }
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getList(
          int index) {
        if (listBuilder_ == null) {
          return list_.get(index);
        } else {
          return listBuilder_.getMessage(index);
        }
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder setList(
          int index,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (listBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureListIsMutable();
          list_.set(index, value);
          onChanged();
        } else {
          listBuilder_.setMessage(index, value);
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder setList(
          int index,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (listBuilder_ == null) {
          ensureListIsMutable();
          list_.set(index, builderForValue.build());
          onChanged();
        } else {
          listBuilder_.setMessage(index, builderForValue.build());
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder addList(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (listBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureListIsMutable();
          list_.add(value);
          onChanged();
        } else {
          listBuilder_.addMessage(value);
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder addList(
          int index,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (listBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          ensureListIsMutable();
          list_.add(index, value);
          onChanged();
        } else {
          listBuilder_.addMessage(index, value);
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder addList(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (listBuilder_ == null) {
          ensureListIsMutable();
          list_.add(builderForValue.build());
          onChanged();
        } else {
          listBuilder_.addMessage(builderForValue.build());
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder addList(
          int index,
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (listBuilder_ == null) {
          ensureListIsMutable();
          list_.add(index, builderForValue.build());
          onChanged();
        } else {
          listBuilder_.addMessage(index, builderForValue.build());
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder addAllList(
          java.lang.Iterable<
                  ? extends
                      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota>
              values) {
        if (listBuilder_ == null) {
          ensureListIsMutable();
          com.google.protobuf.AbstractMessageLite.Builder.addAll(values, list_);
          onChanged();
        } else {
          listBuilder_.addAllMessages(values);
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder clearList() {
        if (listBuilder_ == null) {
          list_ = java.util.Collections.emptyList();
          bitField0_ = (bitField0_ & ~0x00000001);
          onChanged();
        } else {
          listBuilder_.clear();
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public Builder removeList(int index) {
        if (listBuilder_ == null) {
          ensureListIsMutable();
          list_.remove(index);
          onChanged();
        } else {
          listBuilder_.remove(index);
        }
        return this;
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          getListBuilder(int index) {
        return getListFieldBuilder().getBuilder(index);
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
          getListOrBuilder(int index) {
        if (listBuilder_ == null) {
          return list_.get(index);
        } else {
          return listBuilder_.getMessageOrBuilder(index);
        }
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public java.util.List<
              ? extends
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          getListOrBuilderList() {
        if (listBuilder_ != null) {
          return listBuilder_.getMessageOrBuilderList();
        } else {
          return java.util.Collections.unmodifiableList(list_);
        }
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          addListBuilder() {
        return getListFieldBuilder()
            .addBuilder(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                    .getDefaultInstance());
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          addListBuilder(int index) {
        return getListFieldBuilder()
            .addBuilder(
                index,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                    .getDefaultInstance());
      }
      /** <code>repeated .proto.quota.v1.Quota list = 1 [json_name = "list"];</code> */
      public java.util.List<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder>
          getListBuilderList() {
        return getListFieldBuilder().getBuilderList();
      }

      private com.google.protobuf.RepeatedFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          getListFieldBuilder() {
        if (listBuilder_ == null) {
          listBuilder_ =
              new com.google.protobuf.RepeatedFieldBuilderV3<
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                      .QuotaOrBuilder>(
                  list_, ((bitField0_ & 0x00000001) != 0), getParentForChildren(), isClean());
          list_ = null;
        }
        return listBuilder_;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.ListResponse)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.ListResponse)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .ListResponse
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<ListResponse> PARSER =
        new com.google.protobuf.AbstractParser<ListResponse>() {
          @java.lang.Override
          public ListResponse parsePartialFrom(
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

    public static com.google.protobuf.Parser<ListResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<ListResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.ListResponse
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface DeleteRequestOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.DeleteRequest)
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
  }
  /**
   *
   *
   * <pre>
   * DeleteQuotasRequest defines a request to delete a quota entry.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.DeleteRequest}
   */
  public static final class DeleteRequest extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.DeleteRequest)
      DeleteRequestOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use DeleteRequest.newBuilder() to construct.
    private DeleteRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private DeleteRequest() {
      id_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new DeleteRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DeleteRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DeleteRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
                  .class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
                  .Builder.class);
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
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest) obj;

      if (!getId().equals(other.getId())) return false;
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
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
            prototype) {
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
     * DeleteQuotasRequest defines a request to delete a quota entry.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.DeleteRequest}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.DeleteRequest)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DeleteRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DeleteRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = "";
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DeleteRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest(
                this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
                .getDefaultInstance()) return this;
        if (!other.getId().isEmpty()) {
          id_ = other.id_;
          bitField0_ |= 0x00000001;
          onChanged();
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.DeleteRequest)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.DeleteRequest)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DeleteRequest
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<DeleteRequest> PARSER =
        new com.google.protobuf.AbstractParser<DeleteRequest>() {
          @java.lang.Override
          public DeleteRequest parsePartialFrom(
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

    public static com.google.protobuf.Parser<DeleteRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<DeleteRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteRequest
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface DeleteResponseOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.DeleteResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    boolean hasQuota();
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota();
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder();
  }
  /**
   *
   *
   * <pre>
   * DeleteResponse defines the response to a DeleteQuota fulfillment.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.DeleteResponse}
   */
  public static final class DeleteResponse extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.DeleteResponse)
      DeleteResponseOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use DeleteResponse.newBuilder() to construct.
    private DeleteResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private DeleteResponse() {}

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new DeleteResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DeleteResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DeleteResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
                  .class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
                  .Builder.class);
    }

    public static final int QUOTA_FIELD_NUMBER = 1;
    private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    @java.lang.Override
    public boolean hasQuota() {
      return quota_ != null;
    }
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
    }
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
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
      if (quota_ != null) {
        output.writeMessage(1, getQuota());
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (quota_ != null) {
        size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, getQuota());
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
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse) obj;

      if (hasQuota() != other.hasQuota()) return false;
      if (hasQuota()) {
        if (!getQuota().equals(other.getQuota())) return false;
      }
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
      if (hasQuota()) {
        hash = (37 * hash) + QUOTA_FIELD_NUMBER;
        hash = (53 * hash) + getQuota().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
            prototype) {
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
     * DeleteResponse defines the response to a DeleteQuota fulfillment.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.DeleteResponse}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.DeleteResponse)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DeleteResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DeleteResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DeleteResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse(
                this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
              result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.quota_ = quotaBuilder_ == null ? quota_ : quotaBuilder_.build();
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
                .getDefaultInstance()) return this;
        if (other.hasQuota()) {
          mergeQuota(other.getQuota());
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
                  input.readMessage(getQuotaFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
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

      private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          quotaBuilder_;
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return Whether the quota field is set.
       */
      public boolean hasQuota() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return The quota.
       */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
        if (quotaBuilder_ == null) {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        } else {
          return quotaBuilder_.getMessage();
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          quota_ = value;
        } else {
          quotaBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (quotaBuilder_ == null) {
          quota_ = builderForValue.build();
        } else {
          quotaBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder mergeQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)
              && quota_ != null
              && quota_
                  != org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                      .getDefaultInstance()) {
            getQuotaBuilder().mergeFrom(value);
          } else {
            quota_ = value;
          }
        } else {
          quotaBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder clearQuota() {
        bitField0_ = (bitField0_ & ~0x00000001);
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          getQuotaBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getQuotaFieldBuilder().getBuilder();
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
          getQuotaOrBuilder() {
        if (quotaBuilder_ != null) {
          return quotaBuilder_.getMessageOrBuilder();
        } else {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          getQuotaFieldBuilder() {
        if (quotaBuilder_ == null) {
          quotaBuilder_ =
              new com.google.protobuf.SingleFieldBuilderV3<
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                      .QuotaOrBuilder>(getQuota(), getParentForChildren(), isClean());
          quota_ = null;
        }
        return quotaBuilder_;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.DeleteResponse)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.DeleteResponse)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DeleteResponse
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<DeleteResponse> PARSER =
        new com.google.protobuf.AbstractParser<DeleteResponse>() {
          @java.lang.Override
          public DeleteResponse parsePartialFrom(
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

    public static com.google.protobuf.Parser<DeleteResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<DeleteResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DeleteResponse
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface DescribeRequestOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.DescribeRequest)
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
  }
  /**
   *
   *
   * <pre>
   * DescribeResponse defines the request to describe a quota entry.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.DescribeRequest}
   */
  public static final class DescribeRequest extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.DescribeRequest)
      DescribeRequestOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use DescribeRequest.newBuilder() to construct.
    private DescribeRequest(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private DescribeRequest() {
      id_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new DescribeRequest();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DescribeRequest_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DescribeRequest_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                  .class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                  .Builder.class);
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
      size += getUnknownFields().getSerializedSize();
      memoizedSize = size;
      return size;
    }

    @java.lang.Override
    public boolean equals(final java.lang.Object obj) {
      if (obj == this) {
        return true;
      }
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest) obj;

      if (!getId().equals(other.getId())) return false;
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
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
            prototype) {
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
     * DescribeResponse defines the request to describe a quota entry.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.DescribeRequest}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.DescribeRequest)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeRequestOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DescribeRequest_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DescribeRequest_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = "";
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DescribeRequest_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest(
                this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
              result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
              other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
                .getDefaultInstance()) return this;
        if (!other.getId().isEmpty()) {
          id_ = other.id_;
          bitField0_ |= 0x00000001;
          onChanged();
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.DescribeRequest)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.DescribeRequest)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeRequest
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<DescribeRequest> PARSER =
        new com.google.protobuf.AbstractParser<DescribeRequest>() {
          @java.lang.Override
          public DescribeRequest parsePartialFrom(
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

    public static com.google.protobuf.Parser<DescribeRequest> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<DescribeRequest> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeRequest
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface DescribeResponseOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.DescribeResponse)
      com.google.protobuf.MessageOrBuilder {

    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    boolean hasQuota();
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota();
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder();
  }
  /**
   *
   *
   * <pre>
   * DescribeResponse defines the response to a DescribeQuota fulfillment.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.DescribeResponse}
   */
  public static final class DescribeResponse extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.DescribeResponse)
      DescribeResponseOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use DescribeResponse.newBuilder() to construct.
    private DescribeResponse(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private DescribeResponse() {}

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new DescribeResponse();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DescribeResponse_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_DescribeResponse_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
                  .class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
                  .Builder.class);
    }

    public static final int QUOTA_FIELD_NUMBER = 1;
    private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return Whether the quota field is set.
     */
    @java.lang.Override
    public boolean hasQuota() {
      return quota_ != null;
    }
    /**
     * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
     *
     * @return The quota.
     */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
    }
    /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
        getQuotaOrBuilder() {
      return quota_ == null
          ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
              .getDefaultInstance()
          : quota_;
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
      if (quota_ != null) {
        output.writeMessage(1, getQuota());
      }
      getUnknownFields().writeTo(output);
    }

    @java.lang.Override
    public int getSerializedSize() {
      int size = memoizedSize;
      if (size != -1) return size;

      size = 0;
      if (quota_ != null) {
        size += com.google.protobuf.CodedOutputStream.computeMessageSize(1, getQuota());
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
      if (!(obj
          instanceof
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse) obj;

      if (hasQuota() != other.hasQuota()) return false;
      if (hasQuota()) {
        if (!getQuota().equals(other.getQuota())) return false;
      }
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
      if (hasQuota()) {
        hash = (37 * hash) + QUOTA_FIELD_NUMBER;
        hash = (53 * hash) + getQuota().hashCode();
      }
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
            prototype) {
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
     * DescribeResponse defines the response to a DescribeQuota fulfillment.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.DescribeResponse}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.DescribeResponse)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponseOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DescribeResponse_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DescribeResponse_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
                    .class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
                    .Builder.class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_DescribeResponse_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
          build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse(
                this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
              result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.quota_ = quotaBuilder_ == null ? quota_ : quotaBuilder_.build();
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
        if (other
            instanceof
            org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse)
                  other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
              other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
                .getDefaultInstance()) return this;
        if (other.hasQuota()) {
          mergeQuota(other.getQuota());
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
                  input.readMessage(getQuotaFieldBuilder().getBuilder(), extensionRegistry);
                  bitField0_ |= 0x00000001;
                  break;
                } // case 10
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

      private org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota quota_;
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          quotaBuilder_;
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return Whether the quota field is set.
       */
      public boolean hasQuota() {
        return ((bitField0_ & 0x00000001) != 0);
      }
      /**
       * <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code>
       *
       * @return The quota.
       */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota getQuota() {
        if (quotaBuilder_ == null) {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        } else {
          return quotaBuilder_.getMessage();
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (value == null) {
            throw new NullPointerException();
          }
          quota_ = value;
        } else {
          quotaBuilder_.setMessage(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder setQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
              builderForValue) {
        if (quotaBuilder_ == null) {
          quota_ = builderForValue.build();
        } else {
          quotaBuilder_.setMessage(builderForValue.build());
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder mergeQuota(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota value) {
        if (quotaBuilder_ == null) {
          if (((bitField0_ & 0x00000001) != 0)
              && quota_ != null
              && quota_
                  != org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                      .getDefaultInstance()) {
            getQuotaBuilder().mergeFrom(value);
          } else {
            quota_ = value;
          }
        } else {
          quotaBuilder_.mergeFrom(value);
        }
        bitField0_ |= 0x00000001;
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public Builder clearQuota() {
        bitField0_ = (bitField0_ & ~0x00000001);
        quota_ = null;
        if (quotaBuilder_ != null) {
          quotaBuilder_.dispose();
          quotaBuilder_ = null;
        }
        onChanged();
        return this;
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
          getQuotaBuilder() {
        bitField0_ |= 0x00000001;
        onChanged();
        return getQuotaFieldBuilder().getBuilder();
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder
          getQuotaOrBuilder() {
        if (quotaBuilder_ != null) {
          return quotaBuilder_.getMessageOrBuilder();
        } else {
          return quota_ == null
              ? org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                  .getDefaultInstance()
              : quota_;
        }
      }
      /** <code>.proto.quota.v1.Quota quota = 1 [json_name = "quota"];</code> */
      private com.google.protobuf.SingleFieldBuilderV3<
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder>
          getQuotaFieldBuilder() {
        if (quotaBuilder_ == null) {
          quotaBuilder_ =
              new com.google.protobuf.SingleFieldBuilderV3<
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder,
                  org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
                      .QuotaOrBuilder>(getQuota(), getParentForChildren(), isClean());
          quota_ = null;
        }
        return quotaBuilder_;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.DescribeResponse)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.DescribeResponse)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .DescribeResponse
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<DescribeResponse> PARSER =
        new com.google.protobuf.AbstractParser<DescribeResponse>() {
          @java.lang.Override
          public DescribeResponse parsePartialFrom(
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

    public static com.google.protobuf.Parser<DescribeResponse> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<DescribeResponse> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.DescribeResponse
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  public interface QuotaOrBuilder
      extends
      // @@protoc_insertion_point(interface_extends:proto.quota.v1.Quota)
      com.google.protobuf.MessageOrBuilder {

    /**
     *
     *
     * <pre>
     * id assigns a unique identifier to the quota entry.
     * </pre>
     *
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The id.
     */
    java.lang.String getId();
    /**
     *
     *
     * <pre>
     * id assigns a unique identifier to the quota entry.
     * </pre>
     *
     * <code>string id = 1 [json_name = "id"];</code>
     *
     * @return The bytes for id.
     */
    com.google.protobuf.ByteString getIdBytes();

    /**
     *
     *
     * <pre>
     * size assigns the value of the quota entry.
     * </pre>
     *
     * <code>int64 size = 2 [json_name = "size"];</code>
     *
     * @return The size.
     */
    long getSize();

    /**
     *
     *
     * <pre>
     * refresh_milliseconds_interval configures how often the quota service
     * should refresh the quota's value back to the assigned size.
     * </pre>
     *
     * <code>int64 refresh_milliseconds_interval = 3 [json_name = "refreshMillisecondsInterval"];
     * </code>
     *
     * @return The refreshMillisecondsInterval.
     */
    long getRefreshMillisecondsInterval();
  }
  /**
   *
   *
   * <pre>
   * Quota defines details of a quota.
   * </pre>
   *
   * Protobuf type {@code proto.quota.v1.Quota}
   */
  public static final class Quota extends com.google.protobuf.GeneratedMessageV3
      implements
      // @@protoc_insertion_point(message_implements:proto.quota.v1.Quota)
      QuotaOrBuilder {
    private static final long serialVersionUID = 0L;
    // Use Quota.newBuilder() to construct.
    private Quota(com.google.protobuf.GeneratedMessageV3.Builder<?> builder) {
      super(builder);
    }

    private Quota() {
      id_ = "";
    }

    @java.lang.Override
    @SuppressWarnings({"unused"})
    protected java.lang.Object newInstance(UnusedPrivateParameter unused) {
      return new Quota();
    }

    @java.lang.Override
    public final com.google.protobuf.UnknownFieldSet getUnknownFields() {
      return this.unknownFields;
    }

    public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_Quota_descriptor;
    }

    @java.lang.Override
    protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
        internalGetFieldAccessorTable() {
      return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
          .internal_static_proto_quota_v1_Quota_fieldAccessorTable
          .ensureFieldAccessorsInitialized(
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.class,
              org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
                  .class);
    }

    public static final int ID_FIELD_NUMBER = 1;

    @SuppressWarnings("serial")
    private volatile java.lang.Object id_ = "";
    /**
     *
     *
     * <pre>
     * id assigns a unique identifier to the quota entry.
     * </pre>
     *
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
     *
     *
     * <pre>
     * id assigns a unique identifier to the quota entry.
     * </pre>
     *
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

    public static final int SIZE_FIELD_NUMBER = 2;
    private long size_ = 0L;
    /**
     *
     *
     * <pre>
     * size assigns the value of the quota entry.
     * </pre>
     *
     * <code>int64 size = 2 [json_name = "size"];</code>
     *
     * @return The size.
     */
    @java.lang.Override
    public long getSize() {
      return size_;
    }

    public static final int REFRESH_MILLISECONDS_INTERVAL_FIELD_NUMBER = 3;
    private long refreshMillisecondsInterval_ = 0L;
    /**
     *
     *
     * <pre>
     * refresh_milliseconds_interval configures how often the quota service
     * should refresh the quota's value back to the assigned size.
     * </pre>
     *
     * <code>int64 refresh_milliseconds_interval = 3 [json_name = "refreshMillisecondsInterval"];
     * </code>
     *
     * @return The refreshMillisecondsInterval.
     */
    @java.lang.Override
    public long getRefreshMillisecondsInterval() {
      return refreshMillisecondsInterval_;
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
      if (size_ != 0L) {
        output.writeInt64(2, size_);
      }
      if (refreshMillisecondsInterval_ != 0L) {
        output.writeInt64(3, refreshMillisecondsInterval_);
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
      if (size_ != 0L) {
        size += com.google.protobuf.CodedOutputStream.computeInt64Size(2, size_);
      }
      if (refreshMillisecondsInterval_ != 0L) {
        size +=
            com.google.protobuf.CodedOutputStream.computeInt64Size(3, refreshMillisecondsInterval_);
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
      if (!(obj
          instanceof org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota)) {
        return super.equals(obj);
      }
      org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota other =
          (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota) obj;

      if (!getId().equals(other.getId())) return false;
      if (getSize() != other.getSize()) return false;
      if (getRefreshMillisecondsInterval() != other.getRefreshMillisecondsInterval()) return false;
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
      hash = (37 * hash) + SIZE_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getSize());
      hash = (37 * hash) + REFRESH_MILLISECONDS_INTERVAL_FIELD_NUMBER;
      hash = (53 * hash) + com.google.protobuf.Internal.hashLong(getRefreshMillisecondsInterval());
      hash = (29 * hash) + getUnknownFields().hashCode();
      memoizedHashCode = hash;
      return hash;
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(java.nio.ByteBuffer data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(
            java.nio.ByteBuffer data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(com.google.protobuf.ByteString data)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(
            com.google.protobuf.ByteString data,
            com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(byte[] data) throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(byte[] data, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws com.google.protobuf.InvalidProtocolBufferException {
      return PARSER.parseFrom(data, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseDelimitedFrom(java.io.InputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseDelimitedFrom(
            java.io.InputStream input, com.google.protobuf.ExtensionRegistryLite extensionRegistry)
            throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseDelimitedWithIOException(
          PARSER, input, extensionRegistry);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(com.google.protobuf.CodedInputStream input) throws java.io.IOException {
      return com.google.protobuf.GeneratedMessageV3.parseWithIOException(PARSER, input);
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        parseFrom(
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
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota prototype) {
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
     * Quota defines details of a quota.
     * </pre>
     *
     * Protobuf type {@code proto.quota.v1.Quota}
     */
    public static final class Builder
        extends com.google.protobuf.GeneratedMessageV3.Builder<Builder>
        implements
        // @@protoc_insertion_point(builder_implements:proto.quota.v1.Quota)
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.QuotaOrBuilder {
      public static final com.google.protobuf.Descriptors.Descriptor getDescriptor() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_Quota_descriptor;
      }

      @java.lang.Override
      protected com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
          internalGetFieldAccessorTable() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_Quota_fieldAccessorTable
            .ensureFieldAccessorsInitialized(
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.class,
                org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.Builder
                    .class);
      }

      // Construct using
      // org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota.newBuilder()
      private Builder() {}

      private Builder(com.google.protobuf.GeneratedMessageV3.BuilderParent parent) {
        super(parent);
      }

      @java.lang.Override
      public Builder clear() {
        super.clear();
        bitField0_ = 0;
        id_ = "";
        size_ = 0L;
        refreshMillisecondsInterval_ = 0L;
        return this;
      }

      @java.lang.Override
      public com.google.protobuf.Descriptors.Descriptor getDescriptorForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass
            .internal_static_proto_quota_v1_Quota_descriptor;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
          getDefaultInstanceForType() {
        return org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
            .getDefaultInstance();
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota build() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota result =
            buildPartial();
        if (!result.isInitialized()) {
          throw newUninitializedMessageException(result);
        }
        return result;
      }

      @java.lang.Override
      public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
          buildPartial() {
        org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota result =
            new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota(this);
        if (bitField0_ != 0) {
          buildPartial0(result);
        }
        onBuilt();
        return result;
      }

      private void buildPartial0(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota result) {
        int from_bitField0_ = bitField0_;
        if (((from_bitField0_ & 0x00000001) != 0)) {
          result.id_ = id_;
        }
        if (((from_bitField0_ & 0x00000002) != 0)) {
          result.size_ = size_;
        }
        if (((from_bitField0_ & 0x00000004) != 0)) {
          result.refreshMillisecondsInterval_ = refreshMillisecondsInterval_;
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
        if (other
            instanceof org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota) {
          return mergeFrom(
              (org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota) other);
        } else {
          super.mergeFrom(other);
          return this;
        }
      }

      public Builder mergeFrom(
          org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota other) {
        if (other
            == org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
                .getDefaultInstance()) return this;
        if (!other.getId().isEmpty()) {
          id_ = other.id_;
          bitField0_ |= 0x00000001;
          onChanged();
        }
        if (other.getSize() != 0L) {
          setSize(other.getSize());
        }
        if (other.getRefreshMillisecondsInterval() != 0L) {
          setRefreshMillisecondsInterval(other.getRefreshMillisecondsInterval());
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
              case 16:
                {
                  size_ = input.readInt64();
                  bitField0_ |= 0x00000002;
                  break;
                } // case 16
              case 24:
                {
                  refreshMillisecondsInterval_ = input.readInt64();
                  bitField0_ |= 0x00000004;
                  break;
                } // case 24
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
       *
       *
       * <pre>
       * id assigns a unique identifier to the quota entry.
       * </pre>
       *
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
       *
       *
       * <pre>
       * id assigns a unique identifier to the quota entry.
       * </pre>
       *
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
       *
       *
       * <pre>
       * id assigns a unique identifier to the quota entry.
       * </pre>
       *
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
       *
       *
       * <pre>
       * id assigns a unique identifier to the quota entry.
       * </pre>
       *
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
       *
       *
       * <pre>
       * id assigns a unique identifier to the quota entry.
       * </pre>
       *
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

      private long size_;
      /**
       *
       *
       * <pre>
       * size assigns the value of the quota entry.
       * </pre>
       *
       * <code>int64 size = 2 [json_name = "size"];</code>
       *
       * @return The size.
       */
      @java.lang.Override
      public long getSize() {
        return size_;
      }
      /**
       *
       *
       * <pre>
       * size assigns the value of the quota entry.
       * </pre>
       *
       * <code>int64 size = 2 [json_name = "size"];</code>
       *
       * @param value The size to set.
       * @return This builder for chaining.
       */
      public Builder setSize(long value) {

        size_ = value;
        bitField0_ |= 0x00000002;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * size assigns the value of the quota entry.
       * </pre>
       *
       * <code>int64 size = 2 [json_name = "size"];</code>
       *
       * @return This builder for chaining.
       */
      public Builder clearSize() {
        bitField0_ = (bitField0_ & ~0x00000002);
        size_ = 0L;
        onChanged();
        return this;
      }

      private long refreshMillisecondsInterval_;
      /**
       *
       *
       * <pre>
       * refresh_milliseconds_interval configures how often the quota service
       * should refresh the quota's value back to the assigned size.
       * </pre>
       *
       * <code>int64 refresh_milliseconds_interval = 3 [json_name = "refreshMillisecondsInterval"];
       * </code>
       *
       * @return The refreshMillisecondsInterval.
       */
      @java.lang.Override
      public long getRefreshMillisecondsInterval() {
        return refreshMillisecondsInterval_;
      }
      /**
       *
       *
       * <pre>
       * refresh_milliseconds_interval configures how often the quota service
       * should refresh the quota's value back to the assigned size.
       * </pre>
       *
       * <code>int64 refresh_milliseconds_interval = 3 [json_name = "refreshMillisecondsInterval"];
       * </code>
       *
       * @param value The refreshMillisecondsInterval to set.
       * @return This builder for chaining.
       */
      public Builder setRefreshMillisecondsInterval(long value) {

        refreshMillisecondsInterval_ = value;
        bitField0_ |= 0x00000004;
        onChanged();
        return this;
      }
      /**
       *
       *
       * <pre>
       * refresh_milliseconds_interval configures how often the quota service
       * should refresh the quota's value back to the assigned size.
       * </pre>
       *
       * <code>int64 refresh_milliseconds_interval = 3 [json_name = "refreshMillisecondsInterval"];
       * </code>
       *
       * @return This builder for chaining.
       */
      public Builder clearRefreshMillisecondsInterval() {
        bitField0_ = (bitField0_ & ~0x00000004);
        refreshMillisecondsInterval_ = 0L;
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

      // @@protoc_insertion_point(builder_scope:proto.quota.v1.Quota)
    }

    // @@protoc_insertion_point(class_scope:proto.quota.v1.Quota)
    private static final org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        DEFAULT_INSTANCE;

    static {
      DEFAULT_INSTANCE =
          new org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota();
    }

    public static org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        getDefaultInstance() {
      return DEFAULT_INSTANCE;
    }

    private static final com.google.protobuf.Parser<Quota> PARSER =
        new com.google.protobuf.AbstractParser<Quota>() {
          @java.lang.Override
          public Quota parsePartialFrom(
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

    public static com.google.protobuf.Parser<Quota> parser() {
      return PARSER;
    }

    @java.lang.Override
    public com.google.protobuf.Parser<Quota> getParserForType() {
      return PARSER;
    }

    @java.lang.Override
    public org.apache.beam.testinfra.pipelines.proto.quota.v1.QuotaOuterClass.Quota
        getDefaultInstanceForType() {
      return DEFAULT_INSTANCE;
    }
  }

  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_CreateRequest_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_CreateRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_CreateResponse_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_CreateResponse_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_ListRequest_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_ListRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_ListResponse_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_ListResponse_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_DeleteRequest_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_DeleteRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_DeleteResponse_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_DeleteResponse_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_DescribeRequest_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_DescribeRequest_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_DescribeResponse_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_DescribeResponse_fieldAccessorTable;
  private static final com.google.protobuf.Descriptors.Descriptor
      internal_static_proto_quota_v1_Quota_descriptor;
  private static final com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_proto_quota_v1_Quota_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor getDescriptor() {
    return descriptor;
  }

  private static com.google.protobuf.Descriptors.FileDescriptor descriptor;

  static {
    java.lang.String[] descriptorData = {
      "\n\032proto/quota/v1/quota.proto\022\016proto.quot"
          + "a.v1\"<\n\rCreateRequest\022+\n\005quota\030\001 \001(\0132\025.p"
          + "roto.quota.v1.QuotaR\005quota\"=\n\016CreateResp"
          + "onse\022+\n\005quota\030\001 \001(\0132\025.proto.quota.v1.Quo"
          + "taR\005quota\"\r\n\013ListRequest\"9\n\014ListResponse"
          + "\022)\n\004list\030\001 \003(\0132\025.proto.quota.v1.QuotaR\004l"
          + "ist\"\037\n\rDeleteRequest\022\016\n\002id\030\001 \001(\tR\002id\"=\n\016"
          + "DeleteResponse\022+\n\005quota\030\001 \001(\0132\025.proto.qu"
          + "ota.v1.QuotaR\005quota\"!\n\017DescribeRequest\022\016"
          + "\n\002id\030\001 \001(\tR\002id\"?\n\020DescribeResponse\022+\n\005qu"
          + "ota\030\001 \001(\0132\025.proto.quota.v1.QuotaR\005quota\""
          + "o\n\005Quota\022\016\n\002id\030\001 \001(\tR\002id\022\022\n\004size\030\002 \001(\003R\004"
          + "size\022B\n\035refresh_milliseconds_interval\030\003 "
          + "\001(\003R\033refreshMillisecondsInterval2\272\002\n\014Quo"
          + "taService\022I\n\006Create\022\035.proto.quota.v1.Cre"
          + "ateRequest\032\036.proto.quota.v1.CreateRespon"
          + "se\"\000\022C\n\004List\022\033.proto.quota.v1.ListReques"
          + "t\032\034.proto.quota.v1.ListResponse\"\000\022I\n\006Del"
          + "ete\022\035.proto.quota.v1.DeleteRequest\032\036.pro"
          + "to.quota.v1.DeleteResponse\"\000\022O\n\010Describe"
          + "\022\037.proto.quota.v1.DescribeRequest\032 .prot"
          + "o.quota.v1.DescribeResponse\"\000BD\n2org.apa"
          + "che.beam.testinfra.pipelines.proto.quota"
          + ".v1Z\016proto/quota/v1b\006proto3"
    };
    descriptor =
        com.google.protobuf.Descriptors.FileDescriptor.internalBuildGeneratedFileFrom(
            descriptorData, new com.google.protobuf.Descriptors.FileDescriptor[] {});
    internal_static_proto_quota_v1_CreateRequest_descriptor =
        getDescriptor().getMessageTypes().get(0);
    internal_static_proto_quota_v1_CreateRequest_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_CreateRequest_descriptor,
            new java.lang.String[] {
              "Quota",
            });
    internal_static_proto_quota_v1_CreateResponse_descriptor =
        getDescriptor().getMessageTypes().get(1);
    internal_static_proto_quota_v1_CreateResponse_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_CreateResponse_descriptor,
            new java.lang.String[] {
              "Quota",
            });
    internal_static_proto_quota_v1_ListRequest_descriptor =
        getDescriptor().getMessageTypes().get(2);
    internal_static_proto_quota_v1_ListRequest_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_ListRequest_descriptor, new java.lang.String[] {});
    internal_static_proto_quota_v1_ListResponse_descriptor =
        getDescriptor().getMessageTypes().get(3);
    internal_static_proto_quota_v1_ListResponse_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_ListResponse_descriptor,
            new java.lang.String[] {
              "List",
            });
    internal_static_proto_quota_v1_DeleteRequest_descriptor =
        getDescriptor().getMessageTypes().get(4);
    internal_static_proto_quota_v1_DeleteRequest_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_DeleteRequest_descriptor,
            new java.lang.String[] {
              "Id",
            });
    internal_static_proto_quota_v1_DeleteResponse_descriptor =
        getDescriptor().getMessageTypes().get(5);
    internal_static_proto_quota_v1_DeleteResponse_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_DeleteResponse_descriptor,
            new java.lang.String[] {
              "Quota",
            });
    internal_static_proto_quota_v1_DescribeRequest_descriptor =
        getDescriptor().getMessageTypes().get(6);
    internal_static_proto_quota_v1_DescribeRequest_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_DescribeRequest_descriptor,
            new java.lang.String[] {
              "Id",
            });
    internal_static_proto_quota_v1_DescribeResponse_descriptor =
        getDescriptor().getMessageTypes().get(7);
    internal_static_proto_quota_v1_DescribeResponse_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_DescribeResponse_descriptor,
            new java.lang.String[] {
              "Quota",
            });
    internal_static_proto_quota_v1_Quota_descriptor = getDescriptor().getMessageTypes().get(8);
    internal_static_proto_quota_v1_Quota_fieldAccessorTable =
        new com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
            internal_static_proto_quota_v1_Quota_descriptor,
            new java.lang.String[] {
              "Id", "Size", "RefreshMillisecondsInterval",
            });
  }

  // @@protoc_insertion_point(outer_class_scope)
}
