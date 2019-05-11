package org.apache.beam.sdk.extensions.protobuf;


import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.base.MoreObjects;

public class ProtoFieldOverlays {

    /** Overlay for Protobuf primitive types. Primitive values are just passed through. */
    class PrimitiveOverlay extends ProtoFieldOverlay<Object> {
        transient Object cached = null;

        PrimitiveOverlay(FieldDescriptor fieldDescriptor, String name) {
            super(fieldDescriptor, name);
        }

        @Override
        public Object get(Message message) {
            return MoreObjects.firstNonNull(cached, cached = message.getField(fieldDescriptor));
        }

        @Override
        public void set(Message.Builder builder, Object value) {
            builder.setField(builder.getDescriptorForType().findFieldByNumber(fieldDescriptor.getNumber()), value);
        }
    }

    class BytesOverlay extends ProtoFieldOverlay<Object> {
        transient byte[] cached = null;

        BytesOverlay(FieldDescriptor fieldDescriptor, String name) {
            super(fieldDescriptor, name);
        }

        @Override
        public Object get(Message message) {
            return MoreObjects.firstNonNull(cached, cached = message.getField(fieldDescriptor));
        }

        @Override
        public void set(Message.Builder builder, Object value) {
            builder.setField(builder.getDescriptorForType().findFieldByNumber(fieldDescriptor.getNumber()), value);
        }
    }

}
