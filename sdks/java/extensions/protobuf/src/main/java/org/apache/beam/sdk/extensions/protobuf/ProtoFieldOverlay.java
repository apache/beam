package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Message;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueSetter;

import javax.annotation.Nullable;

@Experimental(Kind.SCHEMAS)
public abstract class ProtoFieldOverlay<ValueT> implements FieldValueGetter<Message, ValueT>, FieldValueSetter<Message.Builder, ValueT> {
    protected final FieldDescriptor fieldDescriptor;
    private final String name;

    public ProtoFieldOverlay(FieldDescriptor fieldDescriptor, String name) {
        this.fieldDescriptor = fieldDescriptor;
        this.name = name;
    }

    @Nullable
    @Override
    abstract public ValueT get(Message object);

    @Override
    abstract public void set(Message.Builder builder, @Nullable ValueT value);

    @Override
     public String name() {
        return name;
    }
}