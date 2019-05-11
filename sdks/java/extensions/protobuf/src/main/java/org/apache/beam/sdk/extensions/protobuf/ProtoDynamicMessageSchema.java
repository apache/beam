package org.apache.beam.sdk.extensions.protobuf;

import com.google.protobuf.DynamicMessage;
import org.apache.beam.sdk.annotations.Experimental;
import org.apache.beam.sdk.annotations.Experimental.Kind;
import org.apache.beam.sdk.schemas.FieldValueGetter;
import org.apache.beam.sdk.schemas.FieldValueTypeInformation;
import org.apache.beam.sdk.schemas.GetterBasedSchemaProvider;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.SchemaProvider;
import org.apache.beam.sdk.schemas.SchemaUserTypeCreator;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.vendor.guava.v26_0_jre.com.google.common.collect.Lists;

import javax.annotation.Nullable;
import java.util.List;

@Experimental(Kind.SCHEMAS)
public class ProtoDynamicMessageSchema extends GetterBasedSchemaProvider {
    private static final TypeDescriptor<DynamicMessage> DYNAMIC_MESSAGE_TYPE_DESCRIPTOR =
            TypeDescriptor.of(DynamicMessage.class);
    @Nullable
    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
        checkForDynamicType(typeDescriptor);
        return ProtoSchemaTranslator.getSchema((Class<DynamicMessage>) typeDescriptor.getRawType());
    }

    @Override
    public List<FieldValueGetter> fieldValueGetters(Class<?> targetClass, Schema schema) {
        return null;
    }

    @Override
    public List<FieldValueTypeInformation> fieldValueTypeInformations(Class<?> targetClass, Schema schema) {
        List<FieldValueTypeInformation> types =
                Lists.newArrayListWithCapacity(schema.getFieldCount());
        return null;
    }

    @Override
    public SchemaUserTypeCreator schemaTypeCreator(Class<?> targetClass, Schema schema) {
        return null;
    }



    private <T> void checkForDynamicType(TypeDescriptor<T> typeDescriptor) {
        if (!typeDescriptor.isSubtypeOf(DYNAMIC_MESSAGE_TYPE_DESCRIPTOR)) {
            throw new IllegalArgumentException("ProtoDynamicMessageSchema only handles DynamicMessages.");
        }
    }
}
