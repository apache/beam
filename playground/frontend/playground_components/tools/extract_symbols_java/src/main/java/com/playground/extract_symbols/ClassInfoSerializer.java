package com.playground.extract_symbols;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;

import java.io.IOException;
import java.util.Set;

public class ClassInfoSerializer extends StdSerializer<ClassInfo> {

    protected ClassInfoSerializer(Class<ClassInfo> t) {
        super(t);
    }

    @Override
    public void serialize(ClassInfo value, JsonGenerator gen, SerializerProvider provider) throws IOException {
        gen.writeStartObject();
        gen.writeObjectFieldStart(value.name);
        writeArray(gen, "methods", value.publicMethods);
        writeArray(gen, "properties", value.publicFields);
        gen.writeEndObject();
        gen.writeEndObject();
    }

    void writeArray(JsonGenerator gen, String arrayName, Set<String> values) throws IOException {
        if (!values.isEmpty()) {
            gen.writeArrayFieldStart(arrayName);
            values.forEach(value -> {
                try {
                    gen.writeString(value);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            });
            gen.writeEndArray();
        }
    }
}
