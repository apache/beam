package org.apache.beam.sdk.io.deltalake.example.data;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.StructuredCoder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.text.DateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.List;

public class TestEvent implements Serializable {

    public static final long KEY_MODULO = 7;

    private static final ObjectMapper objectMapper = new ObjectMapper();

    public static final String EMPTY = "";

    public final static DateFormat dateFormat = DateFormat.getDateTimeInstance();

    public static final Schema SCHEMA = Schema.builder()
        .addField("id", Schema.FieldType.INT64)
        .addField("name", Schema.FieldType.STRING)
        .addField("key", Schema.FieldType.STRING)
        .addField("createTime", Schema.FieldType.DATETIME)
        .addField("data", Schema.FieldType.STRING)
        .build();

    public static final Schema AVRO_SCHEMA = Schema.builder()
        .addField("createTime", Schema.FieldType.INT64)
        .addField("data", Schema.FieldType.STRING)
        .addField("id", Schema.FieldType.INT64)
        .addField("key", Schema.FieldType.STRING)
        .addField("name", Schema.FieldType.STRING)
        .build();

    private long id;
    private String name;
    private String key;
    private long createTime;
    private String data;

    public TestEvent() {  }

    public TestEvent(long id, int dataSize, long createTime) {
        this.id = id;
        this.name = "data-" + id;
        this.key = "k-" + (id % KEY_MODULO);
        this.createTime = createTime;
        this.data = RandomStringUtils.random(dataSize, true, true);
    }


    public TestEvent(long id, String name, String key, long createTime, String data) {
        this.id = id;
        this.name = name;
        this.key = key;
        this.createTime = createTime;
        this.data = data;
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getKey() {
        return key;
    }

    public void setKey(String key) {
        this.key = key;
    }

    public long getCreateTime() {
        return createTime;
    }

    public void setCreateTime(long createTime) {
        this.createTime = createTime;
    }

    public String getData() {
        return data;
    }

    public void setData(String data) {
        this.data = data;
    }

    @Override
    public String toString()
    {
        StringBuilder sb = new StringBuilder();

        sb.append("TestEvent {")
            .append("id=" + id)
            .append(", name='" + name + '\'')
            .append(", key='" + key + '\'')
            .append(", created at " + dateFormat.format(new Date(createTime) ))
        ;
        if (data.length() < 20) {
            sb.append(", data='" + data + '\'');
        } else {
            sb.append(", data.size=" + data.length())
              .append(", data.prefix: ='" + data.substring(0, 10) + '\'');
        }
        sb.append('}');

        return sb.toString();
    }

    public String toJsonString() {
        try {
            return objectMapper.writeValueAsString(this);
        } catch (JsonProcessingException e) {
            return EMPTY;
        }
    }

    public static TestEvent fromJson(String json) {
        if ((json == null) || (EMPTY.equals(json))) {
            return new TestEvent(-2, 20, 0);
        }
        try {
            return objectMapper.readValue(json, TestEvent.class);
        } catch (IOException e) {
            return new TestEvent(-3, 30, 0);
        }
    }


    public static class TestEventCoder extends StructuredCoder<TestEvent>
    {
        private static final Coder<String> STRING_CODER = NullableCoder.of(StringUtf8Coder.of());
        private static final Coder<Long> LONG_CODER = NullableCoder.of(VarLongCoder.of());

        private static final Coder<TestEvent> INSTANCE = new TestEventCoder();

        public static Coder<TestEvent> of() { return INSTANCE; }

        @Override
        public void encode(TestEvent value, OutputStream outStream) throws IOException {
            LONG_CODER.encode(value.id, outStream);
            STRING_CODER.encode(value.name, outStream);
            STRING_CODER.encode(value.key, outStream);
            LONG_CODER.encode(value.createTime, outStream);
            STRING_CODER.encode(value.data, outStream);
        }

        @Override
        public TestEvent decode(InputStream inStream) throws IOException {
            return new TestEvent(
                LONG_CODER.decode(inStream),
                STRING_CODER.decode(inStream),
                STRING_CODER.decode(inStream),
                LONG_CODER.decode(inStream),
                STRING_CODER.decode(inStream)
            );
        }

        @Override
        public List<? extends Coder<?>> getCoderArguments() {
            return Collections.emptyList();
        }

        @Override
        public void verifyDeterministic() throws NonDeterministicException {
            STRING_CODER.verifyDeterministic();
            LONG_CODER.verifyDeterministic();
        }
    }


    public static class ToJsonConverter extends SimpleFunction<TestEvent, String>
    {
        private static final long serialVersionUID = -6496088816330493364L;

        @Override
        public String apply(TestEvent input) {
            return input.toJsonString();
        }
    }

    public static class FromJsonConverter extends SimpleFunction<String, TestEvent>
    {
        private static final long serialVersionUID = 7784025765885123360L;

        @Override
        public TestEvent apply(String input) {
            return TestEvent.fromJson(input);
        }
    }

    public static class Generator extends SimpleFunction<Long, TestEvent>
    {
        private static final long serialVersionUID = -7841026821285252094L;

        private final int dataSize;

        public Generator(int dataSize) {
            this.dataSize = dataSize;
        }

        @Override
        public TestEvent apply(Long input) {
            return new TestEvent(input, dataSize, System.currentTimeMillis());
        }
    }

    public static class TestEventFromParquetFn implements SerializableFunction<GenericRecord, TestEvent>
    {
        @Override
        public TestEvent apply(GenericRecord input)
        {
            Long id = (Long)input.get("id");
            String name = String.valueOf(input.get("name"));
            String key = String.valueOf(input.get("key"));
            Long createTime = (Long)input.get("createTime");
            String data = String.valueOf(input.get("data"));
            return new TestEvent(id, name, key, createTime, data);
        }
    }

}
