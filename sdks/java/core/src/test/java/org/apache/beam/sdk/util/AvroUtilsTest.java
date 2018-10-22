package org.apache.beam.sdk.util;

import static org.hamcrest.Matchers.not;
import static org.junit.Assume.assumeThat;

import com.google.common.collect.Lists;
import com.pholser.junit.quickcheck.From;
import com.pholser.junit.quickcheck.Property;
import com.pholser.junit.quickcheck.runner.JUnitQuickcheck;
import java.util.List;
import java.util.function.Function;
import org.apache.avro.RandomData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.utils.AvroUtils;
import org.apache.beam.sdk.util.AvroGenerators.RecordSchemaGenerator;
import org.hamcrest.BaseMatcher;
import org.hamcrest.Description;
import org.junit.runner.RunWith;

/** Tests for conversion between AVRO records and Beam rows. */
@RunWith(JUnitQuickcheck.class)
public class AvroUtilsTest {

  @Property
  @SuppressWarnings("unchecked")
  public void supportsAnyAvroSchema(
      @From(RecordSchemaGenerator.class) org.apache.avro.Schema avroSchema) {
    // not everything is possible to translate
    assumeThat(avroSchema, not(containsField(AvroUtilsTest::hasArrayOfNullable)));
    assumeThat(avroSchema, not(containsField(AvroUtilsTest::hasNonNullUnion)));

    Schema schema = AvroUtils.toSchema(avroSchema);
    Iterable iterable = new RandomData(avroSchema, 10);
    List<GenericRecord> records = Lists.newArrayList((Iterable<GenericRecord>) iterable);

    for (GenericRecord record : records) {
      AvroUtils.toRowStrict(record, schema);
    }
  }

  public static ContainsField containsField(Function<org.apache.avro.Schema, Boolean> predicate) {
    return new ContainsField(predicate);
  }

  // doesn't work because Beam doesn't have unions, only nullable fields
  public static boolean hasNonNullUnion(org.apache.avro.Schema schema) {
    if (schema.getType() == org.apache.avro.Schema.Type.UNION) {
      final List<org.apache.avro.Schema> types = schema.getTypes();

      if (types.size() == 2) {
        return types.get(0).getType() != org.apache.avro.Schema.Type.NULL;
      } else {
        return true;
      }
    }

    return false;
  }

  // doesn't work because Beam doesn't support arrays of nullable types
  public static boolean hasArrayOfNullable(org.apache.avro.Schema schema) {
    if (schema.getType() == org.apache.avro.Schema.Type.ARRAY) {
      org.apache.avro.Schema elementType = schema.getElementType();
      if (elementType.getType() == org.apache.avro.Schema.Type.UNION) {
        return elementType.getTypes().get(0).getType() == org.apache.avro.Schema.Type.NULL;
      }
    }

    return false;
  }

  static class ContainsField extends BaseMatcher<org.apache.avro.Schema> {

    private final Function<org.apache.avro.Schema, Boolean> predicate;

    ContainsField(final Function<org.apache.avro.Schema, Boolean> predicate) {
      this.predicate = predicate;
    }

    @Override
    public boolean matches(final Object item0) {
      if (!(item0 instanceof org.apache.avro.Schema)) {
        return false;
      }

      org.apache.avro.Schema item = (org.apache.avro.Schema) item0;

      if (predicate.apply(item)) {
        return true;
      }

      switch (item.getType()) {
        case RECORD:
          return item.getFields().stream().anyMatch(x -> matches(x.schema()));

        case UNION:
          return item.getTypes().stream().anyMatch(this::matches);

        case ARRAY:
          return matches(item.getElementType());

        case MAP:
          return matches(item.getValueType());

        default:
          return false;
      }
    }

    @Override
    public void describeTo(final Description description) {}
  }
}
