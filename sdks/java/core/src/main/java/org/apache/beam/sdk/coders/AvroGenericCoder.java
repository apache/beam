package org.apache.beam.sdk.coders;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericRecord;

/** AvroCoder specialisation for GenericRecord */
public class AvroGenericCoder extends AvroCoder<GenericRecord> {
  AvroGenericCoder(Schema schema) {
    super(GenericRecord.class, schema);
  }

  public static AvroGenericCoder of(Schema schema) {
    return new AvroGenericCoder(schema);
  }
}
