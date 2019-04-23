package org.apache.beam.sdk.extensions.smb;

import com.google.common.collect.Lists;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.extensions.smb.BucketMetadata.HashType;
import org.apache.beam.sdk.extensions.smb.avro.AvroBucketMetadata;

public class TestUtils {

  static final Schema schema = Schema.createRecord(
      "user", "", "org.apache.beam.sdk.extensions.smb", false,
      Lists.newArrayList(
          new Field("name", Schema.create(Type.STRING), "", null),
          new Field("age", Schema.create(Type.INT), "", null)
      )
  );

  static final Coder<GenericRecord> userCoder = AvroCoder.of(TestUtils.schema);

  static GenericRecord createUserRecord(String name, int age) {
    GenericData.Record result = new GenericData.Record(schema);
    result.put("name", name);
    result.put("age", age);

    return result;
  }

  static AvroBucketMetadata<Integer> tryCreateMetadata(int numBuckets, HashType hashType) {
    try {
      return new AvroBucketMetadata<Integer>(numBuckets, Integer.class, hashType, "age");
    } catch (CannotProvideCoderException e) {
      throw new RuntimeException();
    }
  }

}
