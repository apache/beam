package org.beam.dsls.sql.schema;

import com.fasterxml.jackson.annotation.JsonCreator;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderException;
import org.apache.beam.sdk.coders.StandardCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarIntCoder;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.beam.sdk.coders.Coder.Context;

/**
 * A {@link Coder} for {@link BeamSQLRecordType}.
 *
 */
public class BeamSQLRecordTypeCoder extends StandardCoder<BeamSQLRecordType> {
  private static final StringUtf8Coder stringCoder = StringUtf8Coder.of();
  private static final VarIntCoder intCoder = VarIntCoder.of();

  private static final BeamSQLRecordTypeCoder INSTANCE = new BeamSQLRecordTypeCoder();
  private BeamSQLRecordTypeCoder(){}

  @JsonCreator
  public static BeamSQLRecordTypeCoder of() {
    return INSTANCE;
  }

  @Override
  public void encode(BeamSQLRecordType value, OutputStream outStream,
      org.apache.beam.sdk.coders.Coder.Context context) throws CoderException, IOException {
    Context nested = context.nested();
    intCoder.encode(value.size(), outStream, nested);
    for(String fieldName : value.getFieldsName()){
      stringCoder.encode(fieldName, outStream, nested);
    }
    for(SqlTypeName fieldType : value.getFieldsType()){
      stringCoder.encode(fieldType.name(), outStream, nested);
    }
    outStream.flush();
  }

  @Override
  public BeamSQLRecordType decode(InputStream inStream,
      org.apache.beam.sdk.coders.Coder.Context context) throws CoderException, IOException {
    BeamSQLRecordType typeRecord = new BeamSQLRecordType();
    Context nested = context.nested();
    int size = intCoder.decode(inStream, nested);
    for(int idx=0; idx<size; ++idx){
      typeRecord.getFieldsName().add(stringCoder.decode(inStream, nested));
    }
    for(int idx=0; idx<size; ++idx){
      typeRecord.getFieldsType().add(SqlTypeName.valueOf(stringCoder.decode(inStream, nested)));
    }
    return typeRecord;
  }

  @Override
  public List<? extends Coder<?>> getCoderArguments() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public void verifyDeterministic()
      throws org.apache.beam.sdk.coders.Coder.NonDeterministicException {
    // TODO Auto-generated method stub

  }

}
