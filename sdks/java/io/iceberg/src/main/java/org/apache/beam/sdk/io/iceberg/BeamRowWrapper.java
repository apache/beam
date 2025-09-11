package org.apache.beam.sdk.io.iceberg;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.logicaltypes.DateTime;
import org.apache.beam.sdk.schemas.logicaltypes.Time;
import org.apache.beam.sdk.schemas.logicaltypes.Date;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions;
import org.apache.iceberg.StructLike;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;
import org.apache.iceberg.util.UUIDUtil;

import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static org.apache.beam.sdk.util.Preconditions.checkStateNotNull;

public class BeamRowWrapper implements StructLike {
  private final FieldType[] types;
  private final BiFunction<Row, Integer, ?>[] getters;
  private Row row;
  public BeamRowWrapper(Schema schema, Types.StructType icebergSchema) {
    Preconditions.checkArgument(
      schema.getFieldCount() == icebergSchema.fields().size(),
      "Invalid length: Spark struct type (%s) != Iceberg struct type (%s)",
      schema.getFieldCount(),
      icebergSchema.fields().size());
    this.types = schema.getFields().stream().map(Schema.Field::getType).collect(Collectors.toList()).toArray(FieldType[]::new);
    this.getters = new BiFunction[types.length];
    for (int i = 0; i < types.length; i++) {
      getters[i] = buildGetter(icebergSchema.fields().get(i).type(), types[i]);
    }
  }

  public BeamRowWrapper wrap(Row row) {
    this.row = row;
    return this;
  }

  @Override
  public int size() {
    return types.length;
  }

  @Override
  public <T> T get(int pos, Class<T> javaClass) {
    if (row.getValue(pos) == null) {
      return null;
    } else if (getters[pos] != null) {
      return javaClass.cast(getters[pos].apply(row, pos));
    }

    return javaClass.cast(row.getValue(pos));
  }

  @Override
  public <T> void set(int pos, T value) {
    throw new UnsupportedOperationException(
      "Setting fields in BeamRowWrapper is unsupported because Beam Rows are immutable");
  }

  private static BiFunction<Row, Integer, ?> buildGetter(Type icebergType, FieldType beamType) {
    switch (beamType.getTypeName()) {
      case BYTE:
        return Row::getByte;
      case INT16:
        return Row::getInt16;
      case INT32:
        return Row::getInt32;
      case INT64:
        return Row::getInt64;
      case STRING:
        return Row::getString;
      case BYTES:
        if (icebergType.typeId() == Type.TypeID.UUID) {
          return (row, pos) -> UUIDUtil.convert(row.getBytes(pos));
        } else {
          return (row, pos) -> ByteBuffer.wrap(row.getBytes(pos));
        }
      case DECIMAL:
        return Row::getDecimal;
      case DATETIME:
        return Row::getDateTime;
      case LOGICAL_TYPE:
        switch (beamType.getLogicalType().getIdentifier()) {
          case DateTime.IDENTIFIER:
            return (row, pos) -> row.getLogicalTypeValue(pos, LocalDateTime.class);
          case Time.IDENTIFIER:
            return (row, pos) -> row.getLogicalTypeValue(pos, LocalTime.class);
          case Date.IDENTIFIER:
            return (row, pos) -> row.getLogicalTypeValue(pos, LocalDate.class);
          default:
            throw new RuntimeException("Unsupported Beam logical type " + beamType.getLogicalType().getIdentifier());
        }
      case ROW:
        Schema innerBeamSchema = checkStateNotNull(beamType.getRowSchema());
        Types.StructType structType = (Types.StructType) icebergType;

        BeamRowWrapper nestedWrapper = new BeamRowWrapper(innerBeamSchema, structType);
        return (row, pos) -> nestedWrapper.wrap(row.getRow(pos));
      default:
        return null;
    }
  }
}
