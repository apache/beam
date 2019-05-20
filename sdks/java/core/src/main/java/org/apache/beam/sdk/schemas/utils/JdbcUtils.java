package org.apache.beam.sdk.schemas.utils;

import org.apache.beam.sdk.schemas.LogicalTypes;
import org.apache.beam.sdk.schemas.Schema;
import org.joda.time.Instant;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

public class JdbcUtils {

    private static final Map<Integer, String > RS_TYPE_MAPPING = new HashMap<>();

    /**
     *
     */
    static {
        RS_TYPE_MAPPING.put(Types.BIT, "BIT");
        RS_TYPE_MAPPING.put(Types.TINYINT, "TINYINT");
        RS_TYPE_MAPPING.put(Types.BIGINT, "BIGINT");
        RS_TYPE_MAPPING.put(Types.LONGVARBINARY, "LONGVARBINARY");
        RS_TYPE_MAPPING.put(Types.VARBINARY, "VARBINARY");
        RS_TYPE_MAPPING.put(Types.BINARY, "BINARY");
        RS_TYPE_MAPPING.put(Types.LONGVARCHAR, "LONGVARCHAR");
        RS_TYPE_MAPPING.put(Types.NULL, "NULL");
        RS_TYPE_MAPPING.put(Types.CHAR, "CHAR");
        RS_TYPE_MAPPING.put(Types.NUMERIC, "NUMERIC");
        RS_TYPE_MAPPING.put(Types.DECIMAL, "DECIMAL");
        RS_TYPE_MAPPING.put(Types.INTEGER, "INTEGER");
        RS_TYPE_MAPPING.put(Types.SMALLINT, "SMALLINT");
        RS_TYPE_MAPPING.put(Types.FLOAT, "FLOAT");
        RS_TYPE_MAPPING.put(Types.REAL, "REAL");
        RS_TYPE_MAPPING.put(Types.DOUBLE, "DOUBLE");
        RS_TYPE_MAPPING.put(Types.VARCHAR, "VARCHAR");
        RS_TYPE_MAPPING.put(Types.DATE, "DATE");
        RS_TYPE_MAPPING.put(Types.TIME, "TIME");
        RS_TYPE_MAPPING.put(Types.TIMESTAMP, "TIMESTAMP");
        RS_TYPE_MAPPING.put(Types.OTHER, "OTHER");
    }

    private static Schema.FieldType fromFieldType(String typeName) { //List<TableFieldSchema> nestedFields
        switch (typeName) {
            case "VARCHAR":
            case "CHAR":
            case "LONGVARCHAR":
            case "CLOB":
                return Schema.FieldType.STRING;
            case "BIT":
                return Schema.FieldType.BOOLEAN;
            case "NUMERIC":
                return Schema.FieldType.INT64;
            case "TINYINT":
                return Schema.FieldType.BYTE;
            case "SMALLINT":
                return Schema.FieldType.INT16;
            case "INTEGER":
                return Schema.FieldType.INT32;
            case "BIGINT":
                return Schema.FieldType.INT64;
            case "REAL":
            case "FLOAT":
                return Schema.FieldType.FLOAT;
            case "DOUBLE":
                return Schema.FieldType.DOUBLE;
            case "VARBINARY":
            case "BINARY":
            case "BLOB":
                return Schema.FieldType.BYTES;
            case "DATE":
                return Schema.FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlDateType", "", Schema.FieldType.DATETIME) {});
            case "TIME":
                return Schema.FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlTimeType", "", Schema.FieldType.DATETIME) {});
            case "TIMESTAMP":
                return Schema.FieldType.logicalType(
                        new LogicalTypes.PassThroughLogicalType<Instant>(
                                "SqlTimestampWithLocalTzType", "", Schema.FieldType.DATETIME) {});
            case "DATETIME":
                return Schema.FieldType.DATETIME;
//            case "REF":
//                return Schema.FieldType.?
//            case "ARRAY":
//                return Schema.FieldType.?

//            case "STRUCT":
//            case "RECORD":
//                Schema rowSchema = fromTableFieldSchema(nestedFields);
//                return Schema.FieldType.row(rowSchema);
            default:
                return Schema.FieldType.STRING;
        }
    }

    /**  */
    private static Schema fromResultSetMetaData(ResultSetMetaData metaData) throws SQLException  {
        Schema.Builder schemaBuilder = Schema.builder();

        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String type = RS_TYPE_MAPPING.get(metaData.getColumnType(i));
            Schema.FieldType fieldType = fromFieldType(type);
            Schema.Field field = Schema.Field.of(metaData.getColumnName(i), fieldType).withNullable(metaData.isNullable(i) != ResultSetMetaData.columnNoNulls);
            schemaBuilder.addField(field);
        }
        return schemaBuilder.build();
    }

    /** Convert a JDBC {@link ResultSet} to a Beam {@link Schema}. */
    public static Schema fromResultSetToSchema(ResultSet resultSet) throws SQLException {
        return fromResultSetMetaData(resultSet.getMetaData());
    }

}
