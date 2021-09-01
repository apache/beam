/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.standalone.data;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import io.delta.standalone.types.StructField;
import io.delta.standalone.types.StructType;

/**
 * Represents one row of data containing a non-empty collection of {@code fieldName - value} pairs.
 * It provides APIs to allow retrieval of values through {@code fieldName} lookup. For example,
 *
 * <pre>{@code
 *   if (row.isNullAt("int_field")) {
 *     // handle the null value.
 *   } else {
 *     int x = getInt("int_field");
 *   }
 * }</pre>
 *
 * @see StructType StructType
 * @see StructField StructField
 */
public interface RowRecord {

    /**
     * @return the schema for this {@link RowRecord}
     */
    StructType getSchema();

    /**
     * @return the number of elements in this {@link RowRecord}
     */
    int getLength();

    /**
     * @param fieldName  name of field/column, not {@code null}
     * @return whether the value of field {@code fieldName} is {@code null}
     */
    boolean isNullAt(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive int.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive int
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    int getInt(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive long.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive long
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    long getLong(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive byte.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive byte
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    byte getByte(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive short.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive short
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    short getShort(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive boolean.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive boolean
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    boolean getBoolean(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive float.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive float
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    float getFloat(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a primitive double.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a primitive double
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if {@code null} data value read
     */
    double getDouble(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code String} object.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a String object. {@code null} only if
     *         {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if field is not nullable and {@code null} data value read
     */
    String getString(String fieldName);

    /**
     * Retrieves value from data record and returns the value as binary (byte array).
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as binary (byte array). {@code null} only if
     *         {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if field is not nullable and {@code null} data value read
     */
    byte[] getBinary(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code java.math.BigDecimal}.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as java.math.BigDecimal. {@code null} only if
     *         {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if field is not nullable and {@code null} data value read
     */
    BigDecimal getBigDecimal(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code java.sql.Timestamp}.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as java.sql.Timestamp. {@code null} only if
     *         {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if field is not nullable and {@code null} data value read
     */
    Timestamp getTimestamp(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code java.sql.Date}.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as java.sql.Date. {@code null} only if
     *         {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException if field is not nullable and {@code null} data value read
     */
    Date getDate(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code RowRecord} object.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @return the value for field {@code fieldName} as a {@code RowRecord} object.
     *         {@code null} only if {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException for this field or any nested field, if that field is not
     *                              nullable and {@code null} data value read
     */
    RowRecord getRecord(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code java.util.List<T>} object.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @param <T>  element type
     * @return the value for field {@code fieldName} as a {@code java.util.List<T>} object.
     *         {@code null} only if {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException for this field or any element field, if that field is not
     *                              nullable and {@code null} data value read
     */
    <T> List<T> getList(String fieldName);

    /**
     * Retrieves value from data record and returns the value as a {@code java.util.Map<K, V>}
     * object.
     *
     * @param fieldName  name of field/column, not {@code null}
     * @param <K>  key type
     * @param <V>  value type
     * @return the value for field {@code fieldName} as a {@code java.util.Map<K, V>} object.
     *         {@code null} only if {@code null} value read and field is nullable.
     * @throws IllegalArgumentException if {@code fieldName} does not exist in this schema
     * @throws ClassCastException if data type does not match
     * @throws NullPointerException for this field or any key/value field, if that field is not
     *                              nullable and {@code null} data value read
     */
    <K, V> Map<K, V> getMap(String fieldName);
}
