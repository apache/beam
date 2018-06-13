package org.apache.beam.sdk.schemas;

import static com.google.common.base.Preconditions.checkState;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.schemas.Schema.FieldType;
import org.apache.beam.sdk.schemas.Schema.TypeName;
import org.apache.beam.sdk.schemas.utils.POJOUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.RowWithGetters;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.reflect.FieldValueGetterFactory;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetterFactory;
import org.apache.beam.sdk.values.reflect.PojoValueGetterFactory;
import org.apache.beam.sdk.values.reflect.PojoValueSetterFactory;

public class JavaFieldSchema extends SchemaProvider {
    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
      return POJOUtils.schemaFromClass(typeDescriptor.getRawType());
    }

    // TODO: pull this logic into a base class.

    @Override
    public <T> SerializableFunction<T, Row> toRowFunction(
      TypeDescriptor<T> typeDescriptor) {
      return o -> Row
          .withSchema(schemaFor(typeDescriptor))
          .withFieldValueGetters(fieldValueGetterFactory(), o)
          .build();
    }

  @Override
    public <T> SerializableFunction<Row, T> fromRowFunction(
      TypeDescriptor<T> typeDescriptor) {
      List<FieldValueSetter> setters = fieldValueSetterFactory().createSetters(
          typeDescriptor.getRawType());
      return r -> {
        if (r instanceof RowWithGetters) {
          // Efficient path: simply extract the underlying POJO instead of creating a new one.
          return (T) ((RowWithGetters) r).getGetterTarget();
        } else {
          return fromRow(r, (Class<T>) typeDescriptor.getType());
        }
      };
  }

  private <T> T fromRow(Row row, Class<T> clazz) {
    T object;
    try {
      object = clazz.getDeclaredConstructor().newInstance();
    } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
        | InstantiationException e) {
      throw new RuntimeException("Failed to instantiate object ", e);
    }

    List<FieldValueSetter> setters = fieldValueSetterFactory().createSetters(clazz);
    checkState(setters.size() == row.getFieldCount(),
        "Did not have a matching number of setters and fields.");

    Schema schema = row.getSchema();
    for (int i = 0; i < row.getFieldCount(); ++i) {
      FieldType type = schema.getField(i).getType();
      FieldValueSetter setter = setters.get(i);
      setter.set(object,
          fromValue(
              type,
              row.getValue(i),
              setter.type(),
              setter.elementType(),
              setter.mapKeyType(),
              setter.mapValueType()));
    }
    return object;
  }

  private <T> T fromValue(FieldType type, T value, Type fieldType, Type elemenentType,
                          Type keyType, Type valueType) {
    if (TypeName.ROW.equals(type.getTypeName())) {
      return (T) fromRow((Row) value, (Class) fieldType);
    } else if (TypeName.ARRAY.equals(type.getTypeName())) {
      return (T) fromListValue(type.getCollectionElementType(), (List) value, elemenentType);
    } else if (TypeName.MAP.equals(type.getTypeName())) {
      return (T) fromMapValue(type.getMapKeyType(), type.getMapValueType(), (Map) value,
          keyType, valueType);
    } else {
      return value;
    }
  }

  private <T> List fromListValue(FieldType elementType, List<T> rowList, Type elementClass) {
    List list = Lists.newArrayList();
    for (T element : rowList) {
      list.add(fromValue(elementType, element, elementClass, null, null, null));
    }
    return list;
  }

  private  Map<?, ?> fromMapValue(FieldType keyType, FieldType valueType, Map<?, ?> map,
                                  Type keyClass, Type valueClass) {
      Map newMap = Maps.newHashMap();
      for (Map.Entry<?, ?> entry : map.entrySet()) {
        Object key = fromValue(keyType, entry.getKey(), keyClass, null, null, null);
        Object value = fromValue(valueType, entry.getValue(), valueClass, null, null, null);
        newMap.put(key, value);
      }
      return newMap;
  }

  private FieldValueGetterFactory fieldValueGetterFactory() {
      return new PojoValueGetterFactory();
  }

  private FieldValueSetterFactory fieldValueSetterFactory() {
      return new PojoValueSetterFactory();
  }
}
