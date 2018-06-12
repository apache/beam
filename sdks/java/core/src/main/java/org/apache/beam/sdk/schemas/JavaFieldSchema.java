package org.apache.beam.sdk.schemas;

import static com.google.common.base.Preconditions.checkState;

import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.beam.sdk.schemas.utils.POJOUtils;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.reflect.BoundFieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueGetter;
import org.apache.beam.sdk.values.reflect.FieldValueSetter;

public class JavaFieldSchema extends SchemaProvider {
    @Override
    public <T> Schema schemaFor(TypeDescriptor<T> typeDescriptor) {
      return POJOUtils.schemaFromClass(typeDescriptor.getRawType());
    }

    @Override
    public <T> SerializableFunction<T, Row> toRowFunction(
      TypeDescriptor<T> typeDescriptor) {
      List<FieldValueGetter> getters = POJOUtils.getGetters(typeDescriptor.getRawType());
      return o -> Row
          .withSchema(schemaFor(typeDescriptor))
          .addFieldValueGetters(
              getters.stream()
                  .map(g -> new BoundFieldValueGetter<>(g, o))
                  .collect(Collectors.toList()))
          .build();
    }

  @Override
    public <T> SerializableFunction<Row, T> fromRowFunction(
      TypeDescriptor<T> typeDescriptor) {
      // TODO: Implement fast path for the case where the Row simply wraps the POJO.
      List<FieldValueSetter> setters = POJOUtils.getSetters(typeDescriptor.getRawType());
      return r -> {
        T object;
        try {
          object = ((Class<T>)typeDescriptor.getType()).getDeclaredConstructor().newInstance();
        } catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException
            | InstantiationException e) {
          throw new RuntimeException("Failed to instantiate object ", e);
        }
        List<Object> values = r.getValues();
        checkState(setters.size() == values.size(),
        "Did not have a matching number of setters and values");
        for (int i = 0; i < values.size(); ++i) {
          setters.get(i).set(object, values.get(i));
        }
        return object;
      };
  }

  private <T> List<FieldValueGetter> fieldGetters(TypeDescriptor<T> typeDescriptor) {
      return POJOUtils.getGetters(typeDescriptor.getRawType());
  }

}
