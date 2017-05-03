/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.beam.sdk.io.gcp.bigquery;

import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.Lists;
import java.io.Serializable;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;
import javax.annotation.Nullable;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.transforms.DoFn.ProcessContext;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.ValueInSingleWindow;

/**
 * This class provides the most general way of specifying dynamic BigQuery table destinations.
 * Destinations can be extracted from the input element, and stored as a custom type. Mappings
 * are provided to convert the destination into a BigQuery table reference and a BigQuery schema.
 * The class can read a side input from another PCollection while performing these mappings.
 *
 * <p>For example, consider a PCollection of events, each containing a user-id field. You want to
 * write each user's events to a separate table with a separate schema per user. Since the user-id
 * field is a string, you will represent the destination as a string.
 * <pre>{@code
 * events.apply(BigQueryIO.<UserEvent>write()
 *  .to(new DynamicDestinations<UserEvent, String>() {
 *        public String getDestination(ValueInSingleWindow<String> element) {
 *          return element.getValue().getUserId();
 *        }
 *        public TableDestination getTable(String user) {
 *          return new TableDestination(tableForUser(user), "Table for user " + user);
 *        }
 *        public TableSchema getSchema(String user) {
 *          return tableSchemaForUser(user);
 *        }
 *      })
 *  .withFormatFunction(new SerializableFunction<UserEvent, TableRow>() {
 *     public TableRow apply(UserEvent event) {
 *       return convertUserEventToTableRow(event);
 *     }
 *   }));
 * }</pre>
 *
 * <p>An instance of {@link DynamicDestinations} can also request a side input value that can be
 *  examined from inside {@link #getTable} and {@link #getSchema}. It must return a list of the
 *  side inputs it needs access to in {@link #getSideInputs()}. The value can be examined via the
 *  {@link DynamicDestinations.SideInputAccessor} parameter passed into {@link #getTable}
 *  and {@link #getSchema}.
 *
 * <p>{@code DestinationT} is expected to provide proper hash and equality members. Ideally it will
 * be a compact type with an efficient coder, as these objects may be used as a key in a
 * {@link org.apache.beam.sdk.transforms.GroupByKey}.
 */
public abstract class DynamicDestinations<T, DestinationT> implements Serializable {
  /**
   * Returns the materialized value of the side input. Can be used by concrete
   * {@link DynamicDestinations} instances in {@link #getSchema} or {@link #getTable}.
   */
  public static class SideInputAccessor {
    private ProcessContext processContext;
    SideInputAccessor(ProcessContext processContext) {
      this.processContext = processContext;
    }

    public <SideInputT> SideInputT getSideInputValue(PCollectionView<SideInputT> sideInput)
        throws IllegalStateException {
      @SuppressWarnings("unchecked")
      SideInputT materialized = (SideInputT)  processContext.sideInput(sideInput);
      return materialized;
    }
  }

  /**
   * Specifies that this object needs access to one or more side inputs. This side input must be
   * globally windowed, as it will be accessed from the global window.
   */
  public List<PCollectionView<?>> getSideInputs() {
    return Lists.newArrayList();
  }

  /**
   * Returns an object that represents at a high level which table is being written to. May not
   * return null.
   */
  public abstract DestinationT getDestination(ValueInSingleWindow<T> element);

  /**
   * Returns the coder for {@link DestinationT}. If this is not overridden, then
   * {@link BigQueryIO} will look in the coder registry for a suitable coder. This must be a
   * deterministic coder, as {@link DestinationT} will be used as a key type in a
   * {@link org.apache.beam.sdk.transforms.GroupByKey}.
   */
  @Nullable
  public Coder<DestinationT> getDestinationCoder() {
    return null;
  }

  /**
   * Returns a {@link TableDestination} object for the destination. May not return null.
   */
  public abstract TableDestination getTable(DestinationT destination,
                                            SideInputAccessor sideInputAccessor);

  /**
   * Returns the table schema for the destination. May not return null.
   */
  public abstract TableSchema getSchema(DestinationT destination,
                                        SideInputAccessor sideInputAccessor);


  // Gets the destination coder. If the user does not provide one, try to find one in the coder
  // registry. If no coder can be found, throws CannotProvideCoderException.
  Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
      throws CannotProvideCoderException {
    Coder<DestinationT> destinationCoder = getDestinationCoder();
    // If dynamicDestinations doesn't provide a coder, try to find it in the coder registry.
    // We must first use reflection to figure out what the type parameter is.
    Type superclass = getClass().getGenericSuperclass();
    while (destinationCoder == null) {
      if (superclass instanceof ParameterizedType) {
        ParameterizedType parameterized = (ParameterizedType) superclass;
        if (parameterized.getRawType() == DynamicDestinations.class) {
          // DestinationT is the second parameter.
          Type parameter = parameterized.getActualTypeArguments()[1];
          @SuppressWarnings("unchecked")
          Class<DestinationT> parameterClass = (Class<DestinationT>) parameter;
          destinationCoder = registry.getDefaultCoder(parameterClass);
          if (destinationCoder == null) {
            throw new CannotProvideCoderException("Could not find a coder for destination type "
                + parameterClass);
          }
          break;
        }
      }
      superclass = ((Class) superclass).getGenericSuperclass();
    }
    return destinationCoder;
  }
}
