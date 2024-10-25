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

import static org.apache.beam.sdk.values.TypeDescriptors.extractFromTypeParameters;
import static org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.base.Preconditions.checkState;

import com.google.api.services.bigquery.model.TableConstraints;
import com.google.api.services.bigquery.model.TableSchema;
import java.io.Serializable;
import java.util.List;
import org.apache.beam.sdk.coders.CannotProvideCoderException;
import org.apache.beam.sdk.coders.Coder;
import org.apache.beam.sdk.coders.CoderRegistry;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.PCollectionView;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.beam.sdk.values.ValueInSingleWindow;
import org.apache.beam.vendor.guava.v32_1_2_jre.com.google.common.collect.Lists;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * This class provides the most general way of specifying dynamic BigQuery table destinations.
 * Destinations can be extracted from the input element, and stored as a custom type. Mappings are
 * provided to convert the destination into a BigQuery table reference and a BigQuery schema. The
 * class can read side inputs while performing these mappings.
 *
 * <p>For example, consider a PCollection of events, each containing a user-id field. You want to
 * write each user's events to a separate table with a separate schema per user. Since the user-id
 * field is a string, you will represent the destination as a string.
 *
 * <pre>{@code
 * events.apply(BigQueryIO.<UserEvent>write()
 *  .to(new DynamicDestinations<UserEvent, String>() {
 *        public String getDestination(ValueInSingleWindow<UserEvent> element) {
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
 * <p>An instance of {@link DynamicDestinations} can also use side inputs using {@link
 * #sideInput(PCollectionView)}. The side inputs must be present in {@link #getSideInputs()}. Side
 * inputs are accessed in the global window, so they must be globally windowed.
 *
 * <p>{@code DestinationT} is expected to provide proper hash and equality members. Ideally it will
 * be a compact type with an efficient coder, as these objects may be used as a key in a {@link
 * org.apache.beam.sdk.transforms.GroupByKey}.
 */
public abstract class DynamicDestinations<T, DestinationT> implements Serializable {
  interface SideInputAccessor {
    <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view);
  }

  private transient @Nullable SideInputAccessor sideInputAccessor;
  private transient @Nullable PipelineOptions options;

  static class SideInputAccessorViaProcessContext implements SideInputAccessor {
    private DoFn<?, ?>.ProcessContext processContext;

    SideInputAccessorViaProcessContext(DoFn<?, ?>.ProcessContext processContext) {
      this.processContext = processContext;
    }

    @Override
    public <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
      return processContext.sideInput(view);
    }
  }

  /** Get the current PipelineOptions if set. */
  @Nullable
  PipelineOptions getPipelineOptions() {
    return options;
  }

  /**
   * Specifies that this object needs access to one or more side inputs. This side inputs must be
   * globally windowed, as they will be accessed from the global window.
   */
  public List<PCollectionView<?>> getSideInputs() {
    return Lists.newArrayList();
  }

  /**
   * Returns the value of a given side input. The view must be present in {@link #getSideInputs()}.
   */
  protected final <SideInputT> SideInputT sideInput(PCollectionView<SideInputT> view) {
    checkState(
        getSideInputs().contains(view),
        "View %s not declared in getSideInputs() (%s)",
        view,
        getSideInputs());
    if (sideInputAccessor == null) {
      throw new IllegalStateException("sideInputAccessor (transient field) is null");
    }
    return sideInputAccessor.sideInput(view);
  }

  void setSideInputAccessorFromProcessContext(DoFn<?, ?>.ProcessContext context) {
    this.sideInputAccessor = new SideInputAccessorViaProcessContext(context);
    this.options = context.getPipelineOptions();
  }

  /**
   * Returns an object that represents at a high level which table is being written to. May not
   * return null.
   *
   * <p>The method must return a unique object for different destination tables involved over all
   * BigQueryIO write transforms in the same pipeline. See
   * https://github.com/apache/beam/issues/32335 for details.
   */
  public abstract DestinationT getDestination(@Nullable ValueInSingleWindow<T> element);

  /**
   * Returns the coder for {@link DestinationT}. If this is not overridden, then {@link BigQueryIO}
   * will look in the coder registry for a suitable coder. This must be a deterministic coder, as
   * {@link DestinationT} will be used as a key type in a {@link
   * org.apache.beam.sdk.transforms.GroupByKey}.
   */
  public @Nullable Coder<DestinationT> getDestinationCoder() {
    return null;
  }

  /**
   * Returns a {@link TableDestination} object for the destination. May not return null. Return
   * value needs to be unique to each destination: may not return the same {@link TableDestination}
   * for different destinations.
   */
  public abstract TableDestination getTable(DestinationT destination);

  /** Returns the table schema for the destination. */
  public abstract @Nullable TableSchema getSchema(DestinationT destination);

  /**
   * Returns TableConstraints (including primary and foreign key) to be used when creating the
   * table. Note: this is not currently supported when using FILE_LOADS!.
   */
  public @Nullable TableConstraints getTableConstraints(DestinationT destination) {
    return null;
  }

  // Gets the destination coder. If the user does not provide one, try to find one in the coder
  // registry. If no coder can be found, throws CannotProvideCoderException.
  Coder<DestinationT> getDestinationCoderWithDefault(CoderRegistry registry)
      throws CannotProvideCoderException {
    Coder<DestinationT> destinationCoder = getDestinationCoder();
    if (destinationCoder != null) {
      return destinationCoder;
    }
    // If dynamicDestinations doesn't provide a coder, try to find it in the coder registry.
    TypeDescriptor<DestinationT> descriptor =
        extractFromTypeParameters(
            this,
            DynamicDestinations.class,
            new TypeDescriptors.TypeVariableExtractor<
                DynamicDestinations<T, DestinationT>, DestinationT>() {});
    try {
      return registry.getCoder(descriptor);
    } catch (CannotProvideCoderException e) {
      throw new CannotProvideCoderException(
          "Failed to infer coder for DestinationT from type "
              + descriptor
              + ", please provide it explicitly by overriding getDestinationCoder()",
          e);
    }
  }
}
