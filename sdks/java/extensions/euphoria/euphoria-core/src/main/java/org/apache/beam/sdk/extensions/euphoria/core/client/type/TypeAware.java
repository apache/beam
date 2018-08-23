/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.beam.sdk.extensions.euphoria.core.client.type;

import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Named;
import org.apache.beam.sdk.extensions.euphoria.core.client.operator.base.Operator;
import org.apache.beam.sdk.values.TypeDescriptor;

/**
 * A collection of interfaces which allows access to {@link TypeDescriptor types} of implementing
 * {@link Operator Operators} properties.
 *
 * <p>Note that there is no input typing interface. That is on purpose since all the transformations
 * are chained together and type of input(s) equals to types of previous transform output(s). Source
 * transforms needs to know which type of elements are producing explicitly.
 */
public class TypeAware {

  /** Returns {@link TypeDescriptor} of this operator output type. */
  public interface Output<OutputT> extends Named {

    TypeDescriptor<OutputT> getOutputType();
  }

  /** Returns {@link TypeDescriptor} of this operator key type. */
  public interface Key<KeyT> extends Named {

    TypeDescriptor<KeyT> getKeyType();
  }

  /** Returns {@link TypeDescriptor} of this operator value type. */
  public interface Value<ValueT> extends Named {

    TypeDescriptor<ValueT> getValueType();
  }
}
