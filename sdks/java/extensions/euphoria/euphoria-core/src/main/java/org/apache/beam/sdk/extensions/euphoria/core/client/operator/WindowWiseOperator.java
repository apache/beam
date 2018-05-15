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
package org.apache.beam.sdk.extensions.euphoria.core.client.operator;

import javax.annotation.Nullable;
import org.apache.beam.sdk.extensions.euphoria.core.annotation.audience.Audience;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Window;
import org.apache.beam.sdk.extensions.euphoria.core.client.dataset.windowing.Windowing;
import org.apache.beam.sdk.extensions.euphoria.core.client.flow.Flow;

/** Operator working on some context. */
@Audience(Audience.Type.INTERNAL)
public abstract class WindowWiseOperator<InputT, WindowInT, OutputT, W extends Window<W>>
    extends Operator<InputT, OutputT> implements WindowAware<WindowInT, W> {

  @Nullable protected Windowing<WindowInT, W> windowing;

  public WindowWiseOperator(String name, Flow flow, @Nullable Windowing<WindowInT, W> windowing) {
    super(name, flow);
    this.windowing = windowing;
  }

  @Nullable
  @Override
  public Windowing<WindowInT, W> getWindowing() {
    return windowing;
  }
}
