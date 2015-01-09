/*******************************************************************************
 * Copyright (C) 2014 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 ******************************************************************************/

package com.google.cloud.dataflow.sdk.util.common.worker;

/**
 * An interface for sources which can perform operations on source specifications, such as
 * splitting the source and computing its metadata. See {@code SourceOperationRequest} for details.
 */
public interface SourceFormat {
  /**
   * Performs an operation on the specification of a source.
   * See {@code SourceOperationRequest} for details.
   */
  public OperationResponse performSourceOperation(OperationRequest operation) throws Exception;

  /**
   * A representation of an operation on the specification of a source,
   * e.g. splitting a source into shards, getting the metadata of a source,
   * etc.
   *
   * <p> The common worker framework does not interpret instances of
   * this interface.  But a tool-specific framework can make assumptions
   * about the implementation, and so the concrete Source subclasses used
   * by a tool-specific framework should match.
   */
  public interface OperationRequest {}

  /**
   * A representation of the result of a SourceOperationRequest.
   *
   * <p> See the comment on {@link OperationRequest} for how instances of this
   * interface are used by the rest of the framework.
   */
  public interface OperationResponse {}

  /**
   * A representation of a specification of a source.
   *
   * <p> See the comment on {@link OperationRequest} for how instances of this
   * interface are used by the rest of the framework.
   */
  public interface SourceSpec {}
}
