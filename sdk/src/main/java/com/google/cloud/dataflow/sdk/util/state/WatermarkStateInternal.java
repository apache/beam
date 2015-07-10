/*
 * Copyright (C) 2015 Google Inc.
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
 */
package com.google.cloud.dataflow.sdk.util.state;

import com.google.cloud.dataflow.sdk.annotations.Experimental;
import com.google.cloud.dataflow.sdk.annotations.Experimental.Kind;

import org.joda.time.Instant;

/**
 * State for holding up the watermark to the minimum of input {@code Instant}s.
 *
 * <p> This is intended for internal use only. The watermark will be held up based on the
 * values that are added and only released when items are cleared.
 */
@Experimental(Kind.STATE)
public interface WatermarkStateInternal extends MergeableState<Instant, Instant> {}
