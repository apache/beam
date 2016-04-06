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
/**
 * Provides a simple, powerful model for building both batch and
 * streaming parallel data processing
 * {@link com.google.cloud.dataflow.sdk.Pipeline}s.
 *
 * <p>To use the Google Cloud Dataflow SDK, you build a
 * {@link com.google.cloud.dataflow.sdk.Pipeline}, which manages a graph of
 * {@link com.google.cloud.dataflow.sdk.transforms.PTransform}s
 * and the {@link com.google.cloud.dataflow.sdk.values.PCollection}s that
 * the PTransforms consume and produce.
 *
 * <p>Each Pipeline has a
 * {@link com.google.cloud.dataflow.sdk.runners.PipelineRunner} to specify
 * where and how it should run after pipeline construction is complete.
 *
 */
package com.google.cloud.dataflow.sdk;
