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
package org.apache.beam.runners.dataflow.worker;

/**
 * The optimizer may split run the user combiner in 3 separate phases (ADD, MERGE, and EXTRACT), on
 * separate VMs, as it sees fit. The CombinerPhase dictates which DoFn is actually running in the
 * worker.
 */
// TODO: These strings are part of the service definition, and
// should be added into the definition of the ParDoInstruction,
// but the protiary definitions don't allow for enums yet.
public class CombinePhase {
  public static final String ALL = "all";
  public static final String ADD = "add";
  public static final String MERGE = "merge";
  public static final String EXTRACT = "extract";
}
