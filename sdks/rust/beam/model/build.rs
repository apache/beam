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

use std::path::{Path, PathBuf};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let manifest_dir = PathBuf::from(std::env::var("CARGO_MANIFEST_DIR")?);
    let root_dir = manifest_dir.join("../../../..");

    let pipeline_proto_dir = root_dir.join("model/pipeline/src/main/proto");
    let fn_execution_proto_dir = root_dir.join("model/fn-execution/src/main/proto");
    let job_management_proto_dir = root_dir.join("model/job-management/src/main/proto");

    let proto_sets: &[(&Path, &[&str])] = &[
        (
            &pipeline_proto_dir,
            &[
                "org/apache/beam/model/pipeline/v1/endpoints.proto",
                "org/apache/beam/model/pipeline/v1/schema.proto",
                "org/apache/beam/model/pipeline/v1/metrics.proto",
                "org/apache/beam/model/pipeline/v1/standard_window_fns.proto",
                "org/apache/beam/model/pipeline/v1/beam_runner_api.proto",
                "org/apache/beam/model/pipeline/v1/external_transforms.proto",
            ],
        ),
        (
            &job_management_proto_dir,
            &[
                "org/apache/beam/model/job_management/v1/beam_artifact_api.proto",
                "org/apache/beam/model/job_management/v1/beam_expansion_api.proto",
                "org/apache/beam/model/job_management/v1/beam_job_api.proto",
            ],
        ),
        (
            &fn_execution_proto_dir,
            &[
                "org/apache/beam/model/fn_execution/v1/beam_provision_api.proto",
                "org/apache/beam/model/fn_execution/v1/beam_fn_api.proto",
            ],
        ),
    ];

    let protos: Vec<PathBuf> = proto_sets
        .iter()
        .flat_map(|(dir, files)| files.iter().map(move |file| dir.join(file)))
        .collect();

    let includes = [
        pipeline_proto_dir,
        fn_execution_proto_dir,
        job_management_proto_dir,
    ];

    for proto in &protos {
        println!("cargo:rerun-if-changed={}", proto.display());
    }

    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .compile_protos(&protos, &includes)?;

    Ok(())
}
