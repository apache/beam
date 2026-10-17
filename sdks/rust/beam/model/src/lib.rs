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

//! Generated Protobuf and gRPC definitions for Apache Beam.

#![allow(clippy::all)]
#![allow(unknown_lints)]
#![allow(unused_imports)]
#![allow(non_snake_case)]

pub mod org {
    pub mod apache {
        pub mod beam {
            pub mod model {
                pub mod pipeline {
                    pub mod v1 {
                        tonic::include_proto!("org.apache.beam.model.pipeline.v1");
                    }
                }
                pub mod fn_execution {
                    pub mod v1 {
                        tonic::include_proto!("org.apache.beam.model.fn_execution.v1");
                    }
                }
                pub mod job_management {
                    pub mod v1 {
                        tonic::include_proto!("org.apache.beam.model.job_management.v1");
                    }
                }
                pub mod expansion {
                    pub mod v1 {
                        tonic::include_proto!("org.apache.beam.model.expansion.v1");
                    }
                }
            }
        }
    }
}

// Short top-level paths to the versioned proto modules.
pub mod pipeline {
    pub use crate::org::apache::beam::model::pipeline::v1::*;
}

pub mod fn_execution {
    pub use crate::org::apache::beam::model::fn_execution::v1::*;
}

pub mod job_management {
    pub use crate::org::apache::beam::model::job_management::v1::*;
}

pub mod expansion {
    pub use crate::org::apache::beam::model::expansion::v1::*;
}
