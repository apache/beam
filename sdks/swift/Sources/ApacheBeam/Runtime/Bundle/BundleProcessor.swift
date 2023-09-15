/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 *  License); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an  AS IS BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import Logging

import Foundation

struct BundleProcessor {
    let log: Logging.Logger

    struct Step {
        let transformId: String
        let fn: SerializableFn
        let inputs: [AnyPCollectionStream]
        let outputs: [AnyPCollectionStream]
        let payload: Data
    }

    let steps: [Step]

    init(id: String,
         descriptor: Org_Apache_Beam_Model_FnExecution_V1_ProcessBundleDescriptor,
         collections: [String: AnyPCollection],
         fns: [String: SerializableFn]) throws
    {
        log = Logging.Logger(label: "BundleProcessor(\(id) \(descriptor.id))")

        var temp: [Step] = []
        var coders = BundleCoderContainer(bundle: descriptor)

        var streams: [String: AnyPCollectionStream] = [:]
        // First make streams for everything in this bundle (maybe I could use the pcollection array for this?)
        for (_, transform) in descriptor.transforms {
            for id in transform.inputs.values {
                if streams[id] == nil {
                    streams[id] = collections[id]!.anyStream
                }
            }
            for id in transform.outputs.values {
                if streams[id] == nil {
                    streams[id] = collections[id]!.anyStream
                }
            }
        }

        for (transformId, transform) in descriptor.transforms {
            let urn = transform.spec.urn
            // Map the input and output streams in the correct order
            let inputs = transform.inputs.sorted().map { streams[$0.1]! }
            let outputs = transform.outputs.sorted().map { streams[$0.1]! }

            if urn == "beam:runner:source:v1" {
                let remotePort = try RemoteGrpcPort(serializedData: transform.spec.payload)
                let coder = try Coder.of(name: remotePort.coderID, in: coders)
                log.info("Source '\(transformId)','\(transform.uniqueName)' \(remotePort) \(coder)")
                try temp.append(Step(
                    transformId: transform.uniqueName == "" ? transformId : transform.uniqueName,
                    fn: Source(client: .client(for: ApiServiceDescriptor(proto: remotePort.apiServiceDescriptor), worker: id), coder: coder),
                    inputs: inputs,
                    outputs: outputs,
                    payload: Data()
                ))
            } else if urn == "beam:runner:sink:v1" {
                let remotePort = try RemoteGrpcPort(serializedData: transform.spec.payload)
                let coder = try Coder.of(name: remotePort.coderID, in: coders)
                log.info("Sink '\(transformId)','\(transform.uniqueName)' \(remotePort) \(coder)")
                try temp.append(Step(
                    transformId: transform.uniqueName == "" ? transformId : transform.uniqueName,
                    fn: Sink(client: .client(for: ApiServiceDescriptor(proto: remotePort.apiServiceDescriptor), worker: id), coder: coder),
                    inputs: inputs,
                    outputs: outputs,
                    payload: Data()
                ))

            } else if urn == "beam:transform:pardo:v1" {
                let pardoPayload = try Org_Apache_Beam_Model_Pipeline_V1_ParDoPayload(serializedData: transform.spec.payload)
                if let fn = fns[transform.uniqueName] {
                    temp.append(Step(transformId: transform.uniqueName,
                                     fn: fn,
                                     inputs: inputs,
                                     outputs: outputs,
                                     payload: pardoPayload.doFn.payload))
                } else {
                    log.warning("Unable to map \(transform.uniqueName) to a known SerializableFn. Will be skipped during processing.")
                }
            } else {
                log.warning("Unable to map \(urn). Will be skipped during processing.")
            }
        }
        steps = temp
    }

    public func process(instruction: String, responder: AsyncStream<Org_Apache_Beam_Model_FnExecution_V1_InstructionResponse>.Continuation) async {
        _ = await withThrowingTaskGroup(of: (String, String).self) { group in
            log.info("Starting bundle processing for \(instruction)")
            var count: Int = 0
            do {
                for step in steps {
                    log.info("Starting Task \(step.transformId)")
                    let context = SerializableFnBundleContext(instruction: instruction, transform: step.transformId, payload: step.payload, log: log)
                    group.addTask {
                        try await step.fn.process(context: context, inputs: step.inputs, outputs: step.outputs)
                    }
                    count += 1
                }
                var finished = 0
                for try await (instruction, transform) in group {
                    finished += 1
                    log.info("Task Completed (\(instruction),\(transform)) \(finished) of \(count)")
                }
                log.info("All tasks completed for \(instruction)")
                responder.yield(.with {
                    $0.instructionID = instruction
                })
            } catch {
                responder.yield(.with {
                    $0.instructionID = instruction
                    $0.error = "\(error)"
                })
            }
        }
    }
}
