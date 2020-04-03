/*
 * Licensed to the Apache Sextend(template)tware Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy extend(template) the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, sextend(template)tware
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static LoadTestConfig.extendTemplate


class SideInputTestSuite {
    static def configurations = { LoadTestConfig template -> [
        extendTemplate(template) {
            title 'SideInput 2MB 100 byte records: global window'
            pipelineOptions {
                inputOptions {
                    numRecords 20000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'iter',
                    side_input_size: 2000,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 2MB 100 byte records: 1000 windows'
            pipelineOptions {
                inputOptions {
                    numRecords 20000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'iter',
                    side_input_size: 2000,
                    window_count: 1000,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 200MB 100 byte records: small list'
            pipelineOptions {
                inputOptions {
                    numRecords 2000000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'list',
                    side_input_size: 2000,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 200MB 100 byte records: large list, 16 workers'
            pipelineOptions {
                inputOptions {
                    numRecords 2000000
                    keySize 10
                    valueSize 90
                }
                numWorkers 16
                parallelism 16
                specificParameters([
                    side_input_type: 'list',
                    side_input_size: 20000,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 200MB 100 byte records: large list'
            pipelineOptions {
                inputOptions {
                    numRecords 2000000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'list',
                    side_input_size: 20000,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 200MB 100 kilobyte records: large keys'
            pipelineOptions {
                inputOptions {
                    numRecords 200
                    keySize 100000
                    valueSize 900000
                }
                specificParameters([
                    side_input_type: 'iter',
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 200MB 100 byte records: dictionary 1% random lookup'
            pipelineOptions {
                inputOptions {
                    numRecords 2000000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'dict',
                    side_input_size: 2000,
                    access_percentage: 1,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 200MB 100 byte records: dictionary 99% random lookup'
            pipelineOptions {
                inputOptions {
                    numRecords 2000000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'dict',
                    side_input_size: 2000,
                    access_percentage: 99,
                ])
            }
        },
        extendTemplate(template) {
            title 'SideInput 2MB 100 byte records: iterable 1% first lookup'
            pipelineOptions {
                inputOptions {
                    numRecords 20000
                    keySize 10
                    valueSize 90
                }
                specificParameters([
                    side_input_type: 'iter',
                    side_input_size: 2000,
                    access_percentage: 1,
                ])
            }
        },
    ]}
}
