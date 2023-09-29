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

public struct TuplePTransform<T>: _PrimitivePTransform {
    public let value: T
    let _children: [AnyPTransform]

    public init(value: T) {
        self.value = value
        _children = []
    }

    public init(value: T, children: [AnyPTransform]) {
        self.value = value
        _children = children
    }

    init<T0: PTransform, T1: PTransform>(_ t0: T0, _ t1: T1) where T == (T0, T1) {
        value = (t0, t1)
        _children = [AnyPTransform(t0), AnyPTransform(t1)]
    }
}
