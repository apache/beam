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

public struct EmptyPTransform: _PrimitivePTransform {
    public init() {}
}

public struct _ConditionalPTransform<TruePTransform, FalsePTransform>: _PrimitivePTransform
    where TruePTransform: PTransform, FalsePTransform: PTransform
{
    enum Storage {
        case trueTransform(TruePTransform)
        case falseTransform(FalsePTransform)
    }

    let storage: Storage
}

public extension _ConditionalPTransform {
    var children: [AnyPTransform] {
        switch storage {
        case let .trueTransform(transform):
            return [AnyPTransform(transform)]
        case let .falseTransform(transform):
            return [AnyPTransform(transform)]
        }
    }
}

@resultBuilder
public enum PTransformBuilder {
    public static func buildBlock() -> EmptyPTransform {
        EmptyPTransform()
    }

    public static func buildBlock<Transform>(_ transform: Transform) -> Transform where Transform: PTransform {
        transform
    }

    public static func buildEither<TrueT, FalseT>(first: TrueT) -> _ConditionalPTransform<TrueT, FalseT> where TrueT: PTransform, FalseT: PTransform {
        .init(storage: .trueTransform(first))
    }

    public static func buildEither<TrueT, FalseT>(second: FalseT) -> _ConditionalPTransform<TrueT, FalseT> where TrueT: PTransform, FalseT: PTransform {
        .init(storage: .falseTransform(second))
    }
}

public extension PTransformBuilder {
    static func buildBlock<T0, T1>(_ t0: T0, _ t1: T1) -> TuplePTransform<(T0, T1)> where T0: PTransform, T1: PTransform {
        TuplePTransform<(T0, T1)>(t0, t1)
    }
}
