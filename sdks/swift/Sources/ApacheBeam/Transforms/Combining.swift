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

/// Basic reducers
public extension PCollection {
    func reduce<Result:Codable,K,V>(name:String? = nil,_file:String=#fileID,_line:Int=#line,into:Result,_ accumulator: @Sendable @escaping (V,inout Result) -> Void) -> PCollection<KV<K,Result>> where Of == KV<K,V> {
        return pardo(name:name ?? "\(_file):\(_line)",into) { initialValue,input,output in
            for await (kv,ts,w) in input {
                var result = initialValue
                for v in kv.values {
                    accumulator(v,&result)
                }
                output.emit(KV(kv.key,result),timestamp:ts,window:w)
            }
                
        }
    }
}

/// Convenience functions
public extension PCollection {
    func sum<K,V:Numeric&Codable>(_ name:String? = nil,_file:String=#fileID,_line:Int=#line) -> PCollection<KV<K,V>> where Of == KV<K,V> {
        return reduce(name:name,_file:_file,_line:_line,into: 0,{ a,b in b = b + a })
    }
}
