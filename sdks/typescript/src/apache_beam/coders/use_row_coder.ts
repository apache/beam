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

import { Reader, Writer } from "protobufjs";
import { AtomicType, Schema } from "../proto/schema";
import { Context } from "./coders";
import { RowCoder } from "./row_coder";

// let schema: Schema = {
//     id: "test",
//     fields: [
//         {
//             name: "x",
//             description: "",
//             type: {
//                 nullable: true,
//                 typeInfo: {
//                     oneofKind: "atomicType",
//                     atomicType: AtomicType.STRING
//                 }
//             },
//             id: 0,
//             encodingPosition: 0,
//             options: [],
//         },
//         {
//             name: "y",
//             description: "",
//             type: {
//                 nullable: true,
//                 typeInfo: {
//                     oneofKind: "rowType",
//                     rowType: {
//                         schema: {
//                             id: "test_inner",
//                             fields: [
//                                 {
//                                     name: "a",
//                                     description: "",
//                                     type: {
//                                         nullable: true,
//                                         typeInfo: {
//                                             oneofKind: "arrayType",
//                                             arrayType: {
//                                                 elementType: {
//                                                     nullable: true,
//                                                     typeInfo: {
//                                                         oneofKind: "atomicType",
//                                                         atomicType: AtomicType.INT32
//                                                     }
//                                                 },
//                                             }
//                                         }
//                                     },
//                                     id: 0,
//                                     encodingPosition: 0,
//                                     options: [],
//                                 },
//                                 {
//                                     name: "b",
//                                     description: "",
//                                     type: {
//                                         nullable: true,
//                                         typeInfo: {
//                                             oneofKind: "atomicType",
//                                             atomicType: AtomicType.STRING
//                                         }
//                                     },
//                                     id: 0,
//                                     encodingPosition: 0,
//                                     options: [],
//                                 }
//                             ],
//                             options: [],
//                             encodingPositionsSet: false
//                         }
//                     }
//                 }
//             },
//             id: 0,
//             encodingPosition: 0,
//             options: [],
//         }
//     ],
//     options: [],
//     encodingPositionsSet: false
// };

let schema: Schema = {
  id: "test",
  fields: [
    {
      name: "constructor",
      description: "",
      type: {
        nullable: true,
        typeInfo: {
          oneofKind: "atomicType",
          atomicType: AtomicType.STRING,
        },
      },
      id: 0,
      encodingPosition: 0,
      options: [],
    },
    {
      name: "args",
      description: "",
      type: {
        nullable: true,
        typeInfo: {
          oneofKind: "rowType",
          rowType: {
            schema: {
              id: "test_inner",
              fields: [
                {
                  name: "arg0",
                  description: "",
                  type: {
                    nullable: true,
                    typeInfo: {
                      oneofKind: "atomicType",
                      atomicType: AtomicType.STRING,
                    },
                  },
                  id: 0,
                  encodingPosition: 0,
                  options: [],
                },
              ],
              options: [],
              encodingPositionsSet: false,
            },
          },
        },
      },
      id: 0,
      encodingPosition: 0,
      options: [],
    },
    {
      name: "kwargs",
      description: "",
      type: {
        nullable: true,
        typeInfo: {
          oneofKind: "rowType",
          rowType: {
            schema: {
              id: "test_inner",
              fields: [
                {
                  name: "suffix",
                  description: "",
                  type: {
                    nullable: true,
                    typeInfo: {
                      oneofKind: "atomicType",
                      atomicType: AtomicType.STRING,
                    },
                  },
                  id: 0,
                  encodingPosition: 0,
                  options: [],
                },
              ],
              options: [],
              encodingPositionsSet: false,
            },
          },
        },
      },
      id: 0,
      encodingPosition: 0,
      options: [],
    },
  ],
  options: [],
  encodingPositionsSet: false,
};

let obj = {
  x: "first",
  y: {
    // a: [1,2,3,4,5],
    b: "third",
  },
};

// let obj = {
//     constructor: 'apache_beam.transforms.fully_qualified_named_transform_test._TestTransform',
//     args: { arg0: 'x' },
//     kwargs: { suffix: 'y' },
// }

let writer = new Writer(),
  // row = RowCoder.OfSchema(schema);
  row = RowCoder.OfJSON(obj);

row.encode(obj, writer, Context.needsDelimiters);

let b = writer.finish();

console.log(b);

console.log(row.decode(new Reader(b), Context.needsDelimiters));
