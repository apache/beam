import { Reader, Writer } from "protobufjs";
import { AtomicType, Schema } from "../proto/schema";
import { Context } from "./coders";
import { RowCoder } from "./row_coder";

/*
schema = schema_pb2.Schema(
        id="person",
        fields=[
            schema_pb2.Field(
                name="name",
                type=schema_pb2.FieldType(atomic_type=schema_pb2.STRING)),
            schema_pb2.Field(
                name="age",
                type=schema_pb2.FieldType(atomic_type=schema_pb2.INT32)),
            schema_pb2.Field(
                name="address",
                type=schema_pb2.FieldType(
                    atomic_type=schema_pb2.STRING, nullable=True)),
            schema_pb2.Field(
                name="aliases",
                type=schema_pb2.FieldType(
                    array_type=schema_pb2.ArrayType(
                        element_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING)))),
            schema_pb2.Field(
                name="knows_javascript",
                type=schema_pb2.FieldType(atomic_type=schema_pb2.BOOLEAN)),
            schema_pb2.Field(
                name="payload",
                type=schema_pb2.FieldType(
                    atomic_type=schema_pb2.BYTES, nullable=True)),
            schema_pb2.Field(
                name="custom_metadata",
                type=schema_pb2.FieldType(
                    map_type=schema_pb2.MapType(
                        key_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.STRING),
                        value_type=schema_pb2.FieldType(
                            atomic_type=schema_pb2.int32),
                    ))),
            schema_pb2.Field(
                name="favorite_time",
                type=schema_pb2.FieldType(
                    logical_type=schema_pb2.LogicalType(
                        urn="beam:logical_type:micros_instant:v1",
                        representation=schema_pb2.FieldType(
                            row_type=schema_pb2.RowType(
                                schema=schema_pb2.Schema(
                                    id="micros_instant",
                                    fields=[
                                        schema_pb2.Field(
                                            name="seconds",
                                            type=schema_pb2.FieldType(
                                                atomic_type=schema_pb2.int32)),
                                        schema_pb2.Field(
                                            name="micros",
                                            type=schema_pb2.FieldType(
                                                atomic_type=schema_pb2.int32)),
                                    ])))))),
        ])
*/

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
                    atomicType: AtomicType.STRING
                }
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
                                          atomicType: AtomicType.STRING
                                      }
                                    },
                                    id: 0,
                                    encodingPosition: 0,
                                    options: [],
                                }
                            ],
                            options: [],
                            encodingPositionsSet: false
                        }
                    }
                }
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
                                        atomicType: AtomicType.STRING
                                    }
                                  },
                                  id: 0,
                                  encodingPosition: 0,
                                  options: [],
                              }
                          ],
                          options: [],
                          encodingPositionsSet: false
                      }
                  }
              }
          },
          id: 0,
          encodingPosition: 0,
          options: [],
      }
    ],
    options: [],
    encodingPositionsSet: false
  };
  
// let obj = {
//     x: "first",
//     y: {
//         a: [1,2,3,4,5],
//         b: "third"
//     }
// };

let obj = {
    constructor: 'apache_beam.transforms.fully_qualified_named_transform_test._TestTransform',
    args: { arg0: 'x' },
    kwargs: { suffix: 'y' },
}

let writer = new Writer(),
    row = RowCoder.OfSchema(schema);


row.encode(obj, writer, Context.needsDelimiters);

let b = writer.finish();

console.log(b);

console.log(row.decode(new Reader(b), Context.needsDelimiters));