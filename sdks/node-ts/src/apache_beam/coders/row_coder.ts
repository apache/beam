// import * as runnerApi from '../proto/beam_runner_api';
import * as translations from '../internal/translations'

import { Writer, Reader } from 'protobufjs';
import { Coder, Context, CODER_REGISTRY } from "./coders";


// import {  } from './standard_coders';

import { Schema, Field } from '../proto/schema';
import { BytesCoder, VarIntCoder } from './standard_coders';
import { HighlightSpanKind, NumericLiteral } from 'typescript';



// class Row {


//     constructor(row: any) {
//         Object.entries(row).forEach(([key, value]) => {
//             let kind  = this.getTypeOf(value);

//             switch (kind){
//                 case "object": 
//                     if(Array.isArray(value)){
//                         kind = "array";
//                     }
//                     break;
//                 case "string":
//                     break;
//                 case "number":
//                     if(Number.isInteger(value)){

//                     } else {

//                     }
//                     break;
//                 case "boolean":
//             }
// 'string': new StrUtf8Coder(),
// 'number': new DoubleCoder(),  // TODO(pabloem): What about integers? Do we represent always as doubles?
// 'object': new BsonObjectCoder(),
// 'boolean': new BoolCoder()

//             console.log(`${key} ${kind}`);
//         });    
//     }

//     getTypeOf(value: any): string {
//         return typeof value
//     }
// }


// Row({x: 10, y:10});

export class RowCoder implements Coder<any> {
    public static URN: string = "beam:coder:row:v1";

    private schema: Schema;
    private nFields: number;
    private fieldNames: string[];
    private fieldNullable: (boolean | undefined)[];
    private encodingPositionsAreTrivial: boolean;
    private encodingPositions: number[];
    private encodingPositionsSorted: number[];

    private hasNullableFields: boolean;
    private components: Coder<any>[];

    getNonNullCoderFromType(f: Field): any { }

    static OfSchema(schema: Schema): RowCoder {
        return new RowCoder(schema);
    }

    private constructor(schema: Schema) {
        this.schema = schema;
        this.nFields = this.schema.fields.length;
        this.fieldNames = this.schema.fields.map((f: Field) => f.name);
        this.fieldNullable = this.schema.fields.map((f: Field) => f.type?.nullable);
        // self.constructor = named_tuple_fromschema(schema)
        this.encodingPositions = this.schema.fields.map((_, i) => i);

        if (this.schema.encodingPositionsSet) {
            // Should never be duplicate encoding positions.
            let encPosx = schema.fields.map((f: Field) => f.encodingPosition);
            if (encPosx.length != this.encodingPositions.length) {
                throw new Error(`Schema with id ${this.schema.id} has encoding_positions_set=True, but not all fields have encoding_position set`);
            }
            // Checking if positions are in {0, ..., length-1}
            this.encodingPositionsAreTrivial = encPosx == this.encodingPositions;
            this.encodingPositions = encPosx;
            this.encodingPositionsSorted = encPosx.sort(); // Acsending order
        }

        this.hasNullableFields = this.schema.fields.some((f: Field) => f.type?.nullable);
        this.components = this.encodingPositions
            .map(i => this.schema.fields[i])
            .map((f: Field) => this.getNonNullCoderFromType(f));
    }

    encode(element: any, writer: Writer, context: Context) {
        // let bytesIntCoder = new BytesCoder();

        // The number of attributes in the schema, encoded with
        // beam:coder:varint:v1. This makes it possible to detect certain
        // allowed schema changes (appending or removing columns) in
        // long-running streaming pipelines.
        writer.int64(this.nFields);


        // A byte array representing a packed bitset indicating null fields (a
        //     1 indicating a null) encoded with beam:coder:bytes:v1. The unused
        //     bits in the last byte must be set to 0. If there are no nulls an
        //     empty byte array is encoded.
        //     The two-byte bitset (not including the lenghth-prefix) for the row
        //     [NULL, 0, 0, 0, NULL, 0, 0, NULL, 0, NULL] would be
        //     [0b10010001, 0b00000010]
        let attrs = this.fieldNames.map(name => element[name]);
        if (this.hasNullableFields) {
            if (attrs.some(attr => attr == undefined)) {
                writer.int64(Math.floor((this.nFields + 7) / 8));
                // Pack the bits, little - endian, in consecutive bytes.
                let running = 0;
                attrs.forEach(
                    (attr, i) => {
                        if (i && i % 8 == 0) {
                            writer.bytes(running.toString());
                            running = 0;
                            running |= (attr == undefined ? 1 : 0) << (i % 8);
                        }
                    })
                writer.bytes(running.toString());
            } else {
                // bytesIntCoder.encode(new Uint8Array(), writer, context)
                writer.bytes("0");
            }
        } else {
            // bytesIntCoder.encode(new Uint8Array(), writer, context);
            writer.bytes("0");
        }

        // An encoding for each non-null field, concatenated together.
        let positions = this.encodingPositionsAreTrivial? this.encodingPositions : this.encodingPositionsSorted;
        positions.forEach(
            i => {
            let attr = attrs[i];
            if (attr == undefined) {
                if(!this.fieldNullable[i]) {
                    throw new Error(`Attempted to encode null for non-nullable field \"${this.schema.fields[i].name}\".`);
                }
                continue;
            }
            this.components[i].encode(attr, writer, Context.needsDelimiters);
        })
    }

    decode(reader: Reader, context: Context): any {

    }

    toProto(pipelineComponents: runnerApi.Components): runnerApi.Coder {

    }
}





function





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
                            atomic_type=schema_pb2.INT64),
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
                                                atomic_type=schema_pb2.INT64)),
                                        schema_pb2.Field(
                                            name="micros",
                                            type=schema_pb2.FieldType(
                                                atomic_type=schema_pb2.INT64)),
                                    ])))))),
        ])
*/