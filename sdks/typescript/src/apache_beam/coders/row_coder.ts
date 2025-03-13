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

import * as uuid from "uuid";

import * as runnerApi from "../proto/beam_runner_api";

import { Writer, Reader } from "protobufjs";
import { Coder, Context, ProtoContext, globalRegistry } from "./coders";

import {
  Schema,
  Field,
  FieldType,
  AtomicType,
  LogicalType,
} from "../proto/schema";
import {
  BoolCoder,
  BytesCoder,
  IterableCoder,
  NullableCoder,
  StrUtf8Coder,
  VarIntCoder,
} from "./standard_coders";

interface LogicalTypeInfo<T, R> {
  urn: string;
  reprType: FieldType;
  toRepr(obj: T): R;
  fromRepr(repr: R): T;
}

const logicalTypes: Map<string, LogicalTypeInfo<unknown, unknown>> = new Map();

export function registerLogicalType(logicalType: LogicalTypeInfo<any, any>) {
  logicalTypes.set(logicalType.urn, logicalType);
}

function getLogicalFieldType(urn: string, nullable: boolean = true): FieldType {
  const info = logicalTypes.get(urn)!;
  return {
    nullable: nullable,
    typeInfo: {
      oneofKind: "logicalType",
      logicalType: LogicalType.create({
        urn,
        representation: info.reprType,
      }),
    },
  };
}

class TypePlaceholder {
  constructor(public fieldType: FieldType) {}
}

class LogicalTypePlaceholder extends TypePlaceholder {
  constructor(public urn: string) {
    super(getLogicalFieldType(urn));
  }
}

const argsort = (x) =>
  x
    .map((v, i) => [v, i])
    .sort()
    .map((y) => y[1]);

/**
 * A coder for encoding Objects as row objects with a given schema.
 *
 * This is particularly useful for cross-language interoperability,
 * and is more efficient than the general object encoding scheme as the
 * fields (and their types) are fixed and do not have to be encoded along
 * with each individual element.
 *
 * While RowCoders can be instantiated directly from a schema object,
 * there is also the convenience method `RowCoder.fromJSON()` method that
 * can infer a RowCoder from a prototypical example, e.g.
 *
 *```js
 * const my_row_coder = RowCoder.fromJSON({int_field: 0, str_field: ""});
 *```
 */
export class RowCoder implements Coder<any> {
  public static URN: string = "beam:coder:row:v1";

  private schema: Schema;
  private nFields: number;
  private fieldNames: string[];
  private fieldNullable: (boolean | undefined)[];
  private encodingPositionsAreTrivial: boolean = true;
  private encodingPositions: number[];
  private encodingPositionsArgsSorted: number[];

  private hasNullableFields: boolean;
  private components: Coder<any>[];

  addFieldOfType(obj: any, f: Field, value: any): any {
    if (f.type !== undefined) {
      let typeInfo = f.type?.typeInfo;
      switch (typeInfo.oneofKind) {
        case "atomicType":
        case "rowType":
        case "logicalType":
          obj[f.name] = value;
          break;
        case "arrayType":
          obj[f.name] = value === undefined ? undefined : Array.from(value);
          break;
        // case "iterableType":
        // case "mapType":
        default:
          throw new Error(
            `Encountered a type that is not currently supported by RowCoder: ${JSON.stringify(
              f.type,
            )}`,
          );
      }
      return obj;
    }
  }

  static inferTypeFromJSON(obj: any, nullable: boolean = true): FieldType {
    if (obj instanceof TypePlaceholder) {
      return obj.fieldType;
    }

    let fieldType: FieldType = {
      nullable: nullable,
      typeInfo: {
        oneofKind: undefined,
      },
    };

    switch (typeof obj) {
      case "string":
        fieldType.typeInfo = {
          oneofKind: "atomicType",
          atomicType: AtomicType.STRING,
        };
        break;
      case "boolean":
        fieldType.typeInfo = {
          oneofKind: "atomicType",
          atomicType: AtomicType.BOOLEAN,
        };
        break;
      case "number":
        if (Number.isInteger(obj)) {
          fieldType.typeInfo = {
            oneofKind: "atomicType",
            atomicType: AtomicType.INT64,
          };
        } else {
          // TODO: Support float type.
          // field.type!.typeInfo = {
          //     oneofKind: "atomicType",
          //     atomicType: AtomicType.FLOAT
          // }
        }
        break;
      case "object":
        if (Array.isArray(obj)) {
          fieldType.typeInfo = {
            oneofKind: "arrayType",
            arrayType: {
              // TODO: Infer element type in a better way
              elementType: RowCoder.inferTypeFromJSON(obj[0]),
            },
          };
        } else if (obj instanceof Uint8Array) {
          fieldType.typeInfo = {
            oneofKind: "atomicType",
            atomicType: AtomicType.BYTES,
          };
        } else if (obj.beamLogicalType) {
          const logicalTypeInfo = logicalTypes.get(obj.beamLogicalType);
          fieldType.typeInfo = {
            oneofKind: "logicalType",
            logicalType: {
              urn: obj.beamLogicalType,
              payload: new Uint8Array(),
              representation: logicalTypeInfo?.reprType,
            },
          };
        } else {
          fieldType.typeInfo = {
            oneofKind: "rowType",
            rowType: { schema: RowCoder.inferSchemaOfJSON(obj) },
          };
        }
        break;
      default:
        fieldType.typeInfo = {
          oneofKind: undefined,
        };
    }
    return fieldType;
  }

  static inferSchemaOfJSON(obj: any): Schema {
    let fields: Field[] = Object.entries(obj).map((entry) => {
      return {
        name: entry[0],
        description: "",
        type: RowCoder.inferTypeFromJSON(entry[1]),
        id: 0,
        encodingPosition: 0,
        options: [],
      };
    });
    return {
      id: uuid.v4(),
      fields: fields,
      options: [],
      encodingPositionsSet: false,
    };
  }

  getCoderFromType(t: FieldType): any {
    const nonNullCoder = this.getNonNullCoderFromType(t);
    if (t.nullable) {
      return new NullableCoder(nonNullCoder);
    } else {
      return nonNullCoder;
    }
  }

  getNonNullCoderFromType(t: FieldType): any {
    let typeInfo = t.typeInfo;

    switch (typeInfo.oneofKind) {
      case "atomicType":
        let atomicType: AtomicType = typeInfo.atomicType;
        switch (atomicType) {
          case AtomicType.INT16:
          case AtomicType.INT32:
          case AtomicType.INT64:
            return new VarIntCoder();
          // case AtomicType.BYTE:
          case AtomicType.BYTES:
            return new BytesCoder();
          // case AtomicType.FLOAT:
          // case AtomicType.DOUBLE:
          case AtomicType.STRING:
            return new StrUtf8Coder();
          case AtomicType.BOOLEAN:
            return new BoolCoder();
          default:
            throw new Error(
              `Encountered an Atomic type that is not currently supported by RowCoder: ${atomicType}`,
            );
        }
        break;
      case "arrayType":
        if (typeInfo.arrayType.elementType !== undefined) {
          return new IterableCoder(
            this.getCoderFromType(typeInfo.arrayType.elementType),
          );
        } else {
          throw new Error("ElementType missing on ArrayType");
        }
      // case "iterableType":
      // case "mapType":
      case "rowType":
        if (typeInfo.rowType.schema !== undefined) {
          return RowCoder.fromSchema(typeInfo.rowType.schema);
        } else {
          throw new Error("Schema missing on RowType");
        }
        break;
      case "logicalType":
        const logicalTypeInfo = logicalTypes.get(typeInfo.logicalType.urn);
        if (logicalTypeInfo !== undefined) {
          const reprCoder = this.getCoderFromType(
            typeInfo.logicalType.representation!,
          );
          return {
            encode: (element: any, writer: Writer, context: Context) =>
              reprCoder.encode(
                logicalTypeInfo.toRepr(element),
                writer,
                context,
              ),
            decode: (reader: Reader, context: Context) =>
              logicalTypeInfo.fromRepr(reprCoder.decode(reader, context)),
            toProto: (pipelineContext: ProtoContext) => {
              throw new Error("Ephemeral coder.");
            },
          };
        } else {
          throw new Error(`Unknown logical type: ${typeInfo.logicalType.urn}`);
        }
        break;
      default:
        throw new Error(
          `Encountered a type that is not currently supported by RowCoder: ${JSON.stringify(
            t,
          )}`,
        );
    }
  }

  static fromSchema(schema: Schema): RowCoder {
    return new RowCoder(schema);
  }

  static fromJSON(obj: any): RowCoder {
    return new RowCoder(RowCoder.inferSchemaOfJSON(obj));
  }

  constructor(rawSchema: Schema | Uint8Array) {
    const schema: Schema =
      rawSchema instanceof Uint8Array
        ? Schema.fromBinary(rawSchema as Uint8Array)
        : (rawSchema as Schema);
    this.schema = schema as Schema;
    this.nFields = this.schema.fields.length;
    this.fieldNames = this.schema.fields.map((f: Field) => f.name);
    this.fieldNullable = this.schema.fields.map((f: Field) => f.type?.nullable);
    this.encodingPositions = this.schema.fields.map((_, i) => i);

    if (this.schema.encodingPositionsSet) {
      // Should never be duplicate encoding positions.
      let encPosx = schema.fields.map((f: Field) => f.encodingPosition);
      if (encPosx.length !== this.encodingPositions.length) {
        throw new Error(
          `Schema with id ${this.schema.id} has encoding_positions_set=True, but not all fields have encoding_position set`,
        );
      }
      // Checking if positions are in {0, ..., length-1}
      this.encodingPositionsAreTrivial = encPosx === this.encodingPositions;
      this.encodingPositions = encPosx;
      this.encodingPositionsArgsSorted = argsort(encPosx);
    }

    this.hasNullableFields = this.schema.fields.some(
      (f: Field) => f.type?.nullable,
    );
    this.components = this.encodingPositions
      .map((i) => this.schema.fields[i])
      .map((f: Field) => {
        if (f.type !== undefined) {
          return this.getNonNullCoderFromType(f.type);
        }
      });
  }

  encode(element: any, writer: Writer, context: Context) {
    // The number of attributes in the schema, encoded with
    // beam:coder:varint:v1. This makes it possible to detect certain
    // allowed schema changes (appending or removing columns) in
    // long-running streaming pipelines.
    writer.int32(this.nFields);

    // A byte array representing a packed bitset indicating null fields (a
    //     1 indicating a null) encoded with beam:coder:bytes:v1. The unused
    //     bits in the last byte must be set to 0. If there are no nulls an
    //     empty byte array is encoded.
    //     The two-byte bitset (not including the lenghth-prefix) for the row
    //     [NULL, 0, 0, 0, NULL, 0, 0, NULL, 0, NULL] would be
    //     [0b10010001, 0b00000010]
    let attrs = this.fieldNames.map((name) => element[name]);

    let bytesCoder = new BytesCoder();

    let nullFields: number[] = [];

    if (this.hasNullableFields) {
      if (attrs.some((attr) => attr === null || attr === undefined)) {
        let running = 0;
        attrs.forEach((attr, i) => {
          if (i && i % 8 === 0) {
            nullFields.push(running);
            running = 0;
          }
          running |= (attr === null || attr === undefined ? 1 : 0) << i % 8;
        });
        nullFields.push(running);
      }
    }

    writer.bytes(new Uint8Array(nullFields));

    // An encoding for each non-null field, concatenated together.
    let positions = this.encodingPositionsAreTrivial
      ? this.encodingPositions
      : this.encodingPositionsArgsSorted;

    positions.forEach((i) => {
      let attr = attrs[i];
      if (attr === null || attr === undefined) {
        if (!this.fieldNullable[i]) {
          throw new Error(
            `Attempted to encode null for non-nullable field \"${this.schema.fields[i].name}\".`,
          );
        }
      } else {
        this.components[i].encode(attr, writer, Context.needsDelimiters);
      }
    });
  }

  decode(reader: Reader, context: Context): any {
    let nFields = reader.int32();

    // Addressing Null values
    let nulls: any[],
      hasNulls = false,
      nullMaskBytes = reader.int32(),
      nullMask = reader.buf.slice(reader.pos, reader.pos + nullMaskBytes);

    reader.pos += nullMaskBytes;

    if (nullMask.length > 0) {
      hasNulls = true;

      let running = 0;
      nulls = Array(nFields)
        .fill(0)
        .map((_, i) => {
          if (i % 8 === 0) {
            let chunk = Math.floor(i / 8);
            running = chunk >= nullMask.length ? 0 : nullMask[chunk];
          }
          return (running >> i % 8) & 0x01;
        });
    }

    // Note that if this coder's schema has *fewer* attributes than the encoded
    // value, we just need to ignore the additional values, which will occur
    // here because we only decode as many values as we have coders for.
    let positions = this.encodingPositionsAreTrivial
        ? this.encodingPositions
        : this.encodingPositionsArgsSorted,
      sortedComponents = positions
        .slice(0, Math.min(this.nFields, nFields))
        .map((i) => {
          if (hasNulls && nulls[i]) {
            return undefined;
          } else {
            return this.components[i].decode(reader, Context.needsDelimiters);
          }
        });

    // If this coder's schema has more attributes than the encoded value, then
    // the schema must have changed. Populate the unencoded fields with
    // undefined.
    while (sortedComponents.length < this.nFields) {
      sortedComponents.push(undefined);
    }

    let obj: any = {};
    positions.forEach((i) => {
      obj = this.addFieldOfType(
        obj,
        this.schema.fields[i],
        sortedComponents[i],
      );
    });

    return obj;
  }

  toProto(pipelineContext: ProtoContext): runnerApi.Coder {
    return {
      spec: {
        urn: RowCoder.URN,
        payload: Schema.toBinary(this.schema),
      },
      componentCoderIds: [],
    };
  }
}

globalRegistry().register(RowCoder.URN, RowCoder);

registerLogicalType({
  urn: "beam:logical_type:schema:v1",
  reprType: RowCoder.inferTypeFromJSON(new Uint8Array(), false),
  toRepr: (schema) => Schema.toBinary(schema),
  fromRepr: (serialized) => Schema.fromBinary(serialized),
});
