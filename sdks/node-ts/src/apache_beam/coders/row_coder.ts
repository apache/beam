import * as runnerApi from "../proto/beam_runner_api";
import * as translations from "../internal/translations";

import { Writer, Reader } from "protobufjs";
import { Coder, Context, CODER_REGISTRY } from "./coders";

import { Schema, Field, FieldType, AtomicType } from "../proto/schema";
import { PipelineContext } from "..";
import {
  BoolCoder,
  BytesCoder,
  IterableCoder,
  StrUtf8Coder,
  VarIntCoder,
} from "./standard_coders";
import { Value } from "../proto/google/protobuf/struct";

const argsort = (x) =>
  x
    .map((v, i) => [v, i])
    .sort()
    .map((y) => y[1]);

// const util = require('util');

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
          obj[f.name] = value;
          break;
        case "arrayType":
          obj[f.name] = Array.from(value);
          break;
        // case "iterableType":
        // case "mapType":
        case "rowType":
          if (typeInfo.rowType.schema !== undefined) {
            obj[f.name] = value;
          } else {
            throw new Error("Schema missing on RowType");
          }
          break;
        // case "logicalType":
        default:
          throw new Error(
            `Encountered a type that is not currently supported by RowCoder: ${f.type}`
          );
      }
      return obj;
    }
  }

  private static InferTypeFromJSON(obj: any): FieldType {
    let fieldType: FieldType = {
      nullable: true,
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
              elementType: RowCoder.InferTypeFromJSON(obj[0]),
            },
          };
        } else if (obj instanceof Uint8Array) {
          fieldType.typeInfo = {
            oneofKind: "atomicType",
            atomicType: AtomicType.BYTES,
          };
        } else {
          fieldType.typeInfo = {
            oneofKind: "rowType",
            rowType: { schema: RowCoder.InferSchemaOfJSON(obj) },
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

  static InferSchemaOfJSON(obj: any): Schema {
    let fields: Field[] = Object.entries(obj).map((entry) => {
      return {
        name: entry[0],
        description: "",
        type: RowCoder.InferTypeFromJSON(entry[1]),
        id: 0,
        encodingPosition: 0,
        options: [],
      };
    });
    console.log(
      JSON.stringify(
        {
          id: (Math.random() + 1).toString(36).substring(7),
          fields: fields,
          options: [],
          encodingPositionsSet: false,
        },
        null,
        4
      )
    );
    return {
      id: (Math.random() + 1).toString(36).substring(7),
      fields: fields,
      options: [],
      encodingPositionsSet: false,
    };
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
              `Encountered an Atomic type that is not currently supported by RowCoder: ${atomicType}`
            );
        }
        break;
      case "arrayType":
        if (typeInfo.arrayType.elementType !== undefined) {
          return new IterableCoder(
            this.getNonNullCoderFromType(typeInfo.arrayType.elementType)
          );
        } else {
          throw new Error("ElementType missing on ArrayType");
        }
      // case "iterableType":
      // case "mapType":
      case "rowType":
        if (typeInfo.rowType.schema !== undefined) {
          return RowCoder.OfSchema(typeInfo.rowType.schema);
        } else {
          throw new Error("Schema missing on RowType");
        }
        break;
      // case "logicalType":
      default:
        throw new Error(
          `Encountered a type that is not currently supported by RowCoder: ${t}`
        );
    }
  }

  static OfSchema(schema: Schema): RowCoder {
    return new RowCoder(schema);
  }

  static OfJSON(obj: any): RowCoder {
    return new RowCoder(RowCoder.InferSchemaOfJSON(obj));
  }

  constructor(schema: Schema) {
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
        throw new Error(
          `Schema with id ${this.schema.id} has encoding_positions_set=True, but not all fields have encoding_position set`
        );
      }
      // Checking if positions are in {0, ..., length-1}
      this.encodingPositionsAreTrivial = encPosx === this.encodingPositions;
      this.encodingPositions = encPosx;
      this.encodingPositionsArgsSorted = argsort(encPosx);
    }

    this.hasNullableFields = this.schema.fields.some(
      (f: Field) => f.type?.nullable
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
    // let bytesIntCoder = new BytesCoder();

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
      if (attrs.some((attr) => attr == undefined)) {
        let running = 0;
        attrs.forEach((attr, i) => {
          if (i && i % 8 == 0) {
            nullFields.push(running);
            running = 0;
          }
          running |= (attr == undefined ? 1 : 0) << i % 8;
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
      if (attr == undefined) {
        if (!this.fieldNullable[i]) {
          throw new Error(
            `Attempted to encode null for non-nullable field \"${this.schema.fields[i].name}\".`
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
          if (i % 8 == 0) {
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
    // the schema must have changed. Populate the unencoded fields with nulls.
    while (sortedComponents.length < this.nFields) {
      sortedComponents.push(undefined);
    }

    let obj: any = {};
    positions.forEach((i) => {
      obj = this.addFieldOfType(
        obj,
        this.schema.fields[i],
        sortedComponents[i]
      );
    });

    return obj;
  }

  toProto(pipelineContext: PipelineContext): runnerApi.Coder {
    return {
      spec: {
        urn: RowCoder.URN,
        payload: new Uint8Array(),
      },
      componentCoderIds: [],
    };
  }
}
CODER_REGISTRY.register(RowCoder.URN, RowCoder);
