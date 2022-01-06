import * as BSON from 'bson';
import { Writer, Reader } from 'protobufjs';
import {Coder, CODER_REGISTRY, Context} from './coders'
import {BoolCoder, DoubleCoder, StrUtf8Coder, VarIntCoder} from './standard_coders'
import * as runnerApi from '../proto/beam_runner_api';


export class BsonObjectCoder<T> implements Coder<T> {
    static URN = "beam:coder:bsonjs:v1";
    encode(element: T, writer: Writer, context: Context) {
        const buff = BSON.serialize(element);
        writer.bytes(buff);
    }
    decode(reader: Reader, context: Context): T {
        const encoded = reader.bytes();
        return BSON.deserialize(encoded) as T;
    }
    toProto(pipelineComponents: runnerApi.Components): runnerApi.Coder {
        return {
            spec: {
                urn: BsonObjectCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(BsonObjectCoder.URN, BsonObjectCoder)

export class GeneralObjectCoder<T> implements Coder<T> {
    static URN = "beam:coder:genericobjectjs:v1"
    componentCoders = {
        'string': new StrUtf8Coder(),
        'number': new DoubleCoder(),  // TODO(pabloem): What about integers? Do we represent always as doubles?
        'object': new BsonObjectCoder(),
        'boolean': new BoolCoder()
    };

    // This is a map of type names to type markers. It maps a type name to its
    // marker within a stream.
    typeMarkers = {
        'string': 'S',
        'number': 'N',
        'object': 'O',
        'boolean': 'B'
    }
    // This is a map of type markers to type names. It maps a type marker to its
    // type name.
    markerToTypes = {
        'S': 'string',
        'N': 'number',
        'O': 'object',
        'B': 'boolean'
    };
    encode(element: T, writer: Writer, context: Context) {
        const type = typeof element;
        writer.string(this.typeMarkers[type])
        this.componentCoders[type].encode(element, writer, context);
    }
    decode(reader: Reader, context: Context): T {
        const typeMarker = reader.string()
        const type = this.markerToTypes[typeMarker];
        return this.componentCoders[type].decode(reader, context);
    }
    toProto(pipelineComponents: runnerApi.Components): runnerApi.Coder {
        return {
            spec: {
                urn: GeneralObjectCoder.URN,
                payload: new Uint8Array(),
            },
            componentCoderIds: [],
        }
    }
}
CODER_REGISTRY.register(GeneralObjectCoder.URN, GeneralObjectCoder)