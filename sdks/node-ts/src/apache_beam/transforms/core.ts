import { PTransform, PCollection } from "../base";
import * as translations from '../internal/translations';
import * as runnerApi from '../proto/beam_runner_api';
import { BytesCoder, KVCoder } from "../coders/standard_coders";

import {GroupByKey} from '../base'

export class GroupBy extends PTransform {
    keyFn: (element: any) => any;
    constructor(key: string | ((element: any) => any)) {
        super();
        if ((key as (element: any) => any).call !== undefined) {
            this.keyFn = key as (element: any) => any;
        } else {
            this.keyFn = function(x) { return x[key as string]; };
        }
    }

    expand(input: PCollection): PCollection {
        let kvPcoll = input.map(function (x) { return {'key': this.keyFn(x), 'value': x};});
        
        const inputCoderId = (input as PCollection).proto.coderId;

        // TODO(pabloem): Find the appropriate coder for the key
        const keyCoderId = translations.registerPipelineCoder(
            runnerApi.Coder.create({'spec': runnerApi.FunctionSpec.create({'urn': BytesCoder.URN}),}),
            input.pipeline.proto.components!);
        input.pipeline.coders[keyCoderId] = new BytesCoder();

        const kvCoderProto = runnerApi.Coder.create({
            'spec': runnerApi.FunctionSpec.create({'urn': KVCoder.URN}),
            'componentCoderIds': [keyCoderId, inputCoderId]
        })
        const kvCoderId = translations.registerPipelineCoder(kvCoderProto, input.pipeline.proto.components!);

        kvPcoll.proto.coderId = kvCoderId;
        const kvCoder = new KVCoder(input.pipeline.coders[keyCoderId], input.pipeline.coders[input.proto.coderId]);
        input.pipeline.coders[kvCoderId] = kvCoder;
        
        return kvPcoll.apply(new GroupByKey());
    }
}