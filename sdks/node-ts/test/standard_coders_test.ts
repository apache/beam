import {Coder, CODER_REGISTRY, Context} from "../src/apache_beam/coders/coders";
import { Writer, Reader } from 'protobufjs';

import assertions = require('assert');
import yaml = require('js-yaml');
import fs = require('fs');
import util = require('util');

const STANDARD_CODERS_FILE = '../../model/fn-execution/src/main/resources/org/apache/beam/model/fnexecution/v1/standard_coders.yaml';

// TODO(pabloem): Empty this list.
const UNSUPPORTED_CODERS = [
    "beam:coder:interval_window:v1",
    "beam:coder:iterable:v1",
    "beam:coder:timer:v1",
    "beam:coder:global_window:v1",
    "beam:coder:windowed_value:v1",
    "beam:coder:param_windowed_value:v1",
    "beam:coder:row:v1",
    "beam:coder:sharded_key:v1",
    "beam:coder:custom_window:v1",
    "beam:coder:kv:v1"
];

const _urn_to_json_value_parser = {
    'beam:coder:bytes:v1': x => new TextEncoder().encode(x),
    'beam:coder:bool:v1': x => x,
    'beam:coder:string_utf8:v1': x => x,
    'beam:coder:varint:v1': x => x,
    'beam:coder:double:v1': x => Number(x),
    'beam:coder:kv:v1': (x, components) => (components[0](x['key']), components[1](x['value'])),
    // 'beam:coder:interval_window:v1': lambda x: IntervalWindow(
    //     start=Timestamp(micros=(x['end'] - x['span']) * 1000),
    //     end=Timestamp(micros=x['end'] * 1000)),
    // 'beam:coder:iterable:v1': lambda x,
    // parser: list(map(parser, x)),
    // 'beam:coder:global_window:v1': lambda x: window.GlobalWindow(),
    // 'beam:coder:windowed_value:v1': lambda x,
    // value_parser,
    // window_parser: windowed_value.create(
    //     value_parser(x['value']),
    //     x['timestamp'] * 1000,
    //     tuple(window_parser(w) for w in x['windows'])),
    // 'beam:coder:param_windowed_value:v1': lambda x,
    // value_parser,
    // window_parser: windowed_value.create(
    //     value_parser(x['value']),
    //     x['timestamp'] * 1000,
    //     tuple(window_parser(w) for w in x['windows']),
    //     PaneInfo(
    //         x['pane']['is_first'],
    //         x['pane']['is_last'],
    //         PaneInfoTiming.from_string(x['pane']['timing']),
    //         x['pane']['index'],
    //         x['pane']['on_time_index'])),
    // 'beam:coder:timer:v1': lambda x,
    // value_parser,
    // window_parser: userstate.Timer(
    //     user_key=value_parser(x['userKey']),
    //     dynamic_timer_tag=x['dynamicTimerTag'],
    //     clear_bit=x['clearBit'],
    //     windows=tuple(window_parser(w) for w in x['windows']),
    //     fire_timestamp=None,
    //     hold_timestamp=None,
    //     paneinfo=None) if x['clearBit'] else userstate.Timer(
    //         user_key=value_parser(x['userKey']),
    //         dynamic_timer_tag=x['dynamicTimerTag'],
    //         clear_bit=x['clearBit'],
    //         fire_timestamp=Timestamp(micros=x['fireTimestamp'] * 1000),
    //         hold_timestamp=Timestamp(micros=x['holdTimestamp'] * 1000),
    //         windows=tuple(window_parser(w) for w in x['windows']),
    //         paneinfo=PaneInfo(
    //             x['pane']['is_first'],
    //             x['pane']['is_last'],
    //             PaneInfoTiming.from_string(x['pane']['timing']),
    //             x['pane']['index'],
    //             x['pane']['on_time_index'])),
    // 'beam:coder:double:v1': parse_float,
    // 'beam:coder:sharded_key:v1': lambda x,
    // value_parser: ShardedKey(
    //     key=value_parser(x['key']), shard_id=x['shardId'].encode('utf-8')),
    // 'beam:coder:custom_window:v1': lambda x,
    // window_parser: window_parser(x['window'])
}

interface CoderRepr {
    urn: string,
    payload?: Uint8Array,
    components?: Array<CoderRepr>
}

type CoderSpec = {
    coder: CoderRepr,
    nested?: boolean,
    examples: object
}

function get_json_value_parser(coderRepr: CoderRepr) {
    // TODO(pabloem): support 'beam:coder:row:v1' coder.
    console.log(util.inspect(coderRepr, {colors: true}));

    var value_parser_factory = _urn_to_json_value_parser[coderRepr.urn]

    if (value_parser_factory === undefined) {
        throw new Error(util.format("Do not know how to parse example values for %s", coderRepr))
    }

    if (coderRepr.components !== undefined) {
        const componentParsers = coderRepr.components.map(c => get_json_value_parser(c));
        return x => value_parser_factory(x, componentParsers)
    } else {
        return x => value_parser_factory(x)
    }
}

describe("standard Beam coders on Javascript", function() {
    const docs : Array<CoderSpec> = yaml.loadAll(fs.readFileSync(STANDARD_CODERS_FILE, 'utf8'));
    docs.forEach(doc => {
        const urn = doc.coder.urn;
        if (UNSUPPORTED_CODERS.includes(urn)) {
            return;
        }
        var context = (doc.nested === true) ? Context.needsDelimiters : Context.wholeStream;
        const spec = doc;

        const coder = CODER_REGISTRY.get(urn);
        describeCoder(coder, urn, context, spec);
    });
});

function describeCoder<T>(coder: Coder<T>, urn, context, spec: CoderSpec) {
    describe(util.format("coder %s (%s) in context %s", coder, urn, context), function() {
        let examples = 0;
        const parser = get_json_value_parser(spec.coder);
        for (let expected in spec.examples) {
            var value = parser(spec.examples[expected]);
            examples += 1;
            const expectedEncoded = Buffer.from(expected, 'binary')
            coderCase(coder, value, expectedEncoded, context, examples);
        }
    });
}

function coderCase<T>(coder: Coder<T>, obj, expectedEncoded:Uint8Array, context, exampleCount) {
    it(util.format("encodes example %d correctly", exampleCount), function() {
        var writer = new Writer();
        coder.encode(obj, writer, context);
        assertions.deepEqual(writer.finish(), expectedEncoded);
    });

    it(util.format("decodes example %d correctly", exampleCount), function() {
        const decoded = coder.decode(new Reader(expectedEncoded), context);
        assertions.deepEqual(decoded, obj);
    });
}