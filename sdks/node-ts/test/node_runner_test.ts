import fs from 'fs';
import assert from 'assert';

import { RemoteJobServiceClient } from "../src/apache_beam/runners/node_runner/client";
import { NodeRunner } from "../src/apache_beam/runners/node_runner/runner";
import { JobState_Enum } from "../src/apache_beam/proto/beam_job_api";

const JOB_SERVICE_HOST = process.env['JOB_SERVICE_HOST'];
const JSON_PROTO_PATH = __dirname + '/../../test/testdata/pipeline.json';

describe('node runner', () => {
    it('runs', async function() {
        if (!JOB_SERVICE_HOST) {
            this.skip();
        }

        const pipelineJson = fs.readFileSync(JSON_PROTO_PATH, 'utf-8');

        const runner = new NodeRunner(new RemoteJobServiceClient(JOB_SERVICE_HOST));
        const pipelineResult = await runner.runPipelineWithJsonStringProto(pipelineJson, 'pipeline');

        const event = await pipelineResult.waitUntilFinish(60000);
        assert(event == JobState_Enum.DONE);
    });
})
