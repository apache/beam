import { ChannelCredentials } from "@grpc/grpc-js";
import {
  ExpansionRequest,
  ExpansionResponse,
} from "../../proto/beam_expansion_api";
import {
  ExpansionServiceClient,
  IExpansionServiceClient,
} from "../../proto/beam_expansion_api.grpc-client";
import { expansionReq } from "./expansion_request";

const client = new ExpansionServiceClient(
  // TODO(pabloem): Do we need to implement this?
  "localhost:4444",
  ChannelCredentials.createInsecure()
);

async function main() {
  await callExpand(client);
}

function callExpand(client: IExpansionServiceClient) {
  console.log("Calling Expand function with ", expansionReq);

  let req = expansionReq;

  const call = client.expand(req, (err, value) => {
    if (err) {
      console.log("got err: ", err);
    }
    if (value) {
      console.log("got response message: ", value);
    }
  });

  call.on("metadata", (arg1) => {
    console.log("got response headers: ", arg1);
  });

  call.on("status", (arg1) => {
    console.log("got status: ", arg1);
  });

  return new Promise<void>((resolve) => {
    call.on("status", () => resolve());
  });
}

main()
  .catch((e) => console.error(e))
  .finally(() => process.exit());
