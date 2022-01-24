const fs = require("fs");
const path = require("path");

// TODO: Is there a more standard way to do this?
export const version: string = JSON.parse(
  fs.readFileSync(path.resolve(__dirname, "..", "..", "..", "package.json"))
)["version"];
