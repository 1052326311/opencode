import { runNonInteractiveWithOptions } from "../../src/run/run"

await runNonInteractiveWithOptions(
  {
    server: { endpoint: { url: "http://127.0.0.1:1" } },
    message: ["prompt"],
    format: "json",
    file: ["missing.txt"],
  },
  { root: process.argv[2] },
)
process.stdout.write(`ROOT=${process.cwd()}\n`)
process.exitCode = 0
