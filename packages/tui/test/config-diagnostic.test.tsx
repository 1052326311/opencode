import { expect, test } from "bun:test"
import { createAppFixture } from "./fixture/app"

test("renders the rejected CLI config warning in the TUI", async () => {
  await using setup = await createAppFixture({
    diagnostic: "Invalid CLI config at /tmp/cli.json: leader",
  })
  await setup.ready
  await setup.waitForFrame((frame) => frame.includes("Invalid CLI config"))
  expect(setup.captureCharFrame()).toContain("leader")
})

test("does not render a config warning for valid settings", async () => {
  await using setup = await createAppFixture({ config: { animations: false, session: { sidebar: "hide" } } })
  await setup.ready
  await setup.renderOnce()
  expect(setup.captureCharFrame()).not.toContain("Invalid CLI config")
})
