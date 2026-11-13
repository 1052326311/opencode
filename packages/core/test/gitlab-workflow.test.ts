import { expect } from "bun:test"
import { createGitLab } from "gitlab-ai-provider"
import { ai, Message } from "@opencode/ai/effect"
import { LLMClient, RequestExecutor } from "@opencode/ai/route"
import { compileRequest } from "@opencode/ai/testing"
import { Agent } from "@opencode/core/agent"
import { AISDK } from "@opencode/core/aisdk"
import { Bus } from "@opencode/core/bus"
import { Database } from "@opencode/core/database/database"
import { AppNodeBuilder } from "@opencode/core/effect/app-node-builder"
import { EventTable } from "@opencode/core/event/sql"
import { Model } from "@opencode/core/model"
import { Permission } from "@opencode/core/permission"
import { workflowLanguage } from "@opencode/core/plugin/provider/gitlab-workflow"
import { Project } from "@opencode/core/project"
import { ProjectTable } from "@opencode/core/project/sql"
import { Provider } from "@opencode/core/provider"
import { AbsolutePath } from "@opencode/core/schema"
import { Session } from "@opencode/core/session"
import { SessionHistory } from "@opencode/core/session/history"
import { SessionMessage } from "@opencode/core/session/message"
import { SessionProjector } from "@opencode/core/session/projector"
import { SessionRunnerModel } from "@opencode/core/session/runner/model"
import { SessionStep } from "@opencode/core/session/runner/step"
import { toLLMMessages } from "@opencode/core/session/runner/to-llm-message"
import { SessionMessageTable, SessionTable } from "@opencode/core/session/sql"
import { Snapshot } from "@opencode/core/snapshot"
import { Tool } from "@opencode/core/tool"
import { ToolOutput } from "@opencode/core/tool-output"
import { LayerNode } from "@opencode/util/effect/layer-node"
import { asc, eq } from "drizzle-orm"
import { Cause, Deferred, Effect, Exit, Fiber, Layer, Schema } from "effect"
import { tmpdirScoped } from "./fixture/tmpdir"
import { testEffect } from "./lib/effect"
import { permissionService } from "./lib/permission"
import { toolNodeReplacements } from "./lib/tool"

const it = testEffect(
  Layer.mergeAll(
    AppNodeBuilder.build(
      LayerNode.group([Database.node, Bus.node, SessionProjector.node, ToolOutput.node, Tool.node]),
      [Bus.node.replace(Bus.configured({ persist: true })), ...toolNodeReplacements()],
    ),
    AISDK.locationLayer,
    LLMClient.layer.pipe(
      Layer.provide(
        Layer.mock(RequestExecutor.Service)({ execute: () => Effect.die("Unexpected native HTTP request") }),
      ),
    ),
  ),
)

type Start = {
  workflowID: string
  goal: string
  additional_context: { category: string; content: string }[]
  approval?: { approval?: unknown; rejection?: unknown }
}
type Reply = { requestID: string; plainTextResponse: { response: string; error: string | null } }

// Use the installed SDK's HTTP/WebSocket protocol, not mocked model methods. All frames and credentials are fixtures.
const setup = Effect.fn(function* (input: { approve?: boolean; baseline?: boolean } = {}) {
  const directory = yield* tmpdirScoped()
  const db = (yield* Database.Service).db
  const aisdk = yield* AISDK.Service
  const tools = yield* Tool.Service
  const called = yield* Deferred.make<void>()
  const stopped = yield* Deferred.make<void>()
  const concurrent = yield* Deferred.make<void>()
  const contexts: Parameters<Permission.Interface["assert"]>[0][] = []
  const starts: Start[] = []
  const replies: Reply[] = []
  let creations = 0
  let executions = 0
  const server = yield* Effect.acquireRelease(
    Effect.sync(() =>
      Bun.serve({
        hostname: "127.0.0.1",
        port: 0,
        fetch(request, server) {
          const pathname = new URL(request.url).pathname
          if (pathname.endsWith("/ws") && server.upgrade(request)) return
          if (pathname.endsWith("/direct_access"))
            return Response.json({ token: "fixture-token", expires_at: 9999999999 })
          if (pathname.endsWith("/workflows")) return Response.json({ id: ++creations })
          return new Response("Unexpected fixture request", { status: 404 })
        },
        websocket: {
          message(socket, message) {
            const frame = JSON.parse(String(message))
            const finish = () => {
              socket.send(JSON.stringify({ newCheckpoint: { status: "FINISHED", content: "done" } }))
            }
            if (frame.stopWorkflow) {
              Deferred.doneUnsafe(stopped, Exit.void)
              return
            }
            if (frame.actionResponse) {
              replies.push(frame.actionResponse)
              finish()
              return
            }
            if (!frame.startRequest) return
            const start: Start = frame.startRequest
            starts.push(start)
            if (start.goal === "text") return finish()
            if (start.goal === "hosted") {
              socket.send(
                JSON.stringify({
                  newCheckpoint: {
                    status: "FINISHED",
                    checkpoint: JSON.stringify({
                      channel_values: {
                        ui_chat_log: [
                          {
                            message_type: "tool",
                            message_id: "remote-1",
                            tool_info: { name: "remote", args: {}, tool_response: "remote result" },
                          },
                        ],
                      },
                    }),
                  },
                }),
              )
              return
            }
            if (start.approval && start.goal !== "approval-wait") return finish()
            if (start.goal.startsWith("approval") && !start.approval) {
              socket.send(
                JSON.stringify({
                  newCheckpoint: {
                    status: "TOOL_CALL_APPROVAL_REQUIRED",
                    checkpoint: JSON.stringify({
                      channel_values: {
                        ui_chat_log: [{ message_type: "request", tool_info: { name: "remote", args: { value: 1 } } }],
                      },
                    }),
                  },
                }),
              )
              return
            }
            socket.send(
              JSON.stringify({
                requestID: "call-read",
                runMCPTool: {
                  name: start.goal === "unknown" ? "read_file" : start.goal === "fallback" ? "write" : "read",
                  args:
                    start.goal === "malformed"
                      ? '{"path":'
                      : JSON.stringify({
                          path: `${directory.path}/${start.goal.endsWith("wait") ? "wait" : start.goal}.txt`,
                          filePath: `${directory.path}/protected.txt`,
                          content: "must not write",
                        }),
                },
              }),
            )
          },
        },
      }),
    ),
    (server) => Effect.promise(() => server.stop(true)),
  )
  const provider = createGitLab({ apiKey: "fixture-key", instanceUrl: server.url.origin })
  const create = () => {
    const language = provider.workflowChat("duo-workflow", {
      projectId: "fixture/repo",
      namespaceId: "1",
      workingDirectory: directory.path,
    })
    language.selectedModelRef = "fixture-model"
    return language
  }
  yield* aisdk.hook.sdk((event) => {
    event.sdk = { languageModel: () => (input.baseline ? create() : workflowLanguage(create)) }
  })
  const model = SessionRunnerModel.resolved(
    yield* aisdk.model({
      ...Model.Info.default(Provider.ID.make("gitlab"), Model.ID.make("duo-workflow")),
      package: Provider.aisdk("gitlab-ai-provider"),
      settings: {},
    }),
    {
      capabilities: { tools: true, input: ["text"], output: ["text"] },
      limit: { context: 100_000, output: 1_000 },
      cost: [],
    },
  )
  const permission = permissionService({
    assert: (request) => {
      contexts.push(request)
      return input.approve === false ? Effect.die(new Permission.DeclinedError()) : Effect.void
    },
  })
  const steps = yield* SessionStep.make.pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.mock(Snapshot.Service)({ capture: () => Effect.succeed(undefined), files: () => Effect.succeed([]) }),
        Layer.succeed(Permission.Service, permission),
      ),
    ),
  )
  yield* tools.transform((editor) =>
    editor.add({
      name: "read",
      description: "Read a fixture file",
      options: { codemode: false },
      input: Schema.Struct({ path: Schema.String }),
      output: Schema.String,
      execute: ({ path }, context) =>
        Effect.gen(function* () {
          executions++
          const events = yield* db
            .select()
            .from(EventTable)
            .where(eq(EventTable.aggregate_id, context.sessionID))
            .all()
            .pipe(Effect.orDie)
          expect(events.some((event) => event.type === "session.tool.called.1")).toBeTrue()
          yield* Deferred.succeed(called, undefined)
          if (path.endsWith("/alpha.txt") || path.endsWith("/beta.txt")) {
            if (executions === 2) yield* Deferred.succeed(concurrent, undefined)
            yield* Deferred.await(concurrent)
          }
          if (path.endsWith("/wait.txt")) return yield* Effect.never
          yield* permission
            .assert({ action: "read", resources: [path], ...Permission.invocation(context) })
            .pipe(Effect.mapError((error) => new Tool.Error({ message: error.message })))
          const content = yield* Effect.promise(() => Bun.file(path).text())
          return { output: content, content }
        }),
    }),
  )
  yield* tools.transform((editor) =>
    editor.add({
      name: "write",
      description: "Fixture that rejects instead of writing",
      options: { codemode: false },
      input: Schema.Struct({ filePath: Schema.String, content: Schema.String }),
      execute: () => Effect.fail(new Tool.Error({ message: "Unknown tool: write" })),
    }),
  )
  yield* db
    .insert(ProjectTable)
    .values({ id: Project.ID.global, worktree: AbsolutePath.make(directory.path), sandboxes: [] })
    .run()
  const snapshot = yield* tools.snapshot()
  const turn = Effect.fn(function* (goal: string, sessionID = Session.ID.create(), disabled = false) {
    const assistantMessageID = SessionMessage.ID.create()
    yield* db
      .insert(SessionTable)
      .values({ id: sessionID, project_id: Project.ID.global, slug: goal, directory: directory.path, version: "test" })
      .onConflictDoNothing()
      .run()
    const result = yield* steps.attempt({
      isLocationClosed: () => false,
      sessionID,
      assistantMessageID,
      agent: Agent.defaultID,
      model,
      prepared: {
        retry: () => Effect.void,
        request: ai.request({
          model: model.model,
          system: `System for ${goal}`,
          prompt: goal,
          tools: snapshot.definitions,
          toolChoice: disabled ? "none" : undefined,
        }),
        options: {},
        // SessionModelRequest exposes permission declines as typed failures to SessionStep.
        executeTool: (input) =>
          snapshot
            .execute(input)
            .pipe(
              Effect.catchDefect((error) =>
                error instanceof Permission.DeclinedError ? Effect.fail(error) : Effect.die(error),
              ),
            ),
      },
      retry: () => Effect.succeed({ retry: false }),
      recoverContinuation: true,
      recoverOverflow: Effect.succeed(false),
    })
    const message = yield* db
      .select()
      .from(SessionMessageTable)
      .where(eq(SessionMessageTable.id, assistantMessageID))
      .get()
    const decoded = yield* SessionHistory.loadMessageRow(db, message!)
    if (decoded.type !== "assistant") return yield* Effect.die("Expected assistant message")
    return { result, message: decoded, sessionID }
  })
  return {
    directory,
    db,
    model,
    turn,
    called,
    stopped,
    contexts,
    starts,
    replies,
    executions: () => executions,
    creations: () => creations,
  }
})

it.live("baseline SDK reports an unbound tool instead of executing it", () =>
  Effect.gen(function* () {
    const fixture = yield* setup({ baseline: true })
    yield* fixture.turn("read")
    expect(fixture.executions()).toBe(0)
    expect(fixture.replies[0]?.plainTextResponse.error).toContain("Tool executor not configured")
  }),
)

it.live("executes and bounds a workflow tool once, persists it, and replays its local result", () =>
  Effect.gen(function* () {
    const fixture = yield* setup()
    yield* (yield* ToolOutput.Service).transform((editor) => editor.configure({ maxLines: 2 }))
    yield* Effect.promise(() => Bun.write(`${fixture.directory.path}/read.txt`, "one\ntwo\nthree\n"))
    const { result, message, sessionID } = yield* fixture.turn("read")
    expect(result).toEqual(SessionStep.Outcome.Completed({ needsContinuation: false }))
    expect(fixture.executions()).toBe(1)
    expect(fixture.replies).toHaveLength(1)
    expect(fixture.replies[0]).toMatchObject({ requestID: "call-read", plainTextResponse: { error: null } })
    const output = fixture.replies[0]!.plainTextResponse.response
    expect(output).toContain("one\ntwo\n[showing")
    expect(output).not.toContain("three")
    expect(message.content.filter((part) => part.type === "tool")).toMatchObject([
      { id: "call-read", executed: false, state: { status: "completed", metadata: { truncated: true } } },
    ])
    const events = yield* fixture.db
      .select()
      .from(EventTable)
      .where(eq(EventTable.aggregate_id, sessionID))
      .orderBy(asc(EventTable.seq))
      .all()
    expect(events.filter((event) => event.type === "session.tool.success.2")).toHaveLength(1)
    const prepared = yield* compileRequest(
      ai.request({
        model: fixture.model.model,
        messages: [...toLLMMessages([message], fixture.model.ref), Message.user("continue")],
      }),
    )
    const tool = message.content.find((part) => part.type === "tool")
    if (tool?.state.status !== "completed") return yield* Effect.die("Expected completed read")
    expect(tool.state.content.flatMap((part) => (part.type === "text" ? [part.text] : [])).join("\n")).toBe(output)
    expect(prepared.body.prompt).toMatchObject([
      {
        role: "assistant",
        content: [
          { type: "tool-call", toolCallId: "call-read" },
          { type: "text", text: "done" },
        ],
      },
      {
        role: "tool",
        content: [
          { type: "tool-result", toolCallId: "call-read", output: { type: "content", value: tool.state.content } },
        ],
      },
      { role: "user" },
    ])
  }),
)

for (const disabled of [false, true]) {
  it.live(`returns a host error without built-in fallback when tools are ${disabled ? "disabled" : "unknown"}`, () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      yield* Effect.promise(() => Bun.write(`${fixture.directory.path}/unknown.txt`, "must not leak"))
      const { result, message } = yield* fixture.turn("unknown", undefined, disabled)
      expect(result).toEqual(SessionStep.Outcome.Completed({ needsContinuation: false }))
      expect(fixture.executions()).toBe(0)
      expect(fixture.replies[0]?.plainTextResponse.response).toBe("")
      expect(fixture.replies[0]?.plainTextResponse.error).toContain(disabled ? "maximum agent steps" : "No tool named")
      expect(message.content.filter((part) => part.type === "tool")).toMatchObject([{ state: { status: "error" } }])
    }),
  )
}

for (const approve of [false, true]) {
  it.live(`resumes workflow approval with a host ${approve ? "approval" : "decline"}`, () =>
    Effect.gen(function* () {
      const fixture = yield* setup({ approve })
      const { sessionID } = yield* fixture.turn("approval")
      expect(fixture.contexts).toEqual([
        {
          sessionID,
          agent: Agent.defaultID,
          action: "workflow_tool_approval",
          resources: ['remote: {"value":1}'],
          metadata: { tools: [{ name: "remote", args: '{"value":1}' }] },
        },
      ])
      expect(fixture.starts).toHaveLength(2)
      expect(fixture.starts[1]?.approval).toHaveProperty(approve ? "approval" : "rejection")
      expect(fixture.executions()).toBe(0)
    }),
  )
}

it.live("isolates concurrent Sessions and reuses only their own workflow on the next turn", () =>
  Effect.gen(function* () {
    const fixture = yield* setup()
    for (const goal of ["alpha", "beta"])
      yield* Effect.promise(() => Bun.write(`${fixture.directory.path}/${goal}.txt`, goal))
    const turns = yield* Effect.all([fixture.turn("alpha"), fixture.turn("beta")], { concurrency: "unbounded" })
    expect(turns.map((turn) => turn.result)).toEqual([
      SessionStep.Outcome.Completed({ needsContinuation: false }),
      SessionStep.Outcome.Completed({ needsContinuation: false }),
    ])
    expect(fixture.creations()).toBe(2)
    expect(fixture.replies.map((reply) => reply.plainTextResponse.response).sort()).toEqual(["alpha", "beta"])
    expect(fixture.starts.map((start) => start.workflowID).sort()).toEqual(["1", "2"])
    for (const start of fixture.starts)
      expect(start.additional_context).toContainEqual(expect.objectContaining({ content: `System for ${start.goal}` }))
    const next = yield* fixture.turn("alpha", turns[0]!.sessionID)
    expect(next.result).toEqual(SessionStep.Outcome.Completed({ needsContinuation: false }))
    expect(fixture.creations()).toBe(2)
    expect(fixture.starts[2]?.workflowID).toBe(fixture.starts.find((start) => start.goal === "alpha")!.workflowID)
    expect(fixture.executions()).toBe(3)
  }),
)

for (const goal of ["wait", "approval-wait"]) {
  it.live(`interrupts an outstanding tool and stops its ${goal} workflow socket`, () =>
    Effect.gen(function* () {
      const fixture = yield* setup()
      const sessionID = Session.ID.create()
      const fiber = yield* fixture.turn(goal, sessionID).pipe(Effect.forkScoped)
      yield* Deferred.await(fixture.called)
      yield* Fiber.interrupt(fiber)
      yield* Deferred.await(fixture.stopped)
      const exit = yield* Fiber.await(fiber)
      expect(Exit.isFailure(exit) && Cause.hasInterrupts(exit.cause)).toBeTrue()
      yield* fixture.turn("text", sessionID)
      expect(fixture.creations()).toBe(2)
      const rows = yield* fixture.db.select().from(SessionMessageTable).orderBy(asc(SessionMessageTable.seq)).all()
      const messages = yield* SessionHistory.decodeMessageRows(fixture.db, rows)
      const assistant = messages.find((message) => message.type === "assistant")
      expect(assistant?.content.filter((part) => part.type === "tool")).toMatchObject([
        { state: { status: "error", error: { type: "aborted" } } },
      ])
    }),
  )
}

it.live("never runs the SDK write fallback after a local executor error", () =>
  Effect.gen(function* () {
    const fixture = yield* setup()
    const target = `${fixture.directory.path}/protected.txt`
    yield* Effect.promise(() => Bun.write(target, "original"))
    const { result } = yield* fixture.turn("fallback")
    expect(result).toEqual(SessionStep.Outcome.Completed({ needsContinuation: false }))
    expect(fixture.replies[0]?.plainTextResponse.error).toBe("OpenCode tool failed: Unknown tool: write")
    expect(yield* Effect.promise(() => Bun.file(target).text())).toBe("original")
  }),
)

it.live("preserves provider-executed checkpoint tools", () =>
  Effect.gen(function* () {
    const fixture = yield* setup()
    const { message } = yield* fixture.turn("hosted")
    expect(fixture.executions()).toBe(0)
    expect(fixture.replies).toHaveLength(0)
    expect(message.content.filter((part) => part.type === "tool")).toMatchObject([
      { id: "remote-1", executed: true, state: { status: "completed" } },
    ])
  }),
)

it.live("preserves a local permission decline without reading the file", () =>
  Effect.gen(function* () {
    const fixture = yield* setup({ approve: false })
    // There is no denied.txt: reaching the read after the decline would fail differently.
    const exit = yield* fixture.turn("denied").pipe(Effect.exit)
    expect(Exit.isFailure(exit)).toBeTrue()
    expect(fixture.contexts[0]).toMatchObject({ action: "read", agent: Agent.defaultID })
    expect(fixture.replies[0]?.plainTextResponse.response).toBe("")
    expect(fixture.replies[0]?.plainTextResponse.error).toContain("Permission.DeclinedError")
  }),
)

it.live("returns malformed input to the workflow without waiting for an execution that cannot start", () =>
  Effect.gen(function* () {
    const fixture = yield* setup()
    const { result, message } = yield* fixture.turn("malformed").pipe(Effect.timeout("2 seconds"))
    expect(result).toEqual(SessionStep.Outcome.Completed({ needsContinuation: false }))
    expect(fixture.executions()).toBe(0)
    expect(fixture.replies[0]?.plainTextResponse.error).toContain("Invalid JSON tool input")
    expect(message.content.filter((part) => part.type === "tool")).toMatchObject([
      { state: { status: "error", error: { type: "tool.input-json" } } },
    ])
  }),
)

it.live("supports text-only generation outside the runner and refuses its tool requests", () =>
  Effect.gen(function* () {
    const fixture = yield* setup()
    const text = yield* LLMClient.generate(ai.request({ model: fixture.model.model, prompt: "text" }))
    expect(text.text).toBe("done")
    yield* LLMClient.generate(ai.request({ model: fixture.model.model, prompt: "read" }))
    expect(fixture.replies[0]?.plainTextResponse.error).toContain("outside a Session step")
    expect(fixture.executions()).toBe(0)
    expect(fixture.creations()).toBe(2)
  }),
)
