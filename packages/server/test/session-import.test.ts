import { expect } from "bun:test"
import { Session } from "@opencode/schema/session"
import { SessionTransfer } from "@opencode/schema/session-transfer"
import { Effect, Schema } from "effect"
import { it } from "../../core/test/lib/effect"
import { ServerFetch } from "../src/fetch"

const SessionResponse = Schema.Struct({ data: Schema.toEncoded(Session.Info) })
const SessionsResponse = Schema.Struct({ data: Schema.Array(Schema.toEncoded(Session.Info)) })
const TransferResponse = Schema.Struct({ data: Schema.toEncoded(SessionTransfer.Data) })

const setup = Effect.gen(function* () {
  const handler = yield* ServerFetch.make({
    app: { version: "test" },
    database: { path: ":memory:" },
    fs: { filewatcher: false },
  })
  return (path: string, body?: unknown, status = 200) =>
    Effect.promise(async () => {
      const response = await handler(
        new Request(`http://opencode.local${path}`, {
          method: body === undefined ? "GET" : "POST",
          headers: { "content-type": "application/json" },
          body: body === undefined ? undefined : JSON.stringify(body),
        }),
      )
      const json: unknown = await response.json()
      expect(response.status).toBe(status)
      return json
    })
})

it.live("preserves imported parentID through HTTP import, read, and parent filter", () =>
  Effect.gen(function* () {
    const request = yield* setup
    const parent = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", { title: "Parent" }))
    const id = Session.ID.create()
    const imported = Schema.decodeUnknownSync(SessionResponse)(
      yield* request("/api/experimental/session/import", {
        info: { ...parent.data, id, parentID: parent.data.id, title: "Imported child" },
        messages: [],
      }),
    )
    const read = Schema.decodeUnknownSync(SessionResponse)(yield* request(`/api/session/${id}`))
    const children = Schema.decodeUnknownSync(SessionsResponse)(
      yield* request(`/api/session?parentID=${parent.data.id}`),
    )
    expect({
      imported: imported.data.parentID,
      read: read.data.parentID,
      children: children.data.map((child) => child.id),
    }).toEqual({ imported: parent.data.id, read: parent.data.id, children: [id] })
  }).pipe(Effect.scoped),
)
;["missing", "self"].forEach((parent) => {
  it.live(`rejects a ${parent} parent without creating the imported session`, () =>
    Effect.gen(function* () {
      const request = yield* setup
      const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
      const id = Session.ID.create()
      const parentID = parent === "self" ? id : Session.ID.create()
      const error = yield* request(
        "/api/experimental/session/import",
        { info: { ...template.data, id, parentID }, messages: [] },
        404,
      )
      expect(error).toMatchObject({ _tag: "SessionNotFoundError", sessionID: parentID })
      expect(yield* request(`/api/session/${id}`, undefined, 404)).toMatchObject({
        _tag: "SessionNotFoundError",
        sessionID: id,
      })
    }).pipe(Effect.scoped),
  )
})

it.live("omits native compaction context from sanitized HTTP exports", () =>
  Effect.gen(function* () {
    const request = yield* setup
    const template = Schema.decodeUnknownSync(SessionResponse)(yield* request("/api/session", {}))
    const id = Session.ID.create()
    const providerContext = {
      version: 1,
      provenance: {
        providerID: "openai",
        provider: "openai",
        modelID: "synthetic-model",
        route: "responses",
        protocol: "openai-responses",
        endpoint: "synthetic-endpoint-digest",
      },
      messages: [
        {
          role: "user",
          content: [
            { type: "text", text: "checkpoint-only text" },
            {
              type: "media",
              media: { source: { type: "base64", data: "U1lOVEhFVElDLUlNQUdFLTU1NTg1", mediaType: "image/png" } },
            },
          ],
        },
      ],
    }
    yield* request("/api/experimental/session/import", {
      info: { ...template.data, id },
      messages: [
        {
          id: "msg_checkpoint",
          type: "compaction",
          status: "completed",
          reason: "auto",
          summary: "private summary",
          recent: "private recent",
          providerState: { token: "private state" },
          providerContext,
          time: { created: 1 },
        },
      ],
    })

    const path = `/api/experimental/session/${id}/export`
    const plain = Schema.decodeUnknownSync(TransferResponse)(yield* request(path))
    expect(plain.data.messages[0]).toMatchObject({
      summary: "private summary",
      recent: "private recent",
      providerState: { token: "private state" },
      providerContext,
    })

    const sanitized = Schema.decodeUnknownSync(TransferResponse)(yield* request(`${path}?sanitize=true`))
    expect(sanitized.data.messages[0]).toMatchObject({
      summary: "[redacted:compaction-summary:msg_checkpoint]",
      recent: "[redacted:compaction-recent:msg_checkpoint]",
      providerState: { redacted: "compaction-provider-state:msg_checkpoint" },
    })
    expect(sanitized.data.messages[0]).not.toHaveProperty("providerContext")
    expect(JSON.stringify(sanitized)).not.toContain("checkpoint-only text")
    expect(JSON.stringify(sanitized)).not.toContain("U1lOVEhFVElDLUlNQUdFLTU1NTg1")
  }).pipe(Effect.scoped),
)
