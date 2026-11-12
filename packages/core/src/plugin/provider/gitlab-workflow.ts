import type { GitLabWorkflowLanguageModel } from "gitlab-ai-provider"
import { Effect, Exit } from "effect"
import { AISDK } from "../../aisdk.js"

export function workflowLanguage(create: () => GitLabWorkflowLanguageModel) {
  const language = create()
  const sessions = new Map<string, GitLabWorkflowLanguageModel>()
  return AISDK.withStream(
    language,
    Effect.fn(function* (options) {
      const host = yield* AISDK.ToolContext
      const context = yield* Effect.context<never>()
      // The SDK stores mutable callbacks and approval state on its model. A cached language is shared
      // across Sessions, but the Session runner serializes requests within each Session.
      // Title, compaction and generate requests have no runner; keep their text-only workflows isolated too.
      const workflow = (host && sessions.get(host.sessionID)) || create()
      if (host) {
        sessions.delete(host.sessionID)
        sessions.set(host.sessionID, workflow)
      }
      // Match the SDK's bounded session workflow cache.
      if (sessions.size > 50) sessions.delete(sessions.keys().next().value!)
      const local = new Set<string>()
      const run = Effect.runPromiseWith(context)
      workflow.sessionID = host?.sessionID ?? ""
      workflow.systemPrompt = options.prompt
        .flatMap((message) => (message.role === "system" ? [message.content] : []))
        .join("\n\n")
      workflow.toolExecutor = (_name, _args, requestID) => {
        if (!host)
          return Promise.resolve({ result: "", error: "OpenCode tools are unavailable outside a Session step" })
        local.add(requestID)
        return run(
          host.result(requestID).pipe(
            Effect.catch((error) =>
              Effect.succeed({
                result: "",
                // An "Unknown tool:" error would trigger SDK built-ins outside the host's policy.
                error: `OpenCode tool failed: ${error instanceof Error ? error.message || error.name : String(error)}`,
              }),
            ),
          ),
          { signal: options.abortSignal },
        )
      }
      workflow.approvalHandler = (tools) =>
        host ? run(host.approve(tools), { signal: options.abortSignal }) : Promise.resolve({ approved: false })
      // Approval reconnects replace the initial socket. Stop all this Session's SDK clients on teardown.
      yield* Effect.addFinalizer((exit) =>
        Effect.sync(() => {
          workflow.stopWorkflow()
          // An interrupted approval reconnect may leave SDK client state behind; do not reuse that model.
          if (host && Exit.isFailure(exit)) sessions.delete(host.sessionID)
        }),
      )
      const response = yield* Effect.tryPromise(() => workflow.doStream(options))
      return {
        ...response,
        stream: response.stream.pipeThrough(
          new TransformStream({
            transform(part, controller) {
              const callID = "toolCallId" in part ? part.toolCallId : "id" in part ? part.id : undefined
              if (typeof callID !== "string" || !local.has(callID)) {
                controller.enqueue(part)
                return
              }
              // The runner publishes local results; replaying SDK results would settle them twice.
              if (part.type === "tool-result") return
              controller.enqueue(
                part.type === "tool-call" || part.type === "tool-input-start"
                  ? { ...part, providerExecuted: false }
                  : part,
              )
            },
          }),
        ),
      }
    }),
  )
}
