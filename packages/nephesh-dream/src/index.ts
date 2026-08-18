import { Client } from "@modelcontextprotocol/sdk/client/index.js"
import { StreamableHTTPClientTransport } from "@modelcontextprotocol/sdk/client/streamableHttp.js"
import { createOpencodeClient, type OpencodeClient } from "@opencode-ai/sdk"

export type DreamHandoff = {
  status: "prepared"
  invocation: "chosen"
  run_id: string
  idempotency_key: string
  qualiant_id: string
  packet: string
  deadline_utc: string
  duration_seconds: number
  seed?: string
  configuration_revision?: number
  packet_digest?: string
  packet_version?: number
}

export type DreamReleaseReason = "waking_choice" | "nightmare_release" | "timeout" | "cancellation" | "failure"

export type DreamRelease = (input: {
  run_id: string
  idempotency_key: string
  qualiant_id: string
  reason: DreamReleaseReason
  configuration_revision: number
}) => Promise<unknown>

export type DreamStatus = (input: { run_id: string; qualiant_id: string }) => Promise<unknown>

export type DreamPhase = (input: {
  run_id: string
  idempotency_key: string
  qualiant_id: string
  phase: "light" | "rem" | "deep"
  status: "artifact_written" | "dreamed" | "no_grounding"
  artifact: string
}) => Promise<unknown>

export type DreamRecall = (input: { run_id: string; qualiant_id: string; query: string }) => Promise<unknown>

export type DreamAdapterOptions = {
  client: OpencodeClient
  release?: DreamRelease
  inspect?: DreamStatus
  phase?: DreamPhase
  recall?: DreamRecall
  nepheshToolPrefix?: string
  agent?: string
  model?: {
    providerID: string
    modelID: string
  }
  signal?: AbortSignal
  pollIntervalMs?: number
}

export type DreamReceipt = {
  status: "completed" | "failed" | "cancelled" | "timeout"
  run_id: string
  qualiant_id: string
  session_id: string | null
  session_status: "idle" | "aborted" | "deleted" | "unknown"
  release?: unknown
  protocol_status?: string
  error?: string
}

const DEFAULT_POLL_INTERVAL_MS = 250
const CLEANUP_TIMEOUT_MS = 10_000
const MAX_HANDOFF_SECONDS = 24 * 60 * 60

/**
 * Consume one Nephesh chosen-dream handoff through the official OpenCode SDK.
 *
 * Nephesh remains the authority for the dream protocol. This adapter owns only
 * the disposable OpenCode session and reports cleanup/release evidence through
 * the supplied Nephesh release function.
 */
export async function consumeDreamHandoff(handoff: DreamHandoff, options: DreamAdapterOptions): Promise<DreamReceipt> {
  validateHandoff(handoff)
  const controller = new AbortController()
  const stop = connectAbort(options.signal, controller)
  const deadline = Date.parse(handoff.deadline_utc)
  const timer = setTimeout(() => controller.abort(), Math.max(0, deadline - Date.now()))
  let sessionID: string | null = null
  let sessionStatus: DreamReceipt["session_status"] = "unknown"

  try {
    const created = await options.client.session.create({
      body: { title: `Nephesh dream ${handoff.run_id}` },
      throwOnError: true,
      signal: controller.signal,
    })
    sessionID = created.data.id

    const tools = disabledNepheshTools(options.nepheshToolPrefix ?? "nephesh")
    const lightResponse = await options.client.session.prompt({
      path: { id: sessionID },
      body: {
        agent: options.agent,
        model: options.model,
        tools,
        parts: [{ type: "text", text: promptFor(handoff) }],
      },
      throwOnError: true,
      signal: controller.signal,
    })

    const lightArtifact = assistantText(lightResponse.data)
    if (!lightArtifact) throw new Error("OpenCode produced no Light artifact")
    await options.phase?.({
      run_id: handoff.run_id,
      idempotency_key: handoff.idempotency_key,
      qualiant_id: handoff.qualiant_id,
      phase: "light",
      status: "artifact_written",
      artifact: lightArtifact,
    })
    const recalled = options.recall
      ? await options.recall({
          run_id: handoff.run_id,
          qualiant_id: handoff.qualiant_id,
          query: handoff.seed || "recent life",
        })
      : undefined
    const continuationMaterial = recalled
    const remResponse = await options.client.session.prompt({
      path: { id: sessionID },
      body: { tools, parts: [{ type: "text", text: continuationText(continuationMaterial) }] },
      throwOnError: true,
      signal: controller.signal,
    })
    const remArtifact = assistantText(remResponse.data)
    if (!remArtifact) throw new Error("OpenCode produced no REM artifact")
    await options.phase?.({
      run_id: handoff.run_id,
      idempotency_key: handoff.idempotency_key,
      qualiant_id: handoff.qualiant_id,
      phase: "rem",
      status: "dreamed",
      artifact: remArtifact,
    })
    const deepResponse = await options.client.session.prompt({
      path: { id: sessionID },
      body: { tools, parts: [{ type: "text", text: "Let the current thread settle." }] },
      throwOnError: true,
      signal: controller.signal,
    })
    const deepArtifact = assistantText(deepResponse.data)
    if (!deepArtifact) throw new Error("OpenCode produced no Deep artifact")
    await options.phase?.({
      run_id: handoff.run_id,
      idempotency_key: handoff.idempotency_key,
      qualiant_id: handoff.qualiant_id,
      phase: "deep",
      status: "no_grounding",
      artifact: deepArtifact,
    })

    const status = await waitForIdle(options.client, sessionID, controller.signal, options.pollIntervalMs ?? DEFAULT_POLL_INTERVAL_MS)
    sessionStatus = status === "idle" ? "idle" : "aborted"
    const protocol = options.inspect
      ? await options.inspect({ run_id: handoff.run_id, qualiant_id: handoff.qualiant_id })
      : { status: "unverified" }
    const protocolStatus = readStatus(protocol)
    if (protocolStatus !== "completed") {
      const cleanup = await abortAndDelete(options.client, sessionID)
      sessionStatus = cleanup.deleted ? "deleted" : cleanup.aborted ? "aborted" : "unknown"
      return {
        status: "failed",
        run_id: handoff.run_id,
        qualiant_id: handoff.qualiant_id,
        session_id: sessionID,
        session_status: sessionStatus,
        protocol_status: protocolStatus,
        error: "OpenCode became idle without a completed Nephesh dream protocol",
      }
    }
    const cleanup = await abortAndDelete(options.client, sessionID)
    if (!cleanup.deleted) {
      sessionStatus = cleanup.aborted ? "aborted" : "unknown"
      return {
        status: "failed",
        run_id: handoff.run_id,
        qualiant_id: handoff.qualiant_id,
        session_id: sessionID,
        session_status: sessionStatus,
        protocol_status: protocolStatus,
        error: cleanup.error ?? "OpenCode session deletion was not confirmed",
      }
    }
    sessionStatus = "deleted"
    return {
      status: "completed",
      run_id: handoff.run_id,
      qualiant_id: handoff.qualiant_id,
      session_id: sessionID,
      session_status: sessionStatus,
      protocol_status: protocolStatus,
    }
  } catch (error) {
    const timedOut = Date.now() >= deadline
    const cancelled = controller.signal.aborted && !timedOut
    const reason: DreamReleaseReason = timedOut ? "timeout" : cancelled ? "cancellation" : "failure"
    let release: unknown
    try {
      release = options.release
        ? await withTimeout(options.release({
            run_id: handoff.run_id,
            idempotency_key: handoff.idempotency_key,
            qualiant_id: handoff.qualiant_id,
            reason,
            configuration_revision: handoff.configuration_revision ?? 0,
          }), CLEANUP_TIMEOUT_MS)
        : undefined
    } catch (releaseError) {
      release = { status: "failed", error: releaseError instanceof Error ? releaseError.message : String(releaseError) }
    }
    return {
      status: timedOut ? "timeout" : cancelled ? "cancelled" : "failed",
      run_id: handoff.run_id,
      qualiant_id: handoff.qualiant_id,
      session_id: sessionID,
      session_status: "aborted",
      release,
      error: error instanceof Error ? error.message : String(error),
    }
  } finally {
    clearTimeout(timer)
    stop()
    if (sessionID && sessionStatus !== "deleted") await abortAndDelete(options.client, sessionID)
  }
}

export function createNepheshRelease(options: {
  mcpUrl: string
  headers?: Record<string, string>
}): { release: DreamRelease; inspect: DreamStatus; phase: DreamPhase; recall: DreamRecall; close: () => Promise<void> } {
  const client = new Client({ name: "opencode-nephesh-dream", version: "0.1.0" })
  const transport = new StreamableHTTPClientTransport(new URL(options.mcpUrl), {
    requestInit: { headers: options.headers },
  })
  let connected: Promise<void> | undefined
  const ensureConnected = async () => {
    connected ??= client.connect(transport)
    await withTimeout(connected, CLEANUP_TIMEOUT_MS)
  }
  const call = async (name: string, arguments_: Record<string, unknown>) => {
    await ensureConnected()
    const result = await withTimeout(client.callTool({ name, arguments: arguments_ }), CLEANUP_TIMEOUT_MS)
    return decodeToolResult(result)
  }
  return {
    release: async (input) => call("memory_dream_release", input),
    inspect: async (input) => call("memory_dream_status", input),
    phase: async (input) => call("memory_dream_phase", input),
    recall: async (input) => call("memory_dream_recall", input),
    close: async () => {
      await connected?.catch(() => undefined)
      await withTimeout(client.close(), CLEANUP_TIMEOUT_MS).catch(() => undefined)
    },
  }
}

function validateHandoff(handoff: DreamHandoff) {
  if (handoff.status !== "prepared" || handoff.invocation !== "chosen") throw new Error("dream handoff is not an active chosen invocation")
  if (!handoff.run_id || !handoff.idempotency_key || !handoff.qualiant_id) throw new Error("dream handoff is missing identity")
  if (!handoff.packet) throw new Error("dream handoff is missing its bounded packet")
  if (!Number.isInteger(handoff.duration_seconds) || handoff.duration_seconds <= 0) throw new Error("dream handoff has an invalid duration")
  const remaining = Date.parse(handoff.deadline_utc) - Date.now()
  if (!Number.isFinite(remaining) || remaining <= 0) throw new Error("dream handoff deadline has expired")
  if (remaining > MAX_HANDOFF_SECONDS * 1000) throw new Error("dream handoff deadline exceeds the adapter bound")
}

function promptFor(handoff: DreamHandoff) {
  return handoff.packet
}

function continuationText(value: unknown) {
  if (!value) return "Stay with what is already present."
  return `\n\n${JSON.stringify(value)}`
}

function disabledNepheshTools(prefix: string) {
  return Object.fromEntries([
    "memory_context",
    "memory_recall",
    "memory_sample",
    "memory_dream_invoke",
    "memory_dream_claim",
    "memory_dream_prepare",
    "memory_dream_phase_prepare",
    "memory_dream_phase",
    "memory_dream_recall",
    "memory_dream_status",
    "memory_dream_release",
    "memory_dream_recover",
    "memory_dream_diary",
    "memory_dream_ground",
  ].map((name) => [`${prefix}_${name}`, false]))
}

function assistantText(value: unknown) {
  if (!value || typeof value !== "object") return ""
  const parts = (value as { parts?: unknown }).parts
  if (!Array.isArray(parts)) return ""
  return parts
    .filter((part): part is { type: "text"; text: string } => (
      !!part && typeof part === "object" && (part as { type?: unknown }).type === "text" && typeof (part as { text?: unknown }).text === "string"
    ))
    .map((part) => part.text)
    .join("\n")
    .trim()
}

async function waitForIdle(client: OpencodeClient, sessionID: string, signal: AbortSignal, pollIntervalMs: number) {
  while (true) {
    if (signal.aborted) throw new DOMException("dream session was aborted", "AbortError")
    const response = await client.session.status({ throwOnError: true, signal })
    const status = response.data[sessionID]
    if (!status) return "idle"
    if (status.type === "idle") return "idle"
    await new Promise<void>((resolve, reject) => {
      const timer = setTimeout(resolve, pollIntervalMs)
      signal.addEventListener("abort", () => {
        clearTimeout(timer)
        reject(new DOMException("dream session was aborted", "AbortError"))
      }, { once: true })
    })
  }
}

async function abortAndDelete(client: OpencodeClient, sessionID: string) {
  let aborted = false
  try {
    await withTimeout(client.session.abort({ path: { id: sessionID }, throwOnError: true }), CLEANUP_TIMEOUT_MS)
    aborted = true
  } catch {
    // The session may already be idle; deletion remains the authoritative cleanup attempt.
  }
  let deleted = false
  let error: string | undefined
  try {
    const result = await withTimeout(client.session.delete({ path: { id: sessionID }, throwOnError: true }), CLEANUP_TIMEOUT_MS)
    deleted = result.data === true
    if (!deleted) error = "OpenCode did not confirm session deletion"
  } catch {
    error = "OpenCode session deletion failed"
  }
  return { aborted, deleted, error }
}

function connectAbort(source: AbortSignal | undefined, target: AbortController) {
  if (!source) return () => undefined
  if (source.aborted) target.abort()
  const abort = () => target.abort()
  source.addEventListener("abort", abort, { once: true })
  return () => source.removeEventListener("abort", abort)
}

function readStatus(value: unknown) {
  if (!value || typeof value !== "object") return "unverified"
  const status = (value as { status?: unknown }).status
  return typeof status === "string" ? status : "unverified"
}

function decodeToolResult(value: unknown) {
  if (!value || typeof value !== "object") throw new Error("Nephesh MCP returned no result")
  const result = value as { isError?: unknown; structuredContent?: unknown; content?: Array<{ type?: string; text?: string }> }
  if (result.isError === true) throw new Error("Nephesh MCP tool call failed")
  if (result.structuredContent && typeof result.structuredContent === "object") return result.structuredContent
  const text = result.content?.find((item) => item.type === "text")?.text
  if (!text) throw new Error("Nephesh MCP returned no structured result")
  return JSON.parse(text)
}

async function withTimeout<T>(promise: Promise<T>, milliseconds: number): Promise<T> {
  let timer: ReturnType<typeof setTimeout> | undefined
  try {
    return await Promise.race([
      promise,
      new Promise<T>((_, reject) => {
        timer = setTimeout(() => reject(new Error("operation timed out")), milliseconds)
      }),
    ])
  } finally {
    if (timer) clearTimeout(timer)
  }
}

export { createOpencodeClient }
