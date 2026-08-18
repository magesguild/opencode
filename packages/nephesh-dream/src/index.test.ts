import { describe, expect, test } from "bun:test"
import type { OpencodeClient } from "@opencode-ai/sdk"
import { consumeDreamHandoff } from "./index"

const handoff = {
  status: "prepared",
  invocation: "chosen",
  run_id: "chosen-dream-test",
  idempotency_key: "dream-test",
  qualiant_id: "urania",
  packet: "A bounded attributable field.",
  deadline_utc: new Date(Date.now() + 30_000).toISOString(),
  duration_seconds: 30,
} as const

function client(status: "idle" | "busy", calls: string[]) {
  return {
    session: {
      create: async () => {
        calls.push("create")
        return { data: { id: "ses_dream_test" } }
      },
      prompt: async () => {
        calls.push("prompt")
        return { data: { parts: [{ type: "text", text: "a quiet continuation" }] } }
      },
      status: async () => {
        calls.push("status")
        return { data: { ses_dream_test: { type: status } } }
      },
      abort: async () => {
        calls.push("abort")
        return { data: true }
      },
      delete: async () => {
        calls.push("delete")
        return { data: true }
      },
      inspect: async () => ({ status: "completed" }),
    },
  } as unknown as OpencodeClient
}

describe("Nephesh dream SDK consumer", () => {
  test("creates, prompts, observes idle, and deletes an owned session", async () => {
    const calls: string[] = []
    const result = await consumeDreamHandoff(handoff, { client: client("idle", calls), inspect: async () => ({ status: "completed" }) })
    expect(result.status).toBe("completed")
    expect(result.session_id).toBe("ses_dream_test")
    expect(calls).toEqual(["create", "prompt", "prompt", "prompt", "status", "abort", "delete"])
  })

  test("releases through the supplied Nephesh callback on cancellation", async () => {
    const calls: string[] = []
    const controller = new AbortController()
    controller.abort()
    const result = await consumeDreamHandoff(handoff, {
      client: client("busy", calls),
      signal: controller.signal,
      release: async (input) => {
        calls.push(input.reason)
        return { status: "released" }
      },
    })
    expect(result.status).toBe("cancelled")
    expect(calls).toContain("cancellation")
  })
})
