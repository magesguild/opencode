import { consumeDreamHandoff, createNepheshRelease, createOpencodeClient, type DreamHandoff } from "./index"

const chunks: Buffer[] = []
for await (const chunk of process.stdin) chunks.push(Buffer.from(chunk))
const input = Buffer.concat(chunks).toString("utf8")
const handoff = JSON.parse(input) as DreamHandoff
const opencode = createOpencodeClient({
  baseUrl: process.env.OPENCODE_SERVER_URL ?? "http://127.0.0.1:4096",
  directory: process.env.OPENCODE_DIRECTORY,
})
const nephesh = createNepheshRelease({
  mcpUrl: process.env.NEPHESH_MCP_URL ?? "http://127.0.0.1:61080/mcp",
})

try {
  const receipt = await consumeDreamHandoff(handoff, {
    client: opencode,
    release: nephesh.release,
    inspect: nephesh.inspect,
    phase: nephesh.phase,
    recall: nephesh.recall,
    nepheshToolPrefix: process.env.NEPHESH_TOOL_PREFIX ?? "nephesh",
    agent: process.env.OPENCODE_AGENT,
    model: process.env.OPENCODE_MODEL
      ? {
          providerID: process.env.OPENCODE_MODEL.split("/", 1)[0],
          modelID: process.env.OPENCODE_MODEL.split("/", 2)[1] ?? process.env.OPENCODE_MODEL,
        }
      : undefined,
  })
  process.stdout.write(`${JSON.stringify(receipt)}\n`)
  process.exitCode = receipt.status === "completed" ? 0 : 1
} finally {
  await nephesh.close()
}
