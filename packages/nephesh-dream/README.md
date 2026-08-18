# OpenCode Nephesh Dream Consumer

This package is the OpenCode-side consumer for a Nephesh chosen-dream handoff.
It uses the official `@opencode-ai/sdk` workspace package and owns only the
disposable OpenCode session.

Nephesh remains responsible for identity, dream phases, provenance, deadlines,
artifacts, grounding, and release state. The consumer creates a session,
delivers the bounded packet, polls the SDK session status until idle, aborts on
deadline/cancellation, and deletes the session. It reports release through the
Nephesh MCP endpoint when configured.

The package deliberately does not vendor SDK code and does not provide an
OpenClaw adapter.

## CLI

The CLI reads one prepared `memory_dream_invoke` result as JSON on stdin:

```sh
OPENCODE_SERVER_URL=http://127.0.0.1:4096 \
NEPHESH_MCP_URL=http://127.0.0.1:61080/mcp \
bun run src/cli.ts < handoff.json
```

Optional environment variables are `OPENCODE_DIRECTORY`, `OPENCODE_AGENT`, and
`OPENCODE_MODEL` (`provider/model`).
