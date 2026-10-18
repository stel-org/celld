// This fixed Durable Object exists solely to validate Base's B2.3 local
// execution boundary. The Base control socket derives its scope itself and
// cannot invoke arbitrary classes, methods, paths, or source code.
export class BaseDiagnostic {
  constructor(state) {
    this.state = state;
  }

  async fetch() {
    const executions = (await this.state.storage.get("executions")) ?? 0;
    const next = executions + 1;
    await this.state.storage.put("executions", next);
    return Response.json({ executed: true, executions: next });
  }
}

export default {
  async fetch() {
    return Response.json({ service: "base-diagnostic" });
  },
};
