export interface WorkerMessage {
  message: string;
  path: string;
}

export function createMessage(url: string): WorkerMessage {
  return {
    message: "Hello from Base Worker",
    path: new URL(url).pathname,
  };
}
