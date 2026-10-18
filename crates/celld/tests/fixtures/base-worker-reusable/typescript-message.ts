export interface PreparedMessage {
  worker: string;
  path: string;
}

export function preparedMessage(url: string): PreparedMessage {
  return { worker: "typescript", path: new URL(url).pathname };
}
