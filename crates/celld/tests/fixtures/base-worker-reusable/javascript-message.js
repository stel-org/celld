export function preparedMessage(url) {
  return { worker: "javascript", path: new URL(url).pathname };
}
