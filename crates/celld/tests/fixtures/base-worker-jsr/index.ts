import { concat } from "jsr:@std/bytes@1.0.6";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

export default {
  fetch(request: Request): Response {
    const message = decoder.decode(
      concat([encoder.encode("Hello from "), encoder.encode("JSR")]),
    );
    return Response.json({ message, path: new URL(request.url).pathname });
  },
};
