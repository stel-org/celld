import { unavailable } from "jsr:@std/bytes-does-not-exist@1.0.0";

export default {
  fetch(): Response {
    return Response.json({ unavailable });
  },
};
