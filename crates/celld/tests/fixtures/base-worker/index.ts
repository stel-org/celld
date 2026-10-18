import { createMessage } from "./message.ts";

export default {
  fetch(request: Request): Response {
    return Response.json(createMessage(request.url));
  },
};
