import { preparedMessage } from "./typescript-message.ts";

export default {
  fetch(request: Request): Response {
    return Response.json(preparedMessage(request.url));
  },
};
