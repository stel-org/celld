import { preparedMessage } from "./javascript-message.js";

export default {
  fetch(request) {
    return Response.json(preparedMessage(request.url));
  },
};
