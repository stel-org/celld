import { missing } from "./does-not-exist.ts";

export default {
  fetch(): Response {
    return new Response(missing);
  },
};
