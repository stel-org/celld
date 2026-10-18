import { concat } from "jsr:@std/bytes@99.0.0";

export default {
  fetch(): Response {
    return Response.json({ length: concat([]).byteLength });
  },
};
