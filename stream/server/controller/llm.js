const { LLMService } = require("../service/llm");

const service = new LLMService();

/**
 * 1. Post 요청으로 keepalive chunk 형식으로 SSE와 같은 요청을 보낸다.
 * body로 conversationId를 보내고, server에서 conversation의 가장 마지막 messageId를 만들어준다.
 * client에 stream으로 conversationId와 messageId를 먼저 응답을 보내준다.
 * 이후 AI에게 사용자의 message를 보내고, ai에서 stream이 도착하면, 다시 client stream으로 emit한다.
 * ai 응답이 종료되면, 결과는 FakeMongoModel에 저장하고, client에게는 done 응답을 보내며 sse 연결을 close한다.
 */

/**
 * 2. Post요청으로 conversationId와 Message를 보낸다.
 * 즉시 AI에게 stream 요청을 보내고, client에는 MessageId를 response로 보낸다.
 * 
 * 이후 Client는 GET을 통해 SSE를 보내고, 거기에는 conversationId와 messageId를 쿼리로 포함시켜서 보낸다.
 * 
 */


const registerRoutes = (app) => {
};

module.exports = {
  registerRoutes,
  postStream,
  postStartWrite,
  getReadWindow,
  getReplayWithCursor,
  getReplayWithBuffer,
};
