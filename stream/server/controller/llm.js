const { LLMService } = require("../service/llm");

const service = new LLMService();

/**
 * 1. 간단한 One Pahse LLM Chat 구조
 * Post 요청으로 keepalive chunk 형식으로 SSE와 같은 요청을 보낸다.
 * body로 conversationId를 보내고, server에서 conversation의 가장 마지막 messageId를 만들어준다.
 * client에 stream으로 conversationId와 messageId를 먼저 응답을 보내준다.
 * 이후 AI에게 사용자의 message를 보내고, ai에서 stream이 도착하면, 다시 client stream으로 emit한다.
 * ai 응답이 종료되면, 결과는 FakeMongoModel에 저장하고, client에게는 done 응답을 보내며 sse 연결을 close한다.
 * 
 * // 확인하려는 목표
 * // Post 응답이 도중에 Web이 끊기면(새로고침이나 이전페이지로 가기 등) 사용자는 이후 메시지를 볼수 없음. 
 * // GET /conversation/:id를 통해서만이 확인할수있는, 즉 생성중에는 오히려 스트리밍되던 데이터가 없어서 과거로 롤백되는것처럼보이는 문제가 있음을 Web에서 보여야함.
 */

/**
 * 2. Write와 Read를 분리한 Two Phase LLM Chat 구조
 * Post요청으로 conversationId와 Message를 보낸다.
 * 즉시 AI에게 stream 요청을 보내고, client에는 MessageId를 response로 보낸다.
 * 이후 Map에 conversationId, messageId, chunks: [], done,  error, emitter를 저장한다. Key는 conversationId+messageId이다.
 * 
 * 이후 Client는 GET을 통해 SSE를 보내고, 거기에는 conversationId와 messageId를 쿼리로 포함시켜서 보낸다.
 * 
 * Post 세션에서 AI응답을 stream으로 받으면 Map에 chunk를 추가하고, emitter를 통해 이벤트를 방출한다.
 * 
 * Get 세션에서는 conversationId와 messageId를 쿼리로보내고, 그를 통해 emmiter를 가져온다.
 * 이후 emmiter가 보내는 chunk 발행 및 done 이벤트를 sub한다.
 * sub한 데이터를 sse stream을 통해 client에게 스트리밍해준다. done되면 FakeMongoModel에 저장하고 sse연결을 close한다.
 * 
 * 
 * 
 * // 확인하려는 목표
 * // GET sse가 도중에 끊겨도, 다시 재연결하면 동일한 message에 대해 즉시 stream을 다시 받을수 있음을 통해 stream 읽기 안정성 확보
 * // 첫번째 SSE와 두번째 SSE 사이 재연결 시간 사이에 chunk는 받지모하는 문제가 있음을 UI에서 보여야함.
 * 
 */

/**
 * 3. Cursor와 Replay가 추가된 Chat Stream 구조
 * (2.)와 동일한 구조에 SSE 동작에 Cursor(chunk seq)를 추가한다. seq기준으로 가장 최근까지의 chunks를 전부 전달한다.
 * client에게 해당 chunks를 하나의 chunk로 전달하고, 이후 emit 받는 데이터를 client에게 stream 보낸다.
 * 
 * // 확인하려는 목표
 * // SSE가 중간합류하더라도 연결되기 이전까지의 데이터를 놓치는 것 없이 받을 수 있음을 보일수있다.
 * // 이전까지 replay데이터와, emit 리스닝 사이에 이벤트가 발행되는경우, 그 사이에 이벤트를 누락할 수 있는 문제를 보여야한다.
 */

/**
 * 4. Buffer가 추가된 Chat Stream 구조
 * (3.)와 동일한 구조에 Buffer를 추가한다.
 * 이벤트 emit을 먼저받고, buffer에 쌓는다. 이후 chuks를 전부 보내고, buffer를 우선적으로 처리하고,
 *  buffer가 비어야 live chunk를 client에게 스트리밍한다.
 * 
 * // 확인하려는 목표
 * // replay데이터와 emit 리스닝 사이에 발생되는 이벤트도 놓치지않고 전부 받을수있다.
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
