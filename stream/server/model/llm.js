// 기술 블로그 예시: Mongoose 스타일 메서드를 흉내 낸 인메모리 모델.
const store = new Map(); // _id -> { _id, messages: [{ messageId, content }] } 형태

class ConversationModel {
  static async findOne(query) {
    if (!query || !query._id) return null;
    return store.get(query._id) ?? null;
  }

  static async updateOne(filter, update, options = {}) {
    const id = filter._id;
    const existing = store.get(id);
    if (!existing && !options.upsert) return { acknowledged: true, matchedCount: 0 };

    const doc =
      existing ??
      (() => {
        const created = { _id: id, messages: [] };
        store.set(id, created);
        return created;
      })();

    if (update.$setOnInsert && !existing) {
      Object.assign(doc, update.$setOnInsert);
    }
    if (update.$push && update.$push.messages) {
      doc.messages.push(update.$push.messages);
    }
    store.set(id, doc);
    return { acknowledged: true, matchedCount: 1 };
  }
}

module.exports = {
  ConversationModel,
};
