// 기술 블로그 예시: Mongoose 스타일 메서드를 흉내 낸 인메모리 모델.
type MessageRecord = {
  messageId: string;
  content: string;
  role: "agent";
  timestamp: string;
};

type ConversationRecord = {
  _id: string;
  messages: MessageRecord[];
};

type UpdateDoc = {
  $setOnInsert?: Partial<ConversationRecord>;
  $push?: {
    messages?: MessageRecord;
  };
};

type UpdateOptions = {
  upsert?: boolean;
};

const store = new Map<string, ConversationRecord>(); // _id -> { _id, messages: [{ messageId, content, role, timestamp }] } 형태

class ConversationModel {
  static async findOne(query?: { _id?: string } | null) {
    if (!query || !query._id) return null;
    return store.get(query._id) ?? null;
  }

  static async updateOne(
    filter: { _id: string },
    update: UpdateDoc,
    options: UpdateOptions = {}
  ) {
    const id = filter._id;
    const existing = store.get(id);
    if (!existing && !options.upsert)
      return { acknowledged: true, matchedCount: 0 };

    const doc =
      existing ??
      (() => {
        const created: ConversationRecord = { _id: id, messages: [] };
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

export { ConversationModel };
