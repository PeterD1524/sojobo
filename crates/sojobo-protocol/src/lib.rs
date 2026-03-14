/// Message role in the conversation.
pub enum Role {
    User,
    Assistant,
}

/// Reason the assistant stopped generating.
pub enum StopReason {
    EndTurn,
    ToolUse,
    MaxTokens,
    StopSequence,
}

/// A content block within a message.
pub enum ContentBlock {
    Text {
        text: String,
    },
    ToolUse {
        id: String,
        name: String,
        input: String,
    },
    ToolResult {
        tool_use_id: String,
        content: String,
        is_error: bool,
    },
    Thinking {
        thinking: String,
        signature: String,
    },
}

/// A message in the conversation.
pub struct Message {
    pub role: Role,
    pub content: Vec<ContentBlock>,
}

/// An incremental delta received during streaming.
pub enum Delta {
    TextDelta { text: String },
    InputJsonDelta { partial_json: String },
    ThinkingDelta { thinking: String },
    SignatureDelta { signature: String },
}

/// An SSE event from the streaming API.
pub enum StreamEvent {
    MessageStart,
    ContentBlockStart { index: usize, block: ContentBlock },
    ContentBlockDelta { index: usize, delta: Delta },
    ContentBlockStop { index: usize },
    MessageDelta { stop_reason: StopReason },
    MessageStop,
}
