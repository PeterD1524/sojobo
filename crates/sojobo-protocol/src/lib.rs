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
