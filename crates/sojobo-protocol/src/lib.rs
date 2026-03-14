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
