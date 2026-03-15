//! WHATWG-compliant Server-Sent Events (SSE) parser.
//!
//! Implements the event stream parsing algorithm from the
//! [WHATWG HTML Living Standard §9.2.5–9.2.6][sse-spec], with UTF-8 decoding
//! per the [WHATWG Encoding Standard §6 and §8.1.1][encoding-spec].
//!
//! # Usage
//!
//! Feed raw bytes from an HTTP response body into [`SseParser::push`] as they
//! arrive. The parser handles UTF-8 decoding (including multi-byte sequences
//! split across chunk boundaries), BOM stripping, line splitting (LF/CR/CRLF),
//! field parsing, and event dispatch. Retrieve completed events with
//! [`SseParser::next_event`]. Call [`SseParser::finish`] when the stream ends.
//!
//! [sse-spec]: https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream
//! [encoding-spec]: https://encoding.spec.whatwg.org/#utf-8-decode

/// A parsed SSE event containing an event type and a data payload.
///
/// The `event` field defaults to `"message"` if the stream did not include an
/// `event:` field before the dispatching blank line. The `data` field contains
/// the concatenated values of all `data:` fields for this event, joined by
/// newlines (with the trailing newline stripped).
pub struct SseEvent {
    pub event: String,
    pub data: String,
}

/// Incremental, streaming SSE parser.
///
/// Implements the WHATWG HTML Living Standard §9.2.5–9.2.6 event stream
/// parsing algorithm. Designed for network use where bytes arrive in
/// arbitrarily-sized chunks.
///
/// # Decoding pipeline
///
/// ```text
/// raw bytes ──push()──► utf8_buf ──decode_utf8()──► buf ──process_lines()──► events
///                        │                          │
///                        │ incomplete UTF-8          │ incomplete line
///                        │ sequences retained        │ retained for
///                        │ for next push()           │ next push()
///                        ▼                           ▼
///                   BOM check                  field parsing
///                   (first 3 bytes)            + event dispatch
/// ```
///
/// # Spec references
///
/// - UTF-8 decoding: WHATWG Encoding §6 ("UTF-8 decode") — strip leading BOM,
///   then decode with "replacement" error mode (invalid bytes → U+FFFD).
/// - Line splitting: §9.2.6 — CRLF, LF, or CR each terminate a line.
/// - Field parsing: §9.2.6 — split at first colon, strip one leading space
///   from value. Recognized fields: `event`, `data`, `id`, `retry`.
/// - Event dispatch: §9.2.6 — blank line triggers dispatch. Empty data buffer
///   means no event. Trailing newline on data is stripped. Default event type
///   is `"message"`. Event type buffer resets after dispatch.
pub struct SseParser {
    /// Raw bytes not yet decoded to UTF-8. Between `push()` calls, this holds
    /// trailing bytes that might be an incomplete multi-byte UTF-8 sequence.
    /// These are prepended to the next chunk before decoding.
    utf8_buf: Vec<u8>,

    /// Decoded UTF-8 text awaiting line splitting. May contain a partial line
    /// at the end (no line terminator yet). Consumed line-by-line in
    /// `process_lines()`, with the remainder kept for the next `push()`.
    buf: String,

    /// The `event:` field value for the current event being assembled.
    /// Reset to empty after each event dispatch. If empty at dispatch time,
    /// the event type defaults to `"message"` per §9.2.6.
    event_type: String,

    /// Accumulated `data:` field values for the current event. Each `data:`
    /// line appends its value followed by a newline. On dispatch, the trailing
    /// newline is stripped. If empty at dispatch time, no event is emitted.
    data: String,

    /// Completed events ready to be retrieved via `next_event()`.
    events: Vec<SseEvent>,

    /// Whether the leading BOM check has been performed. Per WHATWG Encoding
    /// §6, the UTF-8 decode algorithm peeks the first three bytes and strips
    /// them if they are `0xEF 0xBB 0xBF`. This flag ensures we only check
    /// once, and that we wait for enough bytes before deciding.
    bom_checked: bool,
}

impl Default for SseParser {
    fn default() -> Self {
        Self::new()
    }
}

impl SseParser {
    pub fn new() -> Self {
        Self {
            utf8_buf: Vec::new(),
            buf: String::new(),
            event_type: String::new(),
            data: String::new(),
            events: Vec::new(),
            bom_checked: false,
        }
    }

    /// Push a chunk of raw bytes from the network into the parser.
    ///
    /// Bytes are appended to the internal UTF-8 buffer, then decoded
    /// incrementally. Incomplete multi-byte sequences at the end of the
    /// combined buffer are retained for the next call. Invalid byte sequences
    /// in the middle are replaced with U+FFFD (one replacement character per
    /// invalid sequence, per WHATWG Encoding §8.1.1).
    ///
    /// After decoding, complete lines are extracted and processed as SSE
    /// fields. Completed events become available via [`next_event`](Self::next_event).
    pub fn push(&mut self, chunk: &[u8]) {
        self.utf8_buf.extend_from_slice(chunk);

        // BOM handling per WHATWG Encoding §6 "UTF-8 decode":
        //
        // "Let buffer be the result of peeking three bytes from ioQueue,
        //  converted to a byte sequence. If buffer is 0xEF 0xBB 0xBF, then
        //  read three bytes from ioQueue."
        //
        // We wait until we have at least 3 bytes (or can rule out a BOM from
        // the bytes we have) before proceeding with decoding. If the first
        // byte isn't 0xEF, or the first two bytes aren't [0xEF, 0xBB], we
        // know immediately it's not a BOM and stop waiting.
        if !self.bom_checked && !self.utf8_buf.is_empty() {
            let bom: &[u8] = &[0xEF, 0xBB, 0xBF];
            if self.utf8_buf.len() >= 3 {
                if self.utf8_buf.starts_with(bom) {
                    self.utf8_buf.drain(..3);
                }
                self.bom_checked = true;
            } else if !bom.starts_with(&self.utf8_buf) {
                // Bytes so far can't be a BOM prefix — stop waiting.
                self.bom_checked = true;
            }
            // Otherwise we have [0xEF] or [0xEF, 0xBB] — could still be a
            // BOM. Wait for more bytes before decoding anything.
        }

        self.decode_utf8();
        self.process_lines();
    }

    /// Signal that the byte stream has ended.
    ///
    /// Any incomplete UTF-8 sequence remaining in the buffer is replaced with
    /// a single U+FFFD, per WHATWG Encoding §8.1.1 (the decoder's handler
    /// returns one `error` when it encounters end-of-queue with `bytes_needed
    /// != 0`; "replacement" error mode emits one U+FFFD per error).
    ///
    /// Any partially assembled event (no trailing blank line) is discarded,
    /// per WHATWG SSE §9.2.6: "any pending data must be discarded."
    pub fn finish(&mut self) {
        if !self.bom_checked {
            self.bom_checked = true;
        }

        if !self.utf8_buf.is_empty() {
            // Final decode: use Utf8Chunks on everything remaining. Unlike
            // decode_utf8(), we don't buffer the last chunk's invalid bytes —
            // no more data is coming. Every invalid sequence becomes U+FFFD.
            for chunk in self.utf8_buf.utf8_chunks() {
                self.buf.push_str(chunk.valid());
                if !chunk.invalid().is_empty() {
                    self.buf.push('\u{FFFD}');
                }
            }
            self.utf8_buf.clear();
            self.process_lines();
        }
        // Any remaining partial event in `data`/`event_type` is implicitly
        // discarded — we don't call dispatch() without a blank line.
    }

    /// Decode as much valid UTF-8 as possible from `utf8_buf` into `buf`.
    ///
    /// Uses [`[u8]::utf8_chunks()`] from the standard library to split the
    /// buffer into alternating valid-UTF-8 / invalid-bytes segments.
    ///
    /// For non-final chunks (where `peek()` sees another chunk after), invalid
    /// bytes are genuinely invalid — they are replaced with U+FFFD (one per
    /// `Utf8Chunk::invalid()` slice, matching WHATWG replacement granularity).
    ///
    /// For the final chunk, trailing invalid bytes are **not consumed** — they
    /// might be an incomplete multi-byte sequence that will be completed by the
    /// next `push()` call. They remain in `utf8_buf` for the next round. If
    /// the stream truly ends, `finish()` will handle them.
    fn decode_utf8(&mut self) {
        let mut consumed = 0;

        let mut chunks = self.utf8_buf.utf8_chunks().peekable();
        while let Some(chunk) = chunks.next() {
            self.buf.push_str(chunk.valid());
            consumed += chunk.valid().len();

            let invalid = chunk.invalid();
            if invalid.is_empty() {
                continue;
            }

            if chunks.peek().is_none() {
                // Last chunk's trailing invalid bytes — might be incomplete.
                // Leave them in utf8_buf for the next push() or finish().
                break;
            }

            // Not the last chunk — these bytes are genuinely invalid.
            // Replace with one U+FFFD per the Utf8Chunk contract:
            // "Lossy decoding would replace this sequence with U+FFFD."
            self.buf.push('\u{FFFD}');
            consumed += invalid.len();
        }

        self.utf8_buf.drain(..consumed);
    }

    /// Retrieve the next completed SSE event, if available.
    ///
    /// Returns `None` if no events are ready. Events become available after
    /// `push()` processes a blank line that terminates an event block.
    pub fn next_event(&mut self) -> Option<SseEvent> {
        if self.events.is_empty() {
            None
        } else {
            Some(self.events.remove(0))
        }
    }

    /// Extract and process complete lines from `buf`.
    ///
    /// Repeatedly splits `buf` at the first line terminator (LF, CR, or CRLF)
    /// and processes each line. Any remaining text without a line terminator
    /// stays in `buf` for the next `push()`.
    fn process_lines(&mut self) {
        loop {
            let Some((line, rest)) = split_line(&self.buf) else {
                break;
            };
            let line = line.to_owned();
            self.buf = rest.to_owned();
            self.process_line(&line);
        }
    }

    /// Process a single line per WHATWG SSE §9.2.6 field parsing rules.
    ///
    /// - Empty line → dispatch the event.
    /// - Line starting with `:` → comment, ignored.
    /// - Line containing `:` → split at first colon into field name and value.
    ///   If value starts with a single U+0020 SPACE, strip it.
    /// - Line without `:` → entire line is the field name, value is empty.
    ///
    /// Recognized field names:
    /// - `"event"` → set the event type buffer.
    /// - `"data"` → append value + LF to the data buffer.
    /// - `"id"` and `"retry"` → ignored (not needed for our use case).
    /// - Anything else → ignored.
    fn process_line(&mut self, line: &str) {
        if line.is_empty() {
            self.dispatch();
            return;
        }

        if line.starts_with(':') {
            return;
        }

        // Split at first colon per §9.2.6:
        // "Collect the characters on the line before the first U+003A COLON
        //  character (:), and let field be that string."
        // "Collect the characters on the line after the first U+003A COLON
        //  character (:), and let value be that string. If value starts with
        //  a U+0020 SPACE character, remove it from value."
        let (field, value) = match line.find(':') {
            Some(pos) => {
                let value = &line[pos + 1..];
                let value = value.strip_prefix(' ').unwrap_or(value);
                (&line[..pos], value)
            }
            // "Otherwise, the string is not empty but does not contain a
            //  U+003A COLON character (:) — Process the field using the whole
            //  line as the field name, and the empty string as the field value."
            None => (line, ""),
        };

        match field {
            "event" => {
                // "Set the event type buffer to the field value."
                self.event_type.clear();
                self.event_type.push_str(value);
            }
            "data" => {
                // "Append the field value to the data buffer, then append a
                //  single U+000A LINE FEED (LF) character to the data buffer."
                self.data.push_str(value);
                self.data.push('\n');
            }
            _ => {}
        }
    }

    /// Dispatch the current event per WHATWG SSE §9.2.6.
    ///
    /// "If the data buffer is an empty string, set the data buffer and the
    ///  event type buffer to the empty string and return."
    ///
    /// Otherwise: strip the trailing LF from the data buffer, use the event
    /// type buffer (or `"message"` if empty) as the event type, push the
    /// completed event, and reset both buffers.
    fn dispatch(&mut self) {
        if self.data.is_empty() {
            // §9.2.6: no data → reset buffers, no event dispatched.
            self.event_type.clear();
            return;
        }

        // "If the data buffer's last character is a U+000A LINE FEED (LF)
        //  character, then remove the last character from the data buffer."
        if self.data.ends_with('\n') {
            self.data.pop();
        }

        // "Initialize event's type attribute to message"
        // "If the event type buffer has a value other than the empty string,
        //  change the type of the newly created event to equal the value of
        //  the event type buffer."
        let event_type = if self.event_type.is_empty() {
            "message".to_owned()
        } else {
            std::mem::take(&mut self.event_type)
        };

        let data = std::mem::take(&mut self.data);
        self.events.push(SseEvent {
            event: event_type,
            data,
        });

        // "Set the data buffer and the event type buffer to the empty string."
        // (Already done by std::mem::take above, and clear() in the empty
        // data path.)
    }
}

/// Split `buf` at the first line terminator per WHATWG SSE §9.2.6.
///
/// Line endings are: CRLF (`\r\n`), LF (`\n`), or CR (`\r`).
///
/// Returns `Some((line_content, remaining))` if a complete line was found,
/// or `None` if the buffer contains no line terminator.
///
/// # Safety of byte-level indexing
///
/// We iterate `buf.bytes()` to find `\n` (0x0A) and `\r` (0x0D). These are
/// single-byte ASCII characters that can never appear as part of a multi-byte
/// UTF-8 sequence (continuation bytes are always 0x80–0xBF). Therefore the
/// byte index `i` is always a valid UTF-8 boundary, and `&buf[..i]` /
/// `&buf[i + 1..]` are safe `str` slices.
fn split_line(buf: &str) -> Option<(&str, &str)> {
    for (i, b) in buf.bytes().enumerate() {
        if b == b'\n' {
            return Some((&buf[..i], &buf[i + 1..]));
        }
        if b == b'\r' {
            let next = i + 1;
            // Check for CRLF pair — consume both characters.
            if buf.as_bytes().get(next) == Some(&b'\n') {
                return Some((&buf[..i], &buf[next + 1..]));
            }
            // Bare CR — just consume the CR.
            return Some((&buf[..i], &buf[next..]));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- Basic SSE parsing (§9.2.6) ---

    #[test]
    fn parse_simple_event() {
        let mut parser = SseParser::new();
        parser.push(b"data: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.event, "message");
        assert_eq!(event.data, "hello");
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn parse_named_event() {
        let mut parser = SseParser::new();
        parser.push(b"event: content_block_delta\ndata: {\"text\":\"hi\"}\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.event, "content_block_delta");
        assert_eq!(event.data, "{\"text\":\"hi\"}");
    }

    #[test]
    fn parse_multiline_data() {
        let mut parser = SseParser::new();
        parser.push(b"data: line1\ndata: line2\ndata: line3\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "line1\nline2\nline3");
    }

    #[test]
    fn skip_comments() {
        let mut parser = SseParser::new();
        parser.push(b": this is a comment\ndata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn multiple_events() {
        let mut parser = SseParser::new();
        parser.push(b"data: first\n\ndata: second\n\n");
        let e1 = parser.next_event().unwrap();
        let e2 = parser.next_event().unwrap();
        assert_eq!(e1.data, "first");
        assert_eq!(e2.data, "second");
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn incremental_push() {
        let mut parser = SseParser::new();
        parser.push(b"data: hel");
        assert!(parser.next_event().is_none());
        parser.push(b"lo\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn blank_line_with_no_data_is_ignored() {
        let mut parser = SseParser::new();
        parser.push(b"\n\ndata: real\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "real");
        assert!(parser.next_event().is_none());
    }

    // --- Line ending variants ---

    #[test]
    fn crlf_line_endings() {
        let mut parser = SseParser::new();
        parser.push(b"data: hello\r\n\r\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn cr_only_line_endings() {
        let mut parser = SseParser::new();
        parser.push(b"data: hello\r\r");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    // --- Field parsing edge cases ---

    #[test]
    fn no_space_after_colon() {
        let mut parser = SseParser::new();
        parser.push(b"data:nospace\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "nospace");
    }

    #[test]
    fn event_type_resets_after_dispatch() {
        let mut parser = SseParser::new();
        parser.push(b"event: custom\ndata: first\n\ndata: second\n\n");
        let e1 = parser.next_event().unwrap();
        let e2 = parser.next_event().unwrap();
        assert_eq!(e1.event, "custom");
        assert_eq!(e2.event, "message");
    }

    // --- WHATWG spec examples (§9.2.6) ---

    #[test]
    fn data_field_no_colon_fires_empty_event() {
        let mut parser = SseParser::new();
        parser.push(b"data\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "");
    }

    #[test]
    fn two_data_fields_no_colon_fires_newline() {
        let mut parser = SseParser::new();
        parser.push(b"data\ndata\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\n");
    }

    #[test]
    fn space_after_colon_is_optional() {
        let mut parser = SseParser::new();
        parser.push(b"data:test\n\ndata: test\n\n");
        let e1 = parser.next_event().unwrap();
        let e2 = parser.next_event().unwrap();
        assert_eq!(e1.data, e2.data);
        assert_eq!(e1.data, "test");
    }

    #[test]
    fn spec_example_stock_data() {
        let mut parser = SseParser::new();
        parser.push(b"data: YHOO\ndata: +2\ndata: 10\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "YHOO\n+2\n10");
    }

    #[test]
    fn only_first_space_after_colon_stripped() {
        let mut parser = SseParser::new();
        parser.push(b"data:  third event\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, " third event");
    }

    // --- BOM handling (WHATWG Encoding §6) ---

    #[test]
    fn bom_stripped_from_first_chunk() {
        let mut parser = SseParser::new();
        parser.push(b"\xef\xbb\xbfdata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn bom_not_stripped_from_later_chunks() {
        let mut parser = SseParser::new();
        parser.push(b"data: first\n\n");
        parser.next_event().unwrap();
        // BOM bytes after the first 3 stream bytes are not stripped — they
        // decode to U+FEFF which prefixes the field name, making it
        // "\u{FEFF}data" which doesn't match "data", so the line is ignored.
        parser.push(b"\xef\xbb\xbfdata: second\n\ndata: third\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "third");
    }

    #[test]
    fn bom_split_across_chunks() {
        let mut parser = SseParser::new();
        // BOM (0xEF 0xBB 0xBF) arrives in two chunks — parser waits for the
        // third byte before deciding.
        parser.push(b"\xef\xbb");
        parser.push(b"\xbfdata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    // --- UTF-8 chunk boundary handling ---

    #[test]
    fn multibyte_char_split_across_chunks() {
        let mut parser = SseParser::new();
        // é is U+00E9, encoded as 0xC3 0xA9 — split between the two bytes.
        parser.push(b"data: \xc3");
        assert!(parser.next_event().is_none());
        parser.push(b"\xa9\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "é");
    }

    #[test]
    fn three_byte_char_split_across_chunks() {
        let mut parser = SseParser::new();
        // € is U+20AC, encoded as 0xE2 0x82 0xAC — split after first byte.
        parser.push(b"data: \xe2");
        assert!(parser.next_event().is_none());
        parser.push(b"\x82\xac\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\u{20AC}");
    }

    #[test]
    fn four_byte_char_split_across_chunks() {
        let mut parser = SseParser::new();
        // U+1F600 (grinning face) encoded as 0xF0 0x9F 0x98 0x80 — split
        // after second byte.
        parser.push(b"data: \xf0\x9f");
        assert!(parser.next_event().is_none());
        parser.push(b"\x98\x80\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\u{1F600}");
    }

    // --- Invalid UTF-8 (WHATWG Encoding §8.1.1 replacement mode) ---

    #[test]
    fn invalid_utf8_replaced_with_fffd() {
        let mut parser = SseParser::new();
        // 0xFF is never a valid UTF-8 byte.
        parser.push(b"data: a\xffb\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "a\u{FFFD}b");
    }

    #[test]
    fn invalid_continuation_byte_replaced() {
        let mut parser = SseParser::new();
        // 0xC3 starts a 2-byte sequence, but 0x28 ('(') is not a valid
        // continuation byte (must be 0x80–0xBF). Per §8.1.1, the decoder
        // resets and restores 0x28 to the queue, emitting one U+FFFD for the
        // orphaned 0xC3. Then 0x28 is decoded as ASCII '('.
        parser.push(b"data: \xc3(ok\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\u{FFFD}(ok");
    }

    // --- End-of-stream handling ---

    #[test]
    fn finish_flushes_incomplete_utf8_as_one_replacement() {
        let mut parser = SseParser::new();
        // Push an incomplete 3-byte sequence (0xE2 0x82 — missing third byte).
        parser.push(b"data: x\xe2\x82");
        assert!(parser.next_event().is_none());
        // End of stream — per §8.1.1, incomplete sequence at end-of-queue
        // returns one error → one U+FFFD in replacement mode.
        parser.finish();
        // But the event itself is incomplete (no blank line), so it's
        // discarded per §9.2.6: "any pending data must be discarded."
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn finish_with_complete_event() {
        let mut parser = SseParser::new();
        parser.push(b"data: ok\n\n");
        parser.push(b"\xe2"); // incomplete trailing byte after event
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "ok");
        parser.finish();
        // The incomplete byte becomes U+FFFD but there's no event wrapper
        // (no blank line), so nothing is dispatched.
        assert!(parser.next_event().is_none());
    }

    // --- Ignored fields ---

    #[test]
    fn id_field_ignored_without_breaking() {
        let mut parser = SseParser::new();
        parser.push(b"id: 123\ndata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn retry_field_ignored_without_breaking() {
        let mut parser = SseParser::new();
        parser.push(b"retry: 5000\ndata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn unknown_field_ignored() {
        let mut parser = SseParser::new();
        parser.push(b"foo: bar\ndata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    // --- Event type edge cases ---

    #[test]
    fn empty_event_field_resets_to_message() {
        let mut parser = SseParser::new();
        // "event:" with empty value sets buffer to "", so dispatch uses "message"
        parser.push(b"event: custom\nevent:\ndata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.event, "message");
    }

    #[test]
    fn multiple_event_fields_last_wins() {
        let mut parser = SseParser::new();
        parser.push(b"event: first\nevent: second\ndata: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.event, "second");
    }

    // --- Data edge cases ---

    #[test]
    fn data_colon_no_value() {
        let mut parser = SseParser::new();
        // "data:" with nothing after colon → empty value + LF appended
        parser.push(b"data:\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "");
    }

    #[test]
    fn comment_between_data_fields() {
        let mut parser = SseParser::new();
        parser.push(b"data: a\n: comment\ndata: b\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "a\nb");
    }

    // --- Line ending variants ---

    #[test]
    fn mixed_line_endings() {
        let mut parser = SseParser::new();
        // LF, then CRLF, then CR as line endings in one stream
        parser.push(b"data: a\ndata: b\r\ndata: c\r\r");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "a\nb\nc");
    }

    // --- Empty / degenerate inputs ---

    #[test]
    fn empty_push() {
        let mut parser = SseParser::new();
        parser.push(b"");
        assert!(parser.next_event().is_none());
        parser.push(b"data: hello\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "hello");
    }

    #[test]
    fn empty_stream_finish() {
        let mut parser = SseParser::new();
        parser.finish();
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn finish_called_twice() {
        let mut parser = SseParser::new();
        parser.push(b"data: hello\n\n");
        parser.next_event().unwrap();
        parser.finish();
        parser.finish();
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn push_after_finish() {
        let mut parser = SseParser::new();
        parser.push(b"data: first\n\n");
        parser.finish();
        // Push after finish — parser should still work
        parser.push(b"data: second\n\n");
        let _ = parser.next_event(); // drain first if still there
    }

    #[test]
    fn events_accumulate_without_consumption() {
        let mut parser = SseParser::new();
        parser.push(b"data: a\n\ndata: b\n\ndata: c\n\n");
        // Don't consume — all three should be available
        let e1 = parser.next_event().unwrap();
        let e2 = parser.next_event().unwrap();
        let e3 = parser.next_event().unwrap();
        assert_eq!(e1.data, "a");
        assert_eq!(e2.data, "b");
        assert_eq!(e3.data, "c");
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn bom_only_stream() {
        let mut parser = SseParser::new();
        parser.push(b"\xef\xbb\xbf");
        parser.finish();
        assert!(parser.next_event().is_none());
    }

    // --- More UTF-8 edge cases ---

    #[test]
    fn first_chunk_single_bom_byte_then_finish() {
        let mut parser = SseParser::new();
        parser.push(b"\xef");
        parser.finish();
        // [0xEF] alone — not a BOM, not valid UTF-8 → U+FFFD
        // But no event structure, so nothing dispatched
        assert!(parser.next_event().is_none());
    }

    #[test]
    fn invalid_byte_before_line_ending() {
        let mut parser = SseParser::new();
        parser.push(b"data: x\xff\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "x\u{FFFD}");
    }

    #[test]
    fn four_byte_char_one_byte_per_push() {
        let mut parser = SseParser::new();
        // U+1F600 = 0xF0 0x9F 0x98 0x80, delivered one byte at a time
        parser.push(b"data: ");
        parser.push(b"\xf0");
        parser.push(b"\x9f");
        parser.push(b"\x98");
        parser.push(b"\x80");
        parser.push(b"\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\u{1F600}");
    }

    // --- WPT (Web Platform Tests) derived cases ---
    // From https://github.com/web-platform-tests/wpt/tree/master/eventsource

    /// WPT format-field-parsing.any.js
    ///
    /// Tests null chars in data/field names, case sensitivity ("Data" != "data"),
    /// only-first-space stripping, and various non-"data" field names.
    #[test]
    fn wpt_field_parsing() {
        let mut parser = SseParser::new();
        // Exact input from WPT (note: includes "data_5\n" and "data:3\r"):
        // data:\0\n data:  2\r Data:1\n data\0:2\n data:1\r \0data:4\n
        // da-ta:3\r data_5\n data:3\r data:\r\n  data:32\n data:4\n
        // + server appends \n (newline=none)
        parser.push(
            b"data:\0\ndata:  2\rData:1\ndata\0:2\ndata:1\r\0data:4\nda-ta:3\rdata_5\ndata:3\rdata:\r\n data:32\ndata:4\n\n",
        );
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\0\n 2\n1\n3\n\n4");
    }

    /// WPT format-newlines.any.js
    /// Mixed CRLF, LF, CR line endings.
    #[test]
    fn wpt_newlines() {
        let mut parser = SseParser::new();
        // data:test\r\n data\n data:test\r\n \r
        parser.push(b"data:test\r\ndata\ndata:test\r\n\r");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "test\n\ntest");
    }

    /// WPT format-bom.any.js
    /// First BOM stripped, second BOM corrupts field name.
    #[test]
    fn wpt_bom() {
        let mut parser = SseParser::new();
        // BOM + data:1\n\n + BOM + data:2\n\n + data:3
        parser.push(b"\xef\xbb\xbfdata:1\n\n\xef\xbb\xbfdata:2\n\ndata:3\n\n");
        let e1 = parser.next_event().unwrap();
        assert_eq!(e1.data, "1");
        // Second BOM prefixes "data:2" → "\u{FEFF}data" ≠ "data" → ignored
        // Blank line dispatches nothing (empty data buffer)
        let e2 = parser.next_event().unwrap();
        assert_eq!(e2.data, "3");
        assert!(parser.next_event().is_none());
    }

    /// WPT format-bom-2.any.js
    /// Double BOM at start — one stripped, second corrupts first data field.
    #[test]
    fn wpt_double_bom() {
        let mut parser = SseParser::new();
        // BOM BOM data:1\n\n data:2\n\n data:3\n\n
        parser.push(b"\xef\xbb\xbf\xef\xbb\xbfdata:1\n\ndata:2\n\ndata:3\n\n");
        // First BOM stripped, second BOM prefixes "data:1" → no event "1"
        let e1 = parser.next_event().unwrap();
        assert_eq!(e1.data, "2");
        let e2 = parser.next_event().unwrap();
        assert_eq!(e2.data, "3");
        assert!(parser.next_event().is_none());
    }

    /// WPT format-comments.any.js
    /// Comments with null chars and long strings are ignored.
    #[test]
    fn wpt_comments() {
        let mut parser = SseParser::new();
        let long = "x".repeat(2 * 1024);
        let input =
            format!("data:1\r:\0\n:\r\ndata:2\n:{long}\rdata:3\n:data:fail\r:{long}\ndata:4\n\n");
        parser.push(input.as_bytes());
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "1\n2\n3\n4");
    }

    /// WPT format-field-event-empty.any.js
    /// Empty "event:" field resets type to "message".
    #[test]
    fn wpt_empty_event_field() {
        let mut parser = SseParser::new();
        // "event: \ndata:data" — event field value is "" (space stripped)
        parser.push(b"event: \ndata:data\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.event, "message");
        assert_eq!(event.data, "data");
    }

    /// WPT format-null-character.any.js
    /// Null character is valid in data.
    #[test]
    fn wpt_null_in_data() {
        let mut parser = SseParser::new();
        parser.push(b"data:\0\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\0");
    }

    /// WPT format-leading-space.any.js
    /// Tab after colon is NOT stripped (only U+0020 SPACE is).
    #[test]
    fn wpt_leading_space() {
        let mut parser = SseParser::new();
        // data:\ttest\r data: \n data:test  (final \n\n added)
        parser.push(b"data:\ttest\rdata: \ndata:test\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "\ttest\n\ntest");
    }

    /// WPT format-field-unknown.any.js (exact input)
    /// "data:test\n data\ndata\nfoobar:xxx\njustsometext\n:thisisacommentyay\ndata:test"
    /// + default \n\n from message.py
    #[test]
    fn wpt_unknown_fields() {
        let mut parser = SseParser::new();
        parser.push(
            b"data:test\n data\ndata\nfoobar:xxx\njustsometext\n:thisisacommentyay\ndata:test\n\n",
        );
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "test\n\ntest");
    }

    /// WPT format-field-data.any.js (exact input)
    /// "data:\n\ndata\ndata\n\ndata:test" + default \n\n
    /// Three events: "", "\n", "test"
    #[test]
    fn wpt_field_data() {
        let mut parser = SseParser::new();
        parser.push(b"data:\n\ndata\ndata\n\ndata:test\n\n");
        let e1 = parser.next_event().unwrap();
        assert_eq!(e1.data, "");
        let e2 = parser.next_event().unwrap();
        assert_eq!(e2.data, "\n");
        let e3 = parser.next_event().unwrap();
        assert_eq!(e3.data, "test");
        assert!(parser.next_event().is_none());
    }

    /// WPT format-field-event.any.js (exact input)
    /// "event:test\ndata:x\n\ndata:x" + default \n\n
    /// First event has type "test", second has type "message"
    #[test]
    fn wpt_field_event() {
        let mut parser = SseParser::new();
        parser.push(b"event:test\ndata:x\n\ndata:x\n\n");
        let e1 = parser.next_event().unwrap();
        assert_eq!(e1.event, "test");
        assert_eq!(e1.data, "x");
        let e2 = parser.next_event().unwrap();
        assert_eq!(e2.event, "message");
        assert_eq!(e2.data, "x");
    }

    /// WPT format-field-retry-empty.any.js (exact input)
    /// "retry\ndata:test" + default \n\n
    /// retry with no value is ignored, event fires normally
    #[test]
    fn wpt_retry_empty() {
        let mut parser = SseParser::new();
        parser.push(b"retry\ndata:test\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "test");
    }

    /// Case sensitivity: "Data" is not "data".
    #[test]
    fn wpt_case_sensitive_field_names() {
        let mut parser = SseParser::new();
        parser.push(b"Data:no\ndata:yes\ndAta:no\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "yes");
    }

    /// Space-prefixed line: " data:32" has field name " data" != "data".
    #[test]
    fn wpt_space_prefixed_field_name() {
        let mut parser = SseParser::new();
        parser.push(b" data:ignored\ndata:kept\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "kept");
    }

    /// Null in field name: "data\0:value" has field name "data\0" != "data".
    #[test]
    fn wpt_null_in_field_name() {
        let mut parser = SseParser::new();
        parser.push(b"data\0:ignored\ndata:kept\n\n");
        let event = parser.next_event().unwrap();
        assert_eq!(event.data, "kept");
    }
}
