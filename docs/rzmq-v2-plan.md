# Plan: retrofit ZMTP/2.0 downgrade into rzmq

Local working copy of rzmq lives at `../rzmq/` (untracked sibling of
fauxdin, clone of `excsn/rzmq` at v0.5.15). This document is fauxdin's
plan for upgrading that dependency so it can replace the synchronous
`zmq` crate currently mandated by the
[`zmq-crate-constraint`](../.claude/memory/zmq-crate-constraint.md) memory.

## Motivation

fauxdin's sink/source pair speaks to Eiger detectors and to whatever
downstream consumers were originally configured to talk to ODIN. Real
Eiger detectors advertise ZMTP/2.0 on the wire (libzmq 3.x era). As of
0.5.15, rzmq is **strictly ZMTP/3.x-only**: it sends a 64-byte v3
greeting eagerly on connect and rejects peers with `major < 3` at the
greeting decode stage. Against a v2-only PUSH peer, rzmq falls into an
indefinite ~1 Hz reconnect loop and the application's `recv()` never
resolves.

The same is true of omq.rs (the other pure-Rust async ZMQ
implementation), with the added pathology that it has no reconnect
backoff at all — ~7,300 reconnect attempts per second when negotiation
fails. See [evidence](#evidence-collected-pre-plan).

Therefore: there is no off-the-shelf async-native ZMQ Rust crate that
can talk to fauxdin's v2-only detectors today. The choices are
(a) keep `zmq = "0.10"` + blocking threads as the sink does today, or
(b) teach rzmq the v2 downgrade path. This plan is (b).

## Evidence collected pre-plan

Captured during the test session that preceded this document; full
session transcript at
`~/.claude/projects/-Users-nickd-dials-fauxdin/879a8b15-378b-4928-89f0-6c0deaa58de6.jsonl`.

Tools written and committed under `contrib/`:

- **`megamind.py`** — pure-stdlib raw ZMTP server. Started life as v1.0;
  commit `8f1e928` switched it to v2.0 (signature + revision=1 +
  socket-type=PUSH + empty identity frame; v2-framed payloads with the
  LONG flag for bodies >255 bytes). Used to confirm rzmq fails the
  same way against v2 and v1 — neither version negotiates.
- **`megaminion.py`** — symmetric ZMTP version probe. Default "drag-feet"
  probe (connect, read, don't write) catches the peer's eagerly-written
  greeting; `--probe-all` opens four connections (silent + send-v1 +
  send-v2 + send-v3) and reports what version each elicits.
- **`pyzmq_push.py`** — minimal libzmq PUSH server (control peer for
  cross-impl checks).

Findings:

| | libzmq 4.x | rzmq 0.5.15 | omq.rs 0.10.0 |
|---|---|---|---|
| Initial bytes written on accept | 10 (signature only, then waits) | 64 (full v3 greeting) | 64 (full v3 greeting) |
| Advertised version | ZMTP/3.1 (minor=1) | ZMTP/3.0 (minor=0) | ZMTP/3.1 (minor=1) |
| Downgrades to v2.0? | Yes (when peer revision=1) | **No** | **No** |
| Downgrades to v1.0? | No (dropped in 4.x) | No | No |
| Frames accepted at TCP before close | (n/a — handshakes) | 2 | 0 |
| Reconnect backoff against v2/v1 peer | (n/a) | ~1 Hz | **none, ~7300 Hz** |

omq.rs codec confirms the gate explicitly in
`omq-proto/src/proto/greeting.rs:31-34`:

```rust
pub const ZMTP_MAJOR: u8 = 3;
pub const ZMTP_MINOR: u8 = 1;
// …
if major < 3 { return Err(Error::UnsupportedZmtpVersion { major, minor: 0 }); }
```

rzmq's equivalent is `core/src/protocol/zmtp/greeting.rs:11-12,99` and
`core/src/sessionx/protocol_handler/handshake.rs:215`.

## Goal

Make `rzmq::SocketType::Pull` (connect side) speak ZMTP/2.0 when the
peer is v2-only, so the fauxdin source can replace its blocking
`zmq`-based PULL with rzmq. The PUSH-bind side and the io_uring backend
are stretch goals scoped out below.

## ZMTP/2.0 vs ZMTP/3.x — what's actually different on the wire

Three diffs:

1. **Greeting is 12 bytes, not 64.**
   - v2: `\xff <8 zero> \x7f <revision=0x01> <socket-type>`
   - v3: same first 11 bytes but `<major=0x03>` at byte 10, plus a 53-byte
     tail (`minor + mechanism[20] + as-server + filler[31]`).
2. **No security handshake, no READY command.** v2 has no mechanism
   negotiation (NULL implicit) and no Properties dictionary. The
   socket-type *byte* in the greeting replaces the READY `Socket-Type`
   property. After the greeting, both sides send one identity *frame*
   (empty for anonymous), then data frames.
3. **Data-frame framing is already compatible.** Both v2 and v3 are
   `<flags><length><body>` with bit 0=MORE, bit 1=LONG. v3 adds bit
   2=COMMAND for command frames; v2 has no commands. rzmq's current
   `ZmtpCodec` produces v2-compatible bytes for any plain data frame.

The **only** structural change required is in the greeting/handshake.
Codec, framer and message-pipe machinery can stay.

Libzmq's actual wire behaviour worth knowing (observed via
`megaminion`):

- libzmq writes only the 10-byte signature first, then waits to see
  the peer's revision before writing bytes 11+ (so it can downgrade).
- Even when downgrading, libzmq's byte 10 is `0x03` (major=3) — it's
  pre-decided and written before the peer is read. This is the
  "libzmq quirk" that `megaminion`'s classifier reasons about.
- rzmq writes all 64 bytes immediately on accept. **This is the
  blocker for negotiation**: by the time we know the peer's
  revision, we've already sent 54 bytes the v2 peer will mis-parse
  as garbage frames.

## Where rzmq is hardcoded to v3

| File | Lines | What's there |
|---|---|---|
| `core/src/protocol/zmtp/greeting.rs` | 11-12 | `GREETING_VERSION_MAJOR_BYTE = 0x03`, `…_MINOR_BYTE = 0x00` |
| `core/src/protocol/zmtp/greeting.rs` | 32-58 | `ZmtpGreeting::encode` writes a fixed 64-byte buffer |
| `core/src/protocol/zmtp/greeting.rs` | 99 | `decode` errors on `major != 0x03` |
| `core/src/sessionx/protocol_handler/handshake.rs` | 145-183 | `send_greeting_impl` does one `write_all(64 bytes)` then transitions |
| `core/src/sessionx/protocol_handler/handshake.rs` | 215 | `if pg.version.0 < 3 { Err(ProtocolViolation) }` |
| `core/src/sessionx/types.rs` | 41-54 | `HandshakeSubPhaseX` hard-codes `SecurityHandshake → ReadyExchange → …` |
| `core/src/io_uring_backend/zmtp_handler.rs` | 280-382 | Parallel handshake state machine for io_uring; same hardcoding |

## Phased plan

### Phase 1 — protocol primitives (no behaviour change)

1. In `greeting.rs`, introduce a `NegotiatedVersion { V2, V3 { minor: u8 } }`
   enum.
2. Add `peek_revision(buf: &[u8; 11]) -> Result<u8>` that validates
   signature bytes 0-9 and returns byte 10.
3. Add `ZmtpV2Greeting { socket_type: u8 }` with `encode` (12 bytes)
   and a partial-decode entry point that consumes revision +
   socket-type given the first 10 bytes are already in hand.
4. Add `socket_type_code(name: &str) -> Option<u8>` ("PULL"→7,
   "PUSH"→8, etc.) — inverse of what rzmq currently puts in the
   READY `Socket-Type` property.
5. Keep `ZmtpGreeting` as the v3 type, callers unchanged.

### Phase 2 — staged greeting send (the core change)

6. Rewrite `send_greeting_impl` as a two-stage send:
   - **Stage A** — write only the 10-byte signature; transition to a
     new state `WaitingForPeerRevision`.
   - **Stage B** (entered after reading peer's bytes 0-10) — inspect
     peer's byte 10:
     - `0x03+` → write our v3 tail (54 bytes) and continue into the
       existing `WaitingForGreeting → SecurityHandshake → …` path.
     - `0x01` → write our v2 tail (2 bytes: `0x01 <socket_type_code>`)
       and jump to a new `V2IdentityExchange` state.
7. Add `WaitingForPeerRevision` and `V2IdentityExchange` to
   `HandshakeSubPhaseX`.

This matches libzmq's on-wire behaviour exactly (10-then-wait), which
is the only way to negotiate without wasting bytes the v2 peer will
mis-parse.

### Phase 3 — v2 path through the handshake

8. In `V2IdentityExchange`:
   - Send a v2-framed empty identity frame (just `0x00 0x00`).
   - Read peer's identity frame (also expected empty for anonymous).
   - Validate peer's socket-type byte is compatible (PULL↔PUSH, etc.).
   - Transition `→ Done`. Set `handler.negotiated_version = V2`.
9. v3 path is unchanged.

### Phase 4 — downstream awareness

10. `ZmtpProtocolHandlerX` gains a
    `negotiated_version: Option<NegotiatedVersion>` field.
11. In v2 sessions, codec asserts no command frames are encoded
    (`debug_assert!(!flags.contains(COMMAND))`), and rejects inbound
    COMMAND-flagged frames as a protocol violation.
12. Heartbeats: rzmq's PING/PONG path uses commands. In v2 sessions,
    disable it — rely on TCP keepalive.

### Phase 5 — io_uring parity (deferred)

13. Replicate Phases 2-4 in `io_uring_backend/zmtp_handler.rs`. Not
    needed for fauxdin's macOS dev loop. Worth a follow-up PR.

### Phase 6 — tests

#### 6a. In-process v2 PUSH harness — **DONE**

`rzmq/core/tests/v2_push_server.rs`. Pure raw-TCP (no rzmq, no libzmq,
no codec). Public API:

- `V2PushServer::spawn(cfg)` / `endpoint()` / `finish()`
- `V2PushServerConfig` — frame_count, frame_parts, frame_size, fill,
  send_socket_type, send_revision, hold_after_send, abort_after,
  phase_timeout
- `FillPattern` — `ByteN` (per-part global index) and `Counter { start }`
  (8-byte LE counter prefix)
- `AbortPoint` — `PostSignature`, `PostGreeting`, `PostIdentity`,
  `AfterNMessages(n)`, `AfterNParts(n)`
- `V2ServerOutcome` — messages_sent, parts_sent, peer_revision,
  peer_byte11, peer_identity, **peer_raw_bytes** (full transcript)

Key properties:

- `set_nodelay(true)` on the accepted socket so the wire-level
  sequencing the test asserts on isn't muddied by Nagle.
- Listener is `drop`ped immediately after `accept()` — a SUT in a
  reconnect loop cannot open a second connection mid-test.
- Hold + drain run concurrently via `tokio::select!`, so the full
  peer-side byte transcript is captured even when the peer sends
  unexpected trailing bytes.
- `peer_raw_bytes` is the canonical capture. `peer_revision` and
  `peer_byte11` are convenience accessors at fixed offsets.

Compiles cleanly against rzmq workspace; no new dependencies (only
`anyhow`, already in dev-deps).

#### 6b. Happy-path test (next concrete deliverable)

`core/tests/v2_downgrade.rs`. Spawn the harness with defaults, connect
rzmq PULL, receive 256 frames, assert:

- Wire level: `peer_revision == 0x01` (rzmq downgraded), `peer_byte11 == 0x07` (PULL).
- Payload level: 256 frames, each 512 bytes, frame *i* is `[i; 512]`.

This test fails on current rzmq with a clear "didn't downgrade" diff
(byte 10 is `0x03`, full v3 greeting visible in `peer_raw_bytes`). The
test goes green after Phase 3.

#### 6c. Negative-path tests

`core/tests/v2_negative.rs`. For each `AbortPoint`, assert:

- rzmq returns an error from `recv()` within the phase timeout
- No reconnect storm (test sees only one connection attempt)
- Where applicable, `peer_raw_bytes` shows rzmq advertising v2

Plus: socket-type mismatch (`send_socket_type: PUB` against rzmq PULL)
errors cleanly; bad revision (`send_revision: 0x02`) is rejected
without retry.

#### 6d. v3 regression guard

`core/tests/v3_still_works.rs`. rzmq PUSH bind ↔ rzmq PULL connect.
Both should negotiate v3 (each sees `peer_revision == 0x03` on the
other side via a v3-version of the harness, or simply via tracing
assertions). All existing `push_pull.rs` tests must still pass.

#### 6e. Cross-impl tier (optional)

`interop/` behind a `cross-impl` feature flag — pyzmq PUSH ↔ rzmq PULL
(v3 path); skip in CI without Python.

## Multipart and capture, by design

The harness emits multipart messages (`frame_parts > 1` sets MORE on
all but the last part). This is essential because fauxdin's real
workload — Eiger image frames — is multipart (header + image). A test
that only exercises single-part messages cannot catch
"rzmq surfaced 2 messages where it should have surfaced 1 multipart".

The `AbortPoint::AfterNParts(n)` variant tests partial-multipart
robustness: tear down after part 1 of a 3-part message and assert
rzmq does **not** deliver a half message to the application.

`peer_raw_bytes` captures everything the peer ever wrote. This means
tests can assert negative properties — e.g., "no COMMAND-flagged frame
appeared on the wire in a v2 session" — by inspecting the transcript.

## Execution order

| # | Step | Acceptance signal |
|---|---|---|
| 1 | Harness ✅ | Compiles |
| 2 | Happy-path test (6b) | Fails meaningfully on current rzmq: `peer_revision == 0x03` |
| 3 | Phase 1 (primitives) | Unit tests for v2 encode/decode round trips |
| 4 | Phase 2 (staged send) | Test from #2 reports `peer_revision == 0x01` |
| 5 | Phase 3 (v2 path) | Test from #2 passes end-to-end |
| 6 | Phase 4 (downstream) | Multipart + counter-fill variants pass |
| 7 | Negative tests (6c) | All `AbortPoint`s produce clean errors |
| 8 | v3 regression (6d) | Existing rzmq tests still green |
| 9 | Cross-impl (6e) | *optional* |
| 10 | Phase 5 (io_uring) | *deferred PR* |

## Change-surface estimate

| Area | Lines |
|---|---|
| `greeting.rs` | ~150 |
| `handshake.rs` (staged send + v2 path) | ~150 |
| `types.rs` (+2 variants) | ~5 |
| Codec + heartbeat (v2 guards) | ~15 |
| **Total feature work** | **~325** |
| Test harness | done (~300) |
| Test files (6b/6c/6d) | ~250 |

Single reviewable PR; split Phase 1 (primitives) from Phases 2-3
(behaviour change) if upstreaming.

## Out of scope

- **ZMTP/1.0.** Different framing entirely (length-before-flags;
  length includes flags). libzmq 4.x dropped v1; real v1-only peers
  are rare. Separate work if needed.
- **PUSH-bind side speaking v2.** Symmetric server-side flow — same
  shape as client downgrade. Add when a v2-only PULL client needs to
  talk to rzmq's bind side.
- **Reconnect backoff.** Independent issue. Even after v2 works,
  mismatched peers should still back off.
- **Non-anonymous identities for v2** (REQ/REP/ROUTER/DEALER). MVP is
  PULL/PUSH; always anonymous.

## Open questions

- **`allow_zmtp2: bool` connection option, default on?** Recommendation:
  yes — quiet upgrade for users who don't care about the wire, opt-out
  for strict v3-only deployments.
- **README update.** rzmq's README claims "Implements core aspects of
  ZMTP/3.1". Adjust to mention v2 fallback in the same PR.
- **Upstreaming.** This is forked work in a local clone of
  `excsn/rzmq`. If accepted upstream, split into two PRs: primitives
  first (Phase 1), then the staged-send change (Phase 2-3).

## Memory implications

If this lands and tests confirm the v2 path works against a real
Eiger detector, the `zmq-crate-constraint` memory should be updated
(not deleted) — the constraint *was* real at rzmq 0.5.15 and the
update should record both the constraint and its resolution. This
serves future-Claude when revisiting the choice.
