<!-- codex: Use git log to update this table. -->
<!-- codex: Include sessions near midnight with the prior day. -->
<!-- codex: Commits hours apart are separate sessions. -->

Derived from local git history.

| Date       | # Commits | Hours | Line Changes    |
| ---------- | --------: | ----: | --------------- |
| 2026-04-18 |        13 |   2.7 | +2,171 / -852   |
| 2026-04-16 |         4 |   0.3 | +934 / -31      |
| 2026-04-15 |        11 |   1.5 | +1,693 / -151   |
| 2026-04-14 |        11 |   2.3 | +3,250 / -1,129 |
| 2026-04-13 |         6 |   0.5 | +3,192 / -207   |

Current code size:

- `src/`: `7,848` lines
- `tests/`: `0` lines
- `scripts/`: `150` lines

## 2026-04-18 Notes

- Added the first simple in-process relay path with `MemoryRelay`, shared subscriptions, delta
  publish/fetch support, and a two-device end-to-end sync test.
- Tightened the engine's remote apply path so incoming deltas update the live `TreeIndex` and
  persisted tree metadata directly instead of rebuilding from disk after every batch.
- Standardized relay event handling around `RelayEvent::Delta(Delta)` and filled in the simple
  apply path for modify, create-directory, remove, and move operations.
- Simplified saved-state hydration by making `TreeIndex::hydrate(...)` the single path for
  rebuilding the in-memory index from persisted tree objects.
- Shifted restart recovery toward re-diffing from the last published snapshot instead of trying to
  preserve unpublished pending deltas across crashes.
- Expanded test coverage around remote delta application, saved tree hydration, and the first
  two-device sync flow through the runtime and watcher loop.
