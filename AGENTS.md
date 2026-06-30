# moni Codex Guidance

## Testing Contract

- Treat automated tests as the verification boundary for moni. If the test suite passes, the user should not need to manually test Discord, NATS, Codex streaming, voice transcription, or async coordination paths.
- Manual testing is an anti-pattern for validating behavior in this repository. Use it only as temporary debugging evidence, then encode the behavior in unit or integration tests before considering the work done.
- Maintain 100% coverage with meaningful unit and integration tests. A passing coverage run is the release signal; manual Discord checks are not a substitute for tests.
- Async synchronization, streaming, rate limiting, background tasks, and message update behavior are not fully protected by Rust's type system. Cover those paths with deterministic tests, fakes, time control, or local services as appropriate.
- Do not increase coverage by excluding files, functions, branches, or lines from coverage. Improve coverage by testing behavior or by refactoring code into testable units.
- Keep coverage expectations honest for both unit tests and integration tests. When behavior crosses process or service boundaries, prefer hermetic local tests over asking the user to verify it in Discord.
