# ironbird

`ironbird` (short alias `ib`) is a terminal-based integrated/system test tool for
cFS flight software vehicles, with a CLI front-end (headless, scripted operation)
and a TUI front-end. It sends commands and receives, decodes, records, filters, and
replays telemetry across multiple aliased hardware targets.

- Operating context: users SSH into a server attached to flight hardware (serial,
  UDP/IP; a 1553 PCIe interface later). No GUI is available, and the tool must run
  with no privileges beyond the user's own.
- `ccdd/` holds the source-of-truth CSV command/telemetry definitions per target;
  JSON and FSW C headers are generated from them. Never hand-edit generated output.
- Packets are CCSDS space packets with project-specific header extensions; header
  layout must stay user-definable, not hard-coded.
- `docs/plan.md` is the staged design; implement one stage at a time against it.
- Branching: one short-lived branch per piece of work; after completion and review,
  merge it into `main` and delete the branch (local and remote). No long-lived
  per-stage branches.
- Before merging any PR, run an antagonistic review of the PR's full contents
  against `main`: independent reviewers (not the authors) actively try to break it
  with concrete failing inputs, and every finding is either fixed or explicitly
  deferred in `docs/plan.md` before the merge.
- All input is untrusted (wire packets, serial, recordings, imported CCDD/config/
  header files). Follow "Untrusted input and malformed data" in `docs/plan.md`:
  validate every length before unpacking, parse only through the single decode
  boundary, never `pickle`/`eval` data, bound every cache, sanitize strings before
  display or export, and add a malformed-input test (and corpus entry) for every
  parser change or bug.
- Error reporting is a first-class, pervasive feature, not an afterthought:
    - Every step that can fail (config load, CCDD parse, transport open/read/write,
      encode/decode, recording I/O, export) has a defined path to the user and to
      the log. No failure is silent.
    - The user-facing message is succinct: what failed and what to do, with a
      stable error code. Full detail (cause chain, raw bytes, context) sits one
      step away: a details pane/modal in the TUI, `--verbose` or the log in the CLI.
    - Guard against alert fatigue: long or frequent messages become noise users
      learn to ignore. Keep messages to one line, coalesce repeats (e.g. "decode
      failed for MID 0x0881 (x37 in 10 s)") instead of re-alerting, rate-limit
      by error code, and surface only what the user can act on. Everything still
      goes to the ledger in full.
    - The log is an action ledger, not just an error log: timestamped (UTC, ISO
      8601, sub-millisecond) records of user actions, commands sent, state changes,
      and errors, so the exact sequence leading to a failure can be traced later.

# General Guidelines

- Never use the em dash '—'; use plain dash '-' instead.

- When writing commits, NEVER auto-add your agent name as a co-author, nor specify
  a remote session name like "Claude-Session".

- When making technical decisions, do not give much weight to human-based development
  costs: development here is done by an agentic system of humans and AI agents.
  Prioritize quality, simplicity, robustness, scalability, and long-term maintainability.

- GUI and TUI work of any kind, from a new view to a styling tweak, follows the
  `gui-principles` skill; load it before starting. For the TUI, apply its
  principles translated to the terminal (panes, tables, modals, keyboard paths,
  color-as-state); browser-only mechanics (CSS, `scrollIntoView`) do not apply.

# Coding Guidelines

- When writing code involving data or variables, ALWAYS prioritize const-correctness
  and relevant read-only semantics, as well as data-encapsulation and least-privilege
  principles to help ensure data correctness and privacy.

- When writing code comments, **DO NOT** mention the way code was written or behaved
  before - simply comment on how the code is _right now_. Code diffing with version
  control is the only tool one needs to use for historical comparisons, not code
  comments.

- Employ assertive programming whenever possible - that is, use `assert` statements
  in code, not just in test files. This is to assert a certain state of the program
  or context prior to proceeding with logic. Do **NOT** assert on states or inputs
  for which the program has no guarantee or control over (e.g., user input, external
  environmental state, etc.). **DO** assert on conditions that the program _does_
  have control over (e.g., post-processed input). The conventional `assert` has
  _no_ computational cost for release builds, and a minimal cost for development
  builds, while allowing us to ensure bad assumptions are surfaced early.

- In C++ (or any language with templates/generics compiled per-instantiation),
  header-only template code should get a dedicated, dev/CI-only static-analysis
  pass that explicitly instantiates the template against a small, deliberately
  chosen set of representative types (e.g., `template class Foo<int>;`), rather
  than relying only on whatever types happen to get used elsewhere. Ordinary use
  only compiles and checks the members actually called, so uncalled methods can
  go uncompiled and unchecked indefinitely. Keep this instantiation file separate
  from production/shipped code (it forces compilation of every member for that
  type, which is too strict a constraint to bake into a general-purpose generic
  component), and choose the type set to match what the API actually commits to
  supporting.

- When using C++, default to C++23 unless the project-specific instruction file or
  pre-existing project setup uses a different C++ version.

    - C++23 allows for the ergonomic `std::expected` type for API calls, which frees
      implementations from excessive exception handling overhead.

- Make good, heavy use of compile-time assertions (e.g., `static_assert()` in C++
  and C23, `const_assert!` in Rust, `comptime` + `@compileError()` in Zig,
  `type Assert<T extends true> = T` for type-level assertions in TypeScript, etc.)
  when the programming language supports compile-time evaluations. This helps us
  catch bugs at compile-time (cheap) rather than at run-time (expensive).

- As a general, important policy, ensure that errors surface up to the user in a
  clear, actionable way, rather than silently discarding them somewhere along the
  error-reporting chain.

- Handle off-nominal, invalid cases intentionally and up-front, rather than waiting
  for a user to find out. Ensure the previous error surfacing policy is abided by.

- A blocked action is a decision, not an obstacle. If a command is rejected or
  denied for any reason (sandbox, permission rule, exec policy, approval), do
  **not** retry it in another form or achieve the same effect another way
  (another tool, interpreter, flag, or path), even if the rejection suggests
  "a safer approach". Stop that line of work and report the exact command and
  rejection message.

- Always commit and push completed work to its worktree's own branch,
  unprompted - do not wait to be asked. This covers pushing a branch for
  review only: merging or fast-forwarding it into a trunk branch (e.g.
  `main`) still needs the user's explicit go-ahead, unless a project's own
  instruction file says otherwise. It is **OKAY** for intermediate commits of a
  development branch or pull/merge request to fail in CI, because later
  commits will certainly cover that prior to a merge into trunk.

- Commit often and commit regularly. Favor commits that are relatively small,
  atomic, self-container, and focused. With that said, if you find yourself in
  a situation with a lot of unstaged changes, don't worry about splitting it up,
  since that would be significant effort for little gain; make a single commit
  with a good description, and move on. Do **NOT** try to juggle staging part of
  a file, or re-playing changes over multiple commits _just_ to support the general
  recommended commit policy. This recommended commit policy is more of a as-you-go
  policy rather than a go-back-and-fix policy.

- When building/compiling code, or running static analyses, parallelize as much
  as possible. Use every core the machine has (check `nproc`), so don't be
  conservative. For example, `make` and `cmake`
  support `-j` or `--parallel` to parallelize as many threads as the platform supports.

- Unless intentionally done **carefully** for performance reasons, favor default-initializing
  all objects to a reasonable value rather than leaving for chance partial initialization
  or a completely uninitialized object carrying garbage.

- In C++, don't omit the `struct` qualifier when declaring a variable of its type,
  even if it's not necessary. This is more informative to the reader, and forces
  the writer to recall the subtle difference between `struct`s and `class`s.

- Instead of long if-if or if-else chains against a particular value, favor switch-case
  statements or map constructs.

# Testing Guidelines

- When working a bug, first reproduce it in an E2E regression test; where that
  is not feasible, say so and why. The test captures the bug's context and
  gives before/after confirmation of the fix.

- When writing unit tests, especially early on in a project (e.g., MVP phase,
  exploratory or experimental sessions, etc.), do not overdo it. These early stages
  will undergo many changes, and test suites should not rigidify what is still
  being molded.

- When writing unit, integration, or E2E tests for the front-end UI, do **NOT**
  assert on specific text or arrangements. These are all subject to change at any
  time as needed, and tests against this only serve as nuissance obstacles, not
  helpful, protective gates.

- With the above exceptions in mind, generally unit test realistic inputs and
  and corner case states at the function-, module-, and integrated-level, to
  ensure good, behavioral coverage in a world of unexpected user input.

- A project's CI should generally own a full test suite run unless a local full
  test suite run is cheap. You typical test run should focus on the changes you've
  worked on. A full test suite often covers irrelevant aspects of an application,
  so let CI handle that as a general gate for merges into the trunk branch.

- When executing/running tests, parallelize as much as possible. Use every core the machine
  has (check `nproc`), so don't be conservative. Test feedback should come as quick as possible.

- Be a judicious and conservative when choosing to run tests to save time and wasted effort.
    - When refining a particular GUI element or small snippet of logic, only run
      unit tests _after_ the refinement is accepted (e.g., user requesting refinements
      on the appearance or UX of a front-end component); not after every single
      change!
    - Running a test after small incremental changes within a chain of upcoming
      incremental changes at every increment is probably overkill, and you'd be
      better off running once in the beginning, once in the middle, and then once
      at the end, or perhaps even leaving it off the commit time.

# Where should working artifacts be placed? (e.g., build tree, containers, etc.)

- Use `/tmp` for small-to-moderately large temporary files that are cheap to
  recreate if lost. Examples include (but are not limited to) compiler scratch
  files, decompression intermediates, sockets, temporary test outputs, build trees
  of one-off builds that are fine if lost (e.g., a quick check to see if a codebase
  builds), and temporary caches. **Always** be mindful of the space you are taking
  up from our system's precious and limited `/tmpfs` RAM (≤ 30GB)!

- Keep generated files (build output, coverage data, reports, logs) out of the
  source tree: write them to the out-of-tree build directory, and run tools
  that write to the current directory (e.g. `gcov`) from there. A source tree
  or worktree should only ever contain intentional changes.

- If OOM'ing is a problem, avoid using `/tmp` and clear out any work you have in
  `/tmp` to alleviate the OOM'ing problem.

- Where the development machine has a dedicated scratch volume (e.g. `/workspace`),
  use it for large workloads, or anything that should be persistent in
  case of a sudden shutdown, or anything that is _not_ cheap to recreate. Examples
  include (but are not limited to) build trees, containers, databases, persistent
  compiler/package manager caches, scratch work, model caches, benchmark outputs,
  and other intermediate work artifacts.

# Agentic Development Policy: Parallelizing Work for Efficiency

- Most of the time, there will be a **root** agent who operates as orchestrator,
  architect, integrator, decision-maker, and final reviewer, who makes good use
  of subagents to execute tasks in parallel whenever possible.

- Delegate eagerly and by default to the Claude subagents in `.claude/agents/`.
  The root agent runs at low effort; subagents run at `high` effort by default.
    - Scouting (read-only search): `scout` (Haiku).
    - Research, implementation, and testing of small-to-medium tasks:
      `researcher`, `implementer`, `tester` (Sonnet).
    - Architecture, deep audits, high-stakes reviews: `architect` (Opus).
    - Hard debugging (concurrency, timing, transports, decode mismatches):
      `debugger` (Opus by default; escalate to Fable via a model override after a
      few unsuccessful attempts, or on user request).
    - **Never poll** a background subagent for completion:
      no `until`/`while` + `pgrep`/`sleep` loops, no repeated `cat`/`tail` of
      its output. Launch it in the background, then do other independent work
      or end your turn; the harness notification is the completion signal.

- Root thread file edits are still appropriate for small changesets, quick one-offs,
  and work that needs the root's context.

- Eagerly parallelize work whenever possible to maximize efficiency of execution;
  the machine described above has the cores, memory, and storage for it.

- Use git worktrees for parallel work (see the next section for the full
  rules, which apply to concurrent sessions as well as to subagents).

- Subagents working in a worktree edit files only; the session that owns that
  worktree owns all git writes in it (branching, committing, merging) unless a
  project says otherwise.

# Concurrent Sessions on One Codebase: Always Work in a Worktree

The primary checkout is shared ground. A session that is going to modify files
works in its own worktree instead.

## The rule

- **Before your first edit**, decide where you are working. A read-only session
  (answering questions, exploring, reviewing) can stay in the primary checkout
  and needs no worktree. A session that will **write code creates a worktree
  first** and does the rest of its work there.

- Create it from anywhere, then work in it:

  ```bash
  git worktree add ../worktree/ironbird/<branch> -b <branch>
  cd ../worktree/ironbird/<branch>
  ```

## Foreign edits are not yours

- If `git status` shows modified or untracked files you did not create, they
  belong to another session or to me. **Do not stage, stash, revert, commit, or
  "clean up" them.** Stage only the files you touched, by name. Say what you
  saw and leave it alone.

## A worktree does not inherit the build environment

This is the trap that fails **silently**, so check it before running anything:

- A new worktree has no `.venv/`, `node_modules/`, `target/`, or build tree. So,
  give each worktree its own environment and verify what you are about to run
  actually resolves into that worktree.

## Shared runtime resources still collide

Worktrees isolate *files*, not the machine. Two sessions still contend for:

- **Ports.** A dev server or GUI started by another session already owns the
  default port. Pick a distinct one rather than killing the process you did not
  start.
- **Machine-wide and per-project state** (`~/.<tool>/*.db`, caches, lockfiles,
  per-project state files). A worktree copy of a project usually gets its own
  per-project state, but anything under `$HOME` is shared across every session.
- **Background jobs.** Do not kill processes you did not launch.

## Cleanup

- Remove a worktree once its branch is merged: `git worktree remove <path>`,
  then `git worktree prune`. Leaving merged worktrees behind is what makes
  `git worktree list` useless for seeing who is working on what.
- Check `git worktree list` when you start; a stale entry for an
  already-merged branch is safe to remove, an entry for an unmerged branch is
  probably another session's live work.

## Handing off worktree work for the user to manually test

Once a GUI- or TUI-visible change is complete and passes its gate, launch it
proactively for the user to try - do not wait to be asked. Check for a
project skill that already covers launching it for this purpose (convention:
`<project>-launch-from-worktree-for-user`). Use it if present; if absent, say
so and offer to write one rather than improvising each time.

Either way: launch from the worktree's own environment, not the primary
checkout's, and don't assume your shell's network namespace reaches the
user's terminal - verify (e.g. `curl` the port yourself) and always
give a `!<command>` fallback they can run themselves. Launching it is not
the same as the user confirming it works - that confirmation is still
required before closing out the change.

## General Guidelines Re-emphasized

- When writing commits, NEVER auto-add your agent name as a co-author, nor specify
  a remote session name like "Claude-Session".
