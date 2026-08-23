---
name: worktrees
description: Create, place, open, and remove git worktrees. Use whenever creating a worktree, working on an isolated branch checkout, reviewing a PR in a separate checkout, or relocating an agent-created worktree.
---

# Git worktrees

The non-negotiable rules:

1. **Ask where the branch comes from — before running anything.** A new
   worktree branch's base ref is never inferred, not from the current HEAD and
   not from `main`. Ask the user which ref to branch off, e.g.:

   > Which ref should `<branch>` start from — `origin/main`, your current
   > `<current-branch>`, or something else?

   Then fetch and pass that ref **explicitly** to `git worktree add`, so the
   answer is visible in the command rather than implied by whatever HEAD
   happened to be:

   ```bash
   git fetch origin                 # if the base is a remote ref
   git worktree add ../dagworker.worktrees/<name> -b <branch> <base-ref>
   ```

   For an existing branch there is no base ref to ask about — just check it
   out. Skip the question **only** when the user already named the base
   themselves ("branch off the release branch", "off my current work").

2. **Location**: every worktree goes in the sibling directory
   `../dagworker.worktrees/<name>` (relative to the repo root) — one worktree
   per branch. Never use `.claude/worktrees/` (gitignored, IDEs hide it) and
   never nest a `.worktrees/` directory inside the repo (IDE folds it into the
   parent VCS root, dirties `git status`).

3. **Create** (from the repo root):

   ```bash
   mkdir -p ../dagworker.worktrees
   git worktree add ../dagworker.worktrees/<name> -b <branch> <base-ref>  # new branch
   git worktree add ../dagworker.worktrees/<name> <existing-branch>
   ```

   Nothing else needs linking: `go.work` is committed and every module path in
   it is relative, so a worktree resolves the workspace against its own
   checkout with no setup.

4. **The test databases are shared, and that is the one real hazard.**
   `make up` starts PostgreSQL and Redis on fixed ports (15432, 16379) from
   `test/e2e/docker-compose.test.yml`. Every worktree talks to those same two
   containers, and `make integration` and `make complexity` both begin by
   truncating them. Two worktrees running either target at once will destroy
   each other's data mid-test and fail in ways that look like real bugs.

   Run database-backed targets in one worktree at a time. `make check`,
   `make test`, `make race` and `make complexity-quick` need no databases and
   are safe to run anywhere, concurrently.

5. **Open it in VS Code — the worktree is not done until this runs.** A
   worktree nobody can see is useless: VS Code never discovers a new sibling
   worktree on its own. Add it to the **current window** as an extra workspace
   root, so the Source Control panel lists it as its own repository on its own
   branch:

   ```bash
   code -a ../dagworker.worktrees/<name>
   ```

   Treat this as part of creating a worktree, not an optional extra — run it in
   the same turn, and tell the user the worktree is now a root in their window
   and which branch it sits on. If `code` is missing from `PATH`, say so and
   point at VS Code's *Shell Command: Install 'code' command in PATH* rather
   than silently leaving the worktree unopened.

   Only use `code <path>` (a separate window) when the user explicitly asks for
   a standalone window. To persist the multi-root layout across restarts, save a
   `*.code-workspace` listing both roots.

6. **Relocate** an agent-created worktree out of `.claude/worktrees/`, then open
   it with `code -a` as in step 5:

   ```bash
   mkdir -p ../dagworker.worktrees
   git worktree move .claude/worktrees/<name> ../dagworker.worktrees/<name>
   ```

7. **Remove** with git, never by deleting the directory. Remove the folder from
   the VS Code workspace too, or it lingers as a missing root:

   ```bash
   git worktree remove ../dagworker.worktrees/<name>   # --force if dirty
   ```
