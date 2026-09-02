---
name: ak-to-ck-sync
description: Check whether the Apache Kafka -> Confluent Kafka branch sync (Semaphore project kafka-overlay, task apache-kafka-test) is failing, and if the failure is a merge conflict, perform the manual merge and open a PR against confluentinc/kafka in the same shape as PR #2076.
argument-hint: "[branch|semaphore-job-or-workflow-url]"
---

# AK to CK Sync Skill

Apache Kafka (`apache/kafka`) is continuously merged into Confluent's fork (`confluentinc/kafka`)
by an automated Semaphore task. When that automation hits a merge conflict, it fails and someone
has to do the merge by hand and open a PR. This skill checks sync status and, for conflict
failures, does that manual merge and opens the PR.

## Mental model

- `apache/kafka:trunk` maps to `confluentinc/kafka:master`. Every other branch name matches 1:1
  (e.g. `apache/kafka:4.4` -> `confluentinc/kafka:4.4`).
- "Latest five branches" = `master` + the four highest `N.M` release branches that exist on
  `confluentinc/kafka` right now. This drifts as Kafka cuts/retires branches — re-derive it, don't
  hardcode a branch list.
- This repo checkout already has both remotes configured: `origin` = `confluentinc/kafka` (push
  ok), `apache-kafka` = `apache/kafka` (fetch-only by policy — never push here). Reuse these exact
  remote names.
- The sync automation lives in a separate private repo, `confluentinc/kafka-overlay`, run as a
  Semaphore **Task** named `apache-kafka-test` under Semaphore **project `kafka-overlay`**. Its
  pipeline file is `.semaphore/ak_to_ck_pipeline.yml` (not visible from this repo). It runs
  `make merge_ak_to_ck`, which shells out to `kafka-test/merge.sh` and — inside a fresh clone of
  `apache/kafka` at `AK_BRANCH` — does roughly `git remote add confluent <CK_REPO>` then
  `git pull --no-rebase --no-edit confluent <CK_BRANCH>`, then fast-forward-pushes the result onto
  CK's branch. It also special-cases `CK_OVERRIDE_FILES=Jenkinsfile` (a CK-only file AK doesn't
  have).
- **Why conflicts happen almost every time**: CK carries its own version strings (e.g.
  `8.4.0-0-ccs` on the `4.4` branch) in a small fixed set of files, while AK bumps its own version
  strings (e.g. `4.4.0`) in the same files on every release-branch commit. Every AK->CK sync hits
  the same handful of paths with a pure version-string conflict. The house convention (see PR
  [#2076](https://github.com/confluentinc/kafka/pull/2076)) is to always keep CK's side —
  literally "ignore AK version upgrades."

**Known low-risk override files** (verified against PR #2076 on `4.3` and a live conflict on `4.4`
while writing this skill — treat as a strong prior, not gospel; a new file can join this list as
Kafka's build evolves):
```
gradle.properties
streams/quickstart/java/pom.xml
streams/quickstart/java/src/main/resources/archetype-resources/pom.xml
streams/quickstart/pom.xml
tests/kafkatest/__init__.py
tests/kafkatest/version.py
Jenkinsfile   (CK-only; usually doesn't exist upstream so it won't even show as a conflict)
```

**Never auto-resolve a conflict outside this list, and never trust the list blindly** — always
look at the actual conflict hunk first (see Step 3). The override list above is one *known*
conflict shape, not the only one that can occur. Anything else is an **unknown conflict**: stop
and get the user's input on how to resolve it (see Step 3) rather than guessing or reusing the
version-upgrade rationale for something it doesn't apply to.

## Step 1 — Determine branches to check

If the user gave an explicit branch or pasted a Semaphore job/workflow URL, use just that.
Otherwise derive the tracked set from `confluentinc/kafka`:

```bash
git fetch origin --prune
git ls-remote --heads origin | grep -oE '[0-9]+\.[0-9]+$' | sort -t. -k1,1n -k2,2n | tail -4
```
Tracked set = `master` + those four. For each, `AK_BRANCH` = `trunk` if `CK_BRANCH == master`,
else identical to `CK_BRANCH`.

## Step 2 — Check current Semaphore sync status per branch

Use `mcp__chewie__semaphore_list_pipelines(project_name="kafka-overlay", branch_name=<AK_BRANCH>,
finished_after="now-4d")` — **not** `semaphore_list_workflows`. The `kafka-overlay` project runs
multiple unrelated scheduled tasks on the same branches (notably a "System Test Kafka Nightly Run"
via `system_test_kafka_trigger.yml`), interleaved in the same list. Do not assume the newest entry
is the sync job.

Filter the results to `name == "apache-kafka-to-confluent-kafka/apache-kafka-test"` (equivalently
`yaml_file_name == "ak_to_ck_pipeline.yml"`), take the newest (first) match, and read its
`result`.

- `result != "FAILED"` -> sync is fine for this branch. Nothing to do.
- `result == "FAILED"` -> note the `ppl_id`, get its job via `mcp__chewie__semaphore_get_pipeline`
  (`blocks[].jobs[]`, the one with `result != "PASSED"`), then read its logs:
  `mcp__chewie__semaphore_get_job_logs(job_id=..., grep_pattern="(CONFLICT \\(content\\)|Automatic merge failed|error:|fatal:)")`.
  - Lines like `CONFLICT (content): Merge conflict in <path>` give you the exact conflicting
    files — extract them.
  - If `result == "FAILED"` but there are **no** `CONFLICT` lines, this is not a merge conflict
    (flaky infra, auth, network, etc.). Report it and stop for that branch — this skill only
    automates the merge-conflict recovery path. Suggest re-running the Semaphore job first.

Gotchas seen in practice:
- `branch_name="trunk"` intermittently 500s on both `semaphore_list_pipelines` and
  `semaphore_list_workflows` for this project. Retry once; if it still fails, fall back to
  reporting master's last-known-good status and say the live re-check errored.
- `semaphore_get_job_logs` can exceed the tool's output-size limit on a big job; it then saves the
  full log to a file and tells you to read it in chunks — or just retry with a narrower
  `grep_pattern`.
- If the user pasted a URL instead: `.../jobs/<uuid>` is a job id directly; `.../workflows/<uuid>?pipeline_id=<uuid2>`
  gives you the workflow id and pipeline id directly — skip straight to `semaphore_get_pipeline`.

Report a status table for all checked branches before doing anything else.

## Step 3 — Manual merge (only for branches confirmed failing on a real conflict)

Do this in an isolated git worktree so the user's current checkout is untouched — **do not** use
the `EnterWorktree` tool for this (it's reserved for cases the user or CLAUDE.md explicitly asks
for); plain `git worktree` commands are fine and keep this skill portable:

```bash
CK_BRANCH=4.4          # example
AK_BRANCH=4.4          # trunk if CK_BRANCH=master
NODOT=$(echo "$CK_BRANCH" | tr -d '.')

git fetch origin "$CK_BRANCH"
git fetch apache-kafka "$AK_BRANCH"
git worktree add "<scratchpad-dir>/ak-ck-merge-$NODOT" -b "ccs/$CK_BRANCH" "origin/$CK_BRANCH"
cd "<scratchpad-dir>/ak-ck-merge-$NODOT"
git merge "apache-kafka/$AK_BRANCH"
```

This will conflict. Go file by file and keep a running note of *how* each one was resolved — you
need that record verbatim for the commit message and PR body later, so don't paraphrase it away.

For **each** conflicting file:
1. Look at the hunk: `grep -n -A2 -B2 '^<<<<<<<\|^=======\|^>>>>>>>' <file>`.
2. If it's on the known-override list above **and** the hunk shows only a version/snapshot string
   differing (nothing else): `git checkout --ours -- <file> && git add <file>`, and note it as
   `<file> - kept CK version (ignore AK version bump)`.
3. Otherwise, this is an **unknown conflict** — do not guess, and do not label it with the
   version-upgrade rationale, because that's very likely not what's actually happening. Stop and
   ask the user how to resolve it: show them the hunk (AskUserQuestion works well if there's a
   short menu of sensible resolutions — keep ours/keep theirs/manual edit — otherwise just ask in
   conversation). Resolve it exactly as they direct, `git add` it, and note it as
   `<file> - <one-line summary of what the user actually decided>`, in their own terms — not a
   restatement of the version-upgrade note.

Once every conflict is staged and noted, build the `Conflicts:` block from that per-file record —
**every file gets its own resolution note; never write one blanket line covering all of them**.
If every conflict genuinely was the known version-bump pattern, the block will end up looking like
PR #2076's; that's a consequence of what actually happened, not a template to fill in by default.
Commit with a message matching house convention:

```bash
git commit -m "$(cat <<EOF
Merge remote-tracking branch 'apache-kafka/$AK_BRANCH' into ccs/$CK_BRANCH

Conflicts:
	<file1> - kept CK version (ignore AK version bump)
	<file2> - kept CK version (ignore AK version bump)
	<file3> - <actual resolution for this one, only if it differed>
EOF
)"
```

Then show the user what actually changed (this is what the PR diff will look like):
```bash
git diff --stat "origin/$CK_BRANCH" HEAD
```
The known-override files should show **zero** diff (their content stayed identical to CK's side);
only genuinely new upstream changes should appear here. Skim that diff too — it's normal upstream
churn (dependency pin bumps, etc.), but it's worth a glance since it's about to go in a PR.

## Step 4 — Confirmation gate (never skip)

Pushing a branch to `confluentinc/kafka` and opening a PR are shared, visible, hard-to-reverse
actions. Before doing either, show the user:
- Which branch, and a link to the failing Semaphore job.
- Every conflict file and how it was resolved.
- The `git diff --stat` from Step 3.
- The exact branch name, PR title, and PR body you're about to push/open.

Get explicit go-ahead before continuing.

## Step 5 — Push and open the PR

Personal branch naming convention (matches PR #2076's head ref `omkreddy-26-Jun30-43-1`):
`<gh-username>-<yy>-<Mon><Day>-<branch-no-dot>-<n>`.

```bash
GH_USER=$(gh api user -q .login)
TAG="${GH_USER}-$(date +%y-%b%-d)-${NODOT}"
N=1
while git ls-remote --heads origin "${TAG}-${N}" | grep -q .; do N=$((N+1)); done
PERSONAL_BRANCH="${TAG}-${N}"
```

**Check for a duplicate PR first** (important if this skill runs repeatedly/unattended):
```bash
gh pr list --repo confluentinc/kafka --base "$CK_BRANCH" --state open \
  --search "Merge remote-tracking branch apache-kafka in:title"
```
If one's already open for this branch, point the user to it instead of opening a second one.

Otherwise, after confirmation, push with **`git push-external`, never plain `git push`**:
`confluentinc/kafka` is a public repo, and Confluent laptops block direct `git push` to
public/external repositories as a proprietary-code-leak safeguard. `git push-external` takes the
exact same arguments as `git push`; it stages the push to a private "Airlock" mirror, waits for an
automated proprietary-code scan, then auto-forwards to the real destination once clear (usually
well under a minute for a routine upstream merge like this). If the scan ever flags something, the
command fails with a link to a "Bypass Pull Request" — that PR has to be reviewed/merged before
re-running the exact same `git push-external` command.
```bash
git push-external origin "ccs/$CK_BRANCH:$PERSONAL_BRANCH"
gh pr create --repo confluentinc/kafka \
  --base "$CK_BRANCH" --head "$PERSONAL_BRANCH" \
  --title "Merge remote-tracking branch 'apache-kafka/$AK_BRANCH' into ccs/$CK_BRANCH" \
  --body "$(cat <<EOF
Conflicts:
$CONFLICTS_BLOCK
EOF
)"
```
`$CONFLICTS_BLOCK` is the exact same per-file resolution record you built for the commit message
in Step 3 — reuse it verbatim, don't re-derive or re-summarize it. Report the PR URL back to the
user.

## Step 6 — Cleanup

```bash
git worktree remove "<scratchpad-dir>/ak-ck-merge-$NODOT"
```

## Reference example

- Live conflict found 2026-08-22/23 on `4.4` (Semaphore job that triggered writing this skill):
  same six files, CK side `8.4.0-0-ccs`, AK side `4.4.0`.
- [PR #2076](https://github.com/confluentinc/kafka/pull/2076): manual merge of `apache-kafka/4.3`
  into `ccs/4.3` on 2026-06-30, same six-file conflict list, head branch `omkreddy-26-Jun30-43-1`,
  base `4.3`, no labels. Its body is just the file list plus one blanket "ignore AK version
  upgrades" line — that's correct *there* because all six conflicts really were that pattern, not
  because the note is a fixed part of the template (see Step 3).

## Notes

- If `git push` is rejected for permissions, the user may need to push to their own fork instead
  of a personal branch on `confluentinc/kafka` directly — surface the error, don't silently
  retry with a different remote.
- A manual-merge PR landing doesn't retroactively fix the failed Semaphore run; the sync job will
  simply pass on its next scheduled run against the now-caught-up branch.
- Handle multiple simultaneously-failing branches independently — one worktree/branch/PR per CK
  branch, each through its own confirmation gate.
