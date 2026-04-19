**MANDATORY: Run this review before EVERY `git commit` in the sigma-ci repo.**
This applies whenever you work on any file in `/Users/tjwells/Desktop/sigma-ci/src/` — no exceptions.

Run a pre-commit review of the sigma-ci tool before committing.

Follow these steps in order:

## Step 1 — Show the diff
Run `git diff --staged` (or `git diff HEAD` if nothing staged). Summarize what changed in plain English.

## Step 2 — TypeScript build check
Run: `npm run build` from the repo root.

If this fails, report the error and stop — do not proceed to commit.

## Step 3 — Regression pattern checks
Search the changed code (and surrounding context) for these known failure modes:

**A. Template literal regex backslash stripping**
Any regex defined inside a TypeScript template literal (backtick string) must double-escape backslashes.
For example: `\d` → `\\d`, `\/` → `\\/`, `\s` → `\\s`.
Grep `server.ts` for any single-backslash regex metacharacters inside template literals.
A `match(/\d/)` inside a template literal will compile as `match(/d/)` — this silently breaks
the regex and can crash the script block with `SyntaxError: Unterminated group`.

**B. No inline onclick with dynamic JS strings**
All interactive buttons and links must use `data-*` attributes, not `onclick="fn(this, '...')"`
or similar inline event handlers with dynamically-generated strings.
Grep `report.ts` for `onclick=` — there should be zero matches for anything that injects a
variable into the onclick value.
The single delegated `document.addEventListener('click', ...)` block handles all button clicks.

**C. `jsAttr` helper is gone**
The `jsAttr()` helper was removed in favour of data-* attributes. Grep all `src/**/*.ts` for
`jsAttr` — must return zero matches.

**D. Suggestion threshold**
In `formula-check.ts`, grep for the similarity threshold. It must be `>= 0.55` (not 0.4 or lower).
A lower threshold produces bad suggestions (unrelated column names that happen to share a few letters).

**E. Columns API is primary source**
In `formula-check.ts`, confirm that `client.getDataModelColumns()` is called and its results
populate `apiColsByElement` before the element loop. The spec `element.columns` array must only
be used as a fallback when the API returns nothing for an element (`availableNames.length === 0`).

**F. Own-name exclusion**
Inside `checkFormulas`, confirm that `ownNorm` (the column's own display name, normalised) is
excluded from `suggestionCandidates`. Prevents a column from suggesting itself as its own fix.

**G. Same-formula ref exclusion**
Inside `checkFormulas`, confirm that `validRefNorms` (the set of already-valid refs in the formula)
is computed and filtered out of `suggestionCandidates`. Prevents e.g. `[Start Date]` from being
suggested as a replacement for the broken `[End Date]` when both appear in the same formula.

## Step 4 — Logic review of changed code
Read every function that was modified. For each one:
- Does it correctly handle elements with no columns (e.g. empty elements, SQL-only sources)?
- Does it avoid mutating the available-names list across elements?
- Are there any regex patterns that could match across unexpected column name formats?
- Does error handling catch per-model failures without aborting the entire run?

## Step 5 — Mental test: formula checker
Trace through these representative cases and confirm the logic handles them correctly:

1. **SQL-source element** (e.g. a `source.kind = "sql"` element with an aggregation in the statement)
   The column alias from `AS alias` in the SQL must appear in `availableNormalized`, so a metric
   that references it (e.g. `Sum([Order Count])`) is NOT flagged as broken.

2. **Calculated metric referencing two valid columns** (e.g. `DateDiff("day",[Start Date],[End Date])`)
   Neither `[Start Date]` nor `[End Date]` should be in `suggestionCandidates` for the other.
   If `[End Date]` is broken, `[Start Date]` must NOT be suggested as a fix.

3. **Column whose display name matches a broken ref** (circular ref scenario)
   If a column named `Email Opt-In` has formula `[Email Opt-In] = 1`, the broken ref `Email Opt-In`
   must NOT suggest the column itself. `ownNorm` exclusion covers this.

4. **Passthrough column** (formula `[SCHEMA/COLUMN_NAME]`)
   `getColumnDisplayName` must return `COLUMN_NAME` (after the `/`), and the columns API should
   provide the authoritative display name. The spec fallback only applies when the API returns nothing.

## Step 6 — Final verdict
Report one of:
- **PASS** — all checks pass; safe to commit
- **FAIL** — list specific issues found; do NOT commit; suggest fixes
- **WARN** — commit is likely safe but flag items to monitor

Only after a PASS verdict should you proceed with `git commit`.
