## General change rules

Do not change files in `//third_party` without user's explicit permission.

### General git commit rules

Any git commit created by Gemini must contain this note as the last line in the
commit message in addition to any commit summaries added:

```
This commit has been created by an automated coding assistant,
with human supervision.
```

Also append the prompt used to generate the commit in full.

### Prefer rebase over merge

Instead of creating merges into a branch try to rebase the current branch on
top of another.

For example, to get content from branch `main` from the `origin` repo use `git
rebase --pull origin main`.

### Rules for Bazel projects

- Use go if you need to create scripts.
- Save scripts into `//tools/ai` directory, you can freely add BUILD.bazel
  file there and build targets to invoke.
- Put each new script into its own new subdirectory, feel free to use that
  subdir for all required work.
- You can use library https://github.com/bitfield/script to get shell script
  like functionality within go.
- Use `bazel run` to run the script when you need it.

### Create pull request

Use the `gh` utility to create the pull request.

Use the remote `origin/main` as a baseline for the pull request.

Any pull request you create must contain this note as the last line in the
commit message in addition to any commit summaries added:

```
This pull request has been created by an automated coding assistant,
with human supervision.
```

Also append the prompt used to generate the pull request in full.

Rebase the branch `main` from remote `origin/main`.

Create a new branch from this branch. Use name pattern `ai-dev-XXX` where XXX
is replaced with date encoded as `YYYYMMDD` and a short nonce. Push the current
branch to remote.

Create the pull request, using in the pull request description a summary of all
the commits between `origin/main` and the top of this branch that will be part
of this pull request.

## Maintenance rules

- If README.md file does not exist, create one.
- Add to README.md the status badges of the github workflows.
- On major updates, update the README.md file with summary of features that exist.
- Format all Markdown files you touch to 80 columns.


## Go code style

- When adding generated HTML, instead of inline strings, use HTML templates,
  and place the template text into string constants.
