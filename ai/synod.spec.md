# Specification: a paxos coordination agent

## Preliminaries

- Start recording how many tokens are used when you start work.
- When done, print out the stats on token use for the entire operation, and
  monetary cost estimate.
- Implementation language is go
- IPC language is gRPC.
- We will be using crypto libraries.
- We will be using SQLite for go as state.
- Use Google's glog go library for logging.
- Use bazel for all source code operations.
- Enforce that when go bazel rules are used, the repo name is `rules_go`.
- Enforce that when go GRPC library is used, the repo name is `grpc`.
- Use `bazel mod tidy` to update module versions in bazel.
- Use `bazel run @rules_go//go -- <args>` to run the go binary, do not use `go` directly.
- Use `bazel run //:gazelle` to update go build rules for new dependencies and
  otherwise.
- Use at least protobuf version 34.0, never downgrade.
- If you need to write temporary files you can use the dir `local`. Prefer to
  create files in a subdirectory of `local` keyed by this session's ID.
  - Anything that needs to be preserved across sessions, store in `local/common`.
- Never ignore errors: either propagate them with context attached, or log them.
- Don't use `glog.Fatalf`, instead log at `glog.Errof` and call `os.Exit` instead.
- Find and save LICENSE for all dirs in //third_party
- Use "Conventional Commits 1.0.0" when creating commits.
- Format all Markdown files to 80 columns.
- When starting new work:
  - create a new git branch.
    - If there are modified files or files in the index, make them part of
      the new branch.
  - Git pull from main to ensure you are reasonably up to date. Use the `gh`
    utility for creatign pull requests.
    - Only create PRs against remote `main` branch.

### Update P.1

- When creating sql data schema, have a field for version. Whenever the
  database schema changes, make sure to increment the expected version.
- At startup compare the schema version in the database with the expected
  version, in case of mismatch do not start.

## Requirements

- Write a single agent binary which runs as a single agent in a Paxos
  algorithm. Call a collection of agents which take part in the same Paxos
  algorithm run a "cell".
- Take as a parameter a directory, which will contain all state files, and all
  constant files.
  - Abort if one is not provided.
  - Assume that all needed files are at a fixed path from this directory. No
    files may be outside of this top level dir.
- Design an API that agents can use to talk Paxos to each other, define RPC
  functions and endpoints for all Paxos messages.
- Use a SQLite for storing the ledger and any other needed state, place it in
  the state directory.
- The state is a key-value store. The keys are unix-like paths, e.g. `/foo/bar`
  and similar.
- Serve a gRPC endpoint that can be used for agents to reach out to each other.
  - If the default gRPC endpoint port is not available, try for 1 minute to find
    a free port to use.
  - When found, print which port it is.
- Serve a http end point which serves a rudimentary web page allowing users to
  monitor the state of the algorithm.
  - If the default HTTP endpoint port is not available, try for 1 minute to find
    a free port to use.
  - When found, print which port it is.
  - Use locally saved bootstrap library to style the pages. Make them reasonably
    pretty.
  - Use multi-card design so that we can have multiple status pages to select
    from.
  - Have one page which shows the current participants in the paxos algorithm.
    - Make sure to show self.
  - Have one page which shows what messages we received and what we replied.
    - pretty-print the messages, have each message key take a row of text in
      a table cell.
  - Have a page which shows the URLs (and links to) all the endpoints of all
    known peers.
  - Have one page which shows in a tabular form the contents of the entire
    key value store, ordered by key name.
- Serve a http endpoint to which a future command line tool can connect to issue
  commands to the paxos consensus cell.
- Implement a Paxos decision algorithm based on this.
- Formulate unit tests for each behavior of the Paxos algorithm.
- Each Paxos message must include the identity of the sender.
- Each Paxos message must include a nonce which helps disambiguate between
  identical proposals from different agents.
- Formulate unit tests for each changes.
- Ensure that all bazel tests pass.
- Ensure that bazel build passes after each change.
- Add logging at each important decision point in the algorithm.

### Identities

- For now, each agent must adopt a unique identity, let it be a UUID. Use this
  UUID for its lifeteime.

#### Update I.1: short names

- For each peer, adopt an identity which is a short human name.
  - Download 1000 names, roughly half male, half female to pick, for each letter
    of the alphabet.
  - Associate each UUID with a short cell-unique human name picked initially at
    random from the list, and use that name in all dashboards.
  - If name proposal is rejected, select a new name, and retry.

#### Update I.2: more names

- Download 1000 names, roughly half male, half female to pick, for each letter
  of the alphabet.
- Make a unified list of these names, select names uniformly at random.
  Prefer names starting with "A" for first peer in the cell, names starting
  with "B" for second etc.

### Dynamic consensus

- When starting a new agent, either supply a --peer flag which gives it a hostport
  for a peer to connect to, or start a new paxos cell if started without.
- Each agent must keep a running view of the agent identities that take part in the
  paxos algorithm.
  - The identities are kept in the store with key `/_internal/peers`, and must
    contain identities of all peers, including self.
  - If no peers are known, then only self should be there.
  - Each agent must run a process which continuously syncs up this
    view with that of other agents. Agents must use Paxos to coordinate this update.
- When joining a new paxos cell:
  - The new participant must share its ID, and its gRPC and HTTPS endpoints.
  - all currently present agents must agree to admit the new agent.
    - Once that is done, the agent is admitted and can take part in decisions.
      Else, that agent will be ignored.

#### Update C.1:

- Periodically (flag-configurable, default 2 minutes) ping all peers to figure
  out if they are there.
  - Add a "ping" gRPC API endpoint for this.
  - For peers that are no longer responding, send a proposal to remove from
    `/_internal/peers`.

### Testing

- Unit tests for each bit of functionality.
- Add an integration test, which starts 5 processes which communicate between
  each other, and just send a command for all to exit. Once they all agree,
  finish. If they take more than 2 minutes, say it's a timeout.
- Add a unit test which starts 5 agents in a cell, then visits each one's
  HTTP endpoints, and verifies that in each of the outputs respectively
  all 5 peers are listed.

### Presentation

#### Update P.1:

- Modify the Messages panel to use peer short names in place of UUIDs. For
  completeness, however, add a table above the messages list, which shows a
  mapping of short name to peer ID, and its API endpoints, which must be
  hyperlinked.

#### Update P.2:

- Modify all HTML pages to reload by default every 1 minute.
- Add to all HTML pages a top dropdown menu, which allows the user to select
  different values for reload periods: 1 second, 10 seconds, 30 seconds, 1 minute
  5 minutes, 10 minutes, 20 minutes, 30 minutes, 1 hour.

#### Update P.3

- Prettify the "Recent messages" column "request" in the "Messages" page:
  - Parse the individual components of each message and display them as
    a set of hierarchical table cells confined within the existing cell.
- Prettify the "KV store" panel display: attempt to parse each value as JSON.
  If parsing is a success, then pretty print the JSON again as a hierarchical
  table.

#### Update P.4

- Make a new feature: when printing JSON and proto, order fields lexicographically by name.
- When printing maps, order the entries lexicographically by the string representation of the key.

### Safety

#### Update S.1: exponential backoff

- Modify all retriable operations to use exponential backoff: start with a small
  timeout, such as 100ms, each next retry is twice as long as the previous.
  - Start by creating a "exponential backoff" module, write tests for its
    behavior, then insert it wherever there are now timed waits.
  - Put in a verbose log line at every decision point in the retry.

## Bugs

### B.1: /api/command

The 'api/command' page is rendered as text, not as HTML, fix.

### B.2: do not persist port info for peers

Since peers can go away and come back, do not persist the gRPC and HTTP port
info for peers.

- Remove the gRPC and HTTP port info from the `/_internal/peers` key, and keep
  that information in a separate, "ephemeral" map which is not persisted.
- Key peers by their UUID. This is not safe, but we will handle safety later.
- Do not remove this info from the HTTP display, only source them from the
  ephemeral map.
