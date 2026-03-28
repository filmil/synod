# Specification: a paxos coordination agent

## Preliminaries

* Start recording how many tokens are used when you start work.
* When done, print out the stats on token use for the entire operation, and
  monetary cost estimate.
* Implementation language is go
* IPC language is gRPC.
* We will be using crypto libraries.
* We will be using SQLite for go as state.
* Use Google's glog go library for logging.
* Use bazel for all source code operations.
* Enforce that when go bazel rules are used, the repo name is `rules_go`.
* Enforce that when go GRPC library is used, the repo name is `grpc`.
* Use `bazel mod tidy` to update module versions in bazel.
* Use `bazel run @rules_go//go -- <args>` to run the go binary, do not use `go` directly.
* Use at least protobuf version 34.0, never downgrade.
* If you need to write temporary files you can use the dir `local`. Prefer to
  create files in a subdirectory of `local` keyed by this session's ID.
  * Anything that needs to be preserved across sessions, store in `local/common`.
* Never ignore errors: either propagate them with context attached, or log them.

## Requirements

* Write a single agent binary which runs as a single agent in a Paxos
  algorithm. Call a collection of agents which take part in the same Paxos
  algorithm run a "cell".
* Take as a parameter a directory, which will contain all state files, and all
  constant files. Abort if one is not provided.  Assume that all needed files
  are at a fixed path from this directory. No files may be outside of this
  top level dir.
* Design an API that agents can use to talk Paxos to each other, define RPC
  functions and endpoints for all Paxos messages.
* Use a SQLite for storing the ledger and any other needed state, place it in
  the state directory.
* The state is a key-value store. The keys are unix-like paths, e.g. `/foo/bar`
  and similar.
* Serve a gRPC endpoint that can be used for agents to reach out to each other.
* Serve a http end point which serves a rudimentary web page allowing users to
  monitor the state of the algorithm.
  * Use locally saved bootstrap library to style the pages. Make them reasonably
    pretty.
  * Use multi-card design so that we can have multiple status pages to select
    from.
  * Have one page which shows the current participants in the paxos algorithm.
    * Make sure to show self.
  * Have one page which shows what messages we received and what we replied.
    * pretty-print the messages, have each message key take a row of text in
      a table cell.
  * Have a page which shows the URLs (and links to) all the endpoints of all
    known peers.
  * Have one page which shows in a tabular form the contents of the entire
    key value store, ordered by key name.
* Serve a http endpoint to which a future command line tool can connect to issue
  commands to the paxos consensus cell.
* Implement a Paxos decision algorithm based on this.
* Formulate unit tests for each behavior of the Paxos algorithm.
* Each Paxos message must include the identity of the sender.
* Each Paxos message must include a nonce which helps disambiguate between
  identical proposals from different agents.
* Formulate unit tests for each changes.
* Ensure that all bazel tests pass.
* Ensure that bazel build passes after each change.
* Add logging at each important decision point in the algorithm.

### Identities

* For now, each agent must adopt a unique identity, let it be a UUID. Use this
  UUID for its lifeteime.

### Dynamic consensus

* When starting a new agent, either supply a --peer flag which gives it a hostport
  for a peer to connect to, or start a new paxos cell if started without.
* Each agent must keep a running view of the agent identities that take part in the
  paxos algorithm.
  * The identities are kept in the store with key `/_internal/peers`, and must
    contain identities of all peers, including self.
  * If no peers are known, then only self should be there.
  * Each agent must run a process which continuously syncs up this
  view with that of other agents. Agents must use Paxos to coordinate this update.
* When joining a new paxos cell, all currently present agents must agree to admit
  the new agent. Once that is done, the agent is admitted and can take part in
  decisions. Else, that agent will be ignored.

### Testing


* Unit tests for each bit of functionality.
* Add an integration test, which starts 5 processes which communicate between
  each other, and just send a command for all to exit. Once they all agree,
  finish. If they take more than 2 minutes, say it's a timeout.
