package server

import (
	"fmt"
	"net/http"
	"sort"

	"github.com/golang/glog"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
)

type HTTPServer struct {
	store *state.Store
	cell  *paxos.Cell
	addr  string
}

func NewHTTPServer(addr string, store *state.Store, cell *paxos.Cell) *HTTPServer {
	return &HTTPServer{
		addr:  addr,
		store: store,
		cell:  cell,
	}
}

func (s *HTTPServer) Run() error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/messages", s.handleMessages)
	mux.HandleFunc("/peers", s.handlePeers)
	mux.HandleFunc("/api/command", s.handleCommand)
	
	// Serve locally saved bootstrap
	mux.HandleFunc("/static/bootstrap.min.css", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "third_party/bootstrap/bootstrap.min.css")
	})
	mux.HandleFunc("/static/bootstrap.bundle.min.js", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "third_party/bootstrap/bootstrap.bundle.min.js")
	})

	glog.Infof("HTTP server listening at %v", s.addr)
	return http.ListenAndServe(s.addr, mux)
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	agentID, _ := s.store.GetAgentID()
	members, _ := s.store.GetMembers()
	highestIndex, _ := s.store.GetHighestLedgerIndex()

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Synod Agent - %s</title>
    <link href="/static/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
      <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link active" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link" href="/messages">Messages</a></li>
            <li class="nav-item"><a class="nav-link" href="/peers">Peers</a></li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="container">
      <div class="row">
        <div class="col-md-4">
          <div class="card shadow-sm mb-4">
            <div class="card-header bg-primary text-white">Agent Info</div>
            <div class="card-body">
              <p class="card-text"><strong>ID:</strong> <small class="text-muted">%s</small></p>
              <p class="card-text"><strong>Ledger Size:</strong> %d</p>
            </div>
          </div>
        </div>
        <div class="col-md-8">
          <div class="card shadow-sm mb-4">
            <div class="card-header bg-success text-white">Participants</div>
            <div class="card-body">
              <div class="table-responsive">
                <table class="table table-hover">
                  <thead>
                    <tr><th>Agent ID</th><th>Address</th></tr>
                  </thead>
                  <tbody>`, agentID, agentID, highestIndex)

	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		fmt.Fprintf(w, "<tr><td><small>%s</small></td><td>%s</td></tr>", id, members[id])
	}

	fmt.Fprintf(w, `
                  </tbody>
                </table>
              </div>
            </div>
          </div>

          <div class="card shadow-sm mb-4">
            <div class="card-header bg-info text-white">Propose Command</div>
            <div class="card-body">
              <form action="/api/command" method="post" class="row g-3">
                <div class="col-auto">
                  <input type="text" name="value" class="form-control" placeholder="Command value">
                </div>
                <div class="col-auto">
                  <button type="submit" class="btn btn-primary">Propose</button>
                </div>
              </form>
            </div>
          </div>
        </div>
      </div>
    </div>
    <script src="/static/bootstrap.bundle.min.js"></script>
  </body>
</html>`)
}

func (s *HTTPServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	agentID, _ := s.store.GetAgentID()
	entries, _ := s.store.GetRecentMessages(100)

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Synod Agent Messages - %s</title>
    <link href="/static/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
      <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link active" href="/messages">Messages</a></li>
            <li class="nav-item"><a class="nav-link" href="/peers">Peers</a></li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="container">
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-secondary text-white">Recent Messages</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-hover">
              <thead>
                <tr>
                  <th>ID</th><th>Time</th><th>Type</th><th>Sender</th><th>Receiver</th><th>Request</th><th>Reply</th>
                </tr>
              </thead>
              <tbody>`, agentID)

	for _, e := range entries {
		fmt.Fprintf(w, `
                <tr>
                  <td>%d</td>
                  <td><small class="text-nowrap">%s</small></td>
                  <td><span class="badge bg-info text-dark">%s</span></td>
                  <td><small>%s</small></td>
                  <td><small>%s</small></td>
                  <td><pre class="small mb-0" style="max-height: 100px; overflow: auto;"><code>%s</code></pre></td>
                  <td><pre class="small mb-0" style="max-height: 100px; overflow: auto;"><code>%s</code></pre></td>
                </tr>`, e.ID, e.Timestamp, e.Type, e.Sender, e.Receiver, e.Message, e.Reply)
	}

	fmt.Fprintf(w, `
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    <script src="/static/bootstrap.bundle.min.js"></script>
  </body>
</html>`)
}

func (s *HTTPServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	agentID, _ := s.store.GetAgentID()
	members, _ := s.store.GetMembers()

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Synod Agent Peers - %s</title>
    <link href="/static/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
      <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link" href="/messages">Messages</a></li>
            <li class="nav-item"><a class="nav-link active" href="/peers">Peers</a></li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="container">
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-warning text-dark">Known Peers</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-hover">
              <thead>
                <tr>
                  <th>Agent ID</th>
                  <th>gRPC URL Endpoint</th>
                </tr>
              </thead>
              <tbody>`, agentID)

	var ids []string
	for id := range members {
		if id != agentID { // Tabulate all *other* known peers
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	for _, id := range ids {
		addr := members[id]
		urlEndpoint := fmt.Sprintf("grpc://%s", addr)
		fmt.Fprintf(w, `
                <tr>
                  <td><code>%s</code></td>
                  <td><a href="%s" class="text-decoration-none">%s</a></td>
                </tr>`, id, urlEndpoint, urlEndpoint)
	}

	fmt.Fprintf(w, `
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </div>
    <script src="/static/bootstrap.bundle.min.js"></script>
  </body>
</html>`)
}

func (s *HTTPServer) handleCommand(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}
	
	val := r.FormValue("value")
	if val == "" {
		http.Error(w, "Value is required", http.StatusBadRequest)
		return
	}

	glog.Infof("HTTP: Received proposal for value: %s", val)
	if err := s.cell.Propose(r.Context(), []byte(val)); err != nil {
		glog.Errorf("HTTP: Proposal failed: %v", err)
		http.Error(w, fmt.Sprintf("Paxos proposal failed: %v", err), http.StatusInternalServerError)
		return
	}
	
	fmt.Fprintf(w, "Consensus reached for value: %s. <a href='/'>Back</a>", val)
}
