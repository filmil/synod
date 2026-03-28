package server

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	"github.com/golang/glog"
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

func (s *HTTPServer) Run(lis net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/messages", s.handleMessages)
	mux.HandleFunc("/peers", s.handlePeers)
	mux.HandleFunc("/store", s.handleStore)
	mux.HandleFunc("/api/command", s.handleCommand)
	
	// Serve locally saved bootstrap via runfiles
	mux.HandleFunc("/static/bootstrap.min.css", func(w http.ResponseWriter, r *http.Request) {
		path, err := runfiles.Rlocation("_main/third_party/bootstrap/bootstrap.min.css")
		if err != nil {
			// fallback for bzlmod if _main doesn't work
			path, err = runfiles.Rlocation("synod/third_party/bootstrap/bootstrap.min.css")
		}
		if err != nil {
			glog.Errorf("Failed to resolve runfile: %v", err)
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.ServeFile(w, r, path)
	})
	mux.HandleFunc("/static/bootstrap.bundle.min.js", func(w http.ResponseWriter, r *http.Request) {
		path, err := runfiles.Rlocation("_main/third_party/bootstrap/bootstrap.bundle.min.js")
		if err != nil {
			path, err = runfiles.Rlocation("synod/third_party/bootstrap/bootstrap.bundle.min.js")
		}
		if err != nil {
			glog.Errorf("Failed to resolve runfile: %v", err)
			http.Error(w, "File not found", http.StatusNotFound)
			return
		}
		http.ServeFile(w, r, path)
	})

	return http.Serve(lis, mux)
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	agentID, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	
	val, _, _, _, err := s.store.GetKVEntry("/_internal/peers")
	if err != nil {
		http.Error(w, "Failed to get peers from KV store", http.StatusInternalServerError)
		return
	}
	members := make(map[string]state.PeerInfo)
	if val != nil {
		if err := json.Unmarshal(val, &members); err != nil {
			glog.Errorf("Failed to unmarshal peers map: %v", err)
		}
	}

	kvs, err := s.store.GetAllKVs()
	if err != nil {
		http.Error(w, "Failed to get KV store", http.StatusInternalServerError)
		return
	}

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
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent: %s</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link active" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link" href="/store">KV Store</a></li>
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
              <p class="card-text"><strong>Name:</strong> %s</p>
              <p class="card-text"><strong>Keys Count:</strong> %d</p>
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
                    <tr><th>Agent ID</th><th>Name</th><th>gRPC Address</th><th>HTTP Dashboard</th></tr>
                  </thead>
                  <tbody>`, shortName, shortName, agentID, shortName, len(kvs))

	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		label := id
		if id == agentID {
			label = fmt.Sprintf("%s <span class=\"badge bg-secondary\">self</span>", id)
		}
		info := members[id]
		httpLink := ""
		if info.HTTPURL != "" {
			httpLink = fmt.Sprintf("<a href=\"%s\" class=\"text-decoration-none\" target=\"_blank\">%s</a>", info.HTTPURL, info.HTTPURL)
		} else {
			httpLink = "<span class=\"text-muted\">N/A</span>"
		}
		fmt.Fprintf(w, "<tr><td><small>%s</small></td><td>%s</td><td><code>%s</code></td><td>%s</td></tr>", label, info.ShortName, info.GRPCAddr, httpLink)
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
                  <input type="text" name="key" class="form-control" placeholder="Key (e.g. /my/key)">
                </div>
                <div class="col-auto">
                  <input type="text" name="value" class="form-control" placeholder="Value">
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
	_, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	entries, err := s.store.GetRecentMessages(100)
	if err != nil {
		http.Error(w, "Failed to get recent messages", http.StatusInternalServerError)
		return
	}

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
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent: %s</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link" href="/store">KV Store</a></li>
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
              <tbody>`, shortName, shortName)

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
	agentID, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	
	val, _, _, _, err := s.store.GetKVEntry("/_internal/peers")
	if err != nil {
		http.Error(w, "Failed to get peers from KV store", http.StatusInternalServerError)
		return
	}

	members := make(map[string]state.PeerInfo)
	if val != nil {
		if err := json.Unmarshal(val, &members); err != nil {
			glog.Errorf("Failed to unmarshal peers map: %v", err)
		}
	}

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
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent: %s</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link" href="/store">KV Store</a></li>
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
                  <th>Name</th>
                  <th>gRPC Address</th>
                  <th>HTTP Dashboard</th>
                </tr>
              </thead>
              <tbody>`, shortName, shortName)

	var ids []string
	for id := range members {
		if id != agentID { // Tabulate all *other* known peers
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	for _, id := range ids {
		info := members[id]
		grpcURL := fmt.Sprintf("grpc://%s", info.GRPCAddr)
		httpLink := ""
		if info.HTTPURL != "" {
			httpLink = fmt.Sprintf("<a href=\"%s\" class=\"text-decoration-none\" target=\"_blank\">%s</a>", info.HTTPURL, info.HTTPURL)
		} else {
			httpLink = "<span class=\"text-muted\">N/A</span>"
		}
		fmt.Fprintf(w, `
                <tr>
                  <td><code>%s</code></td>
                  <td>%s</td>
                  <td><a href="%s" class="text-decoration-none">%s</a></td>
                  <td>%s</td>
                </tr>`, id, info.ShortName, grpcURL, grpcURL, httpLink)
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

func (s *HTTPServer) handleStore(w http.ResponseWriter, r *http.Request) {
	_, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	kvs, err := s.store.GetAllKVs()
	if err != nil {
		http.Error(w, "Failed to get KV store", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Synod Agent KV Store - %s</title>
    <link href="/static/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
      <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent: %s</span>
        <button class="navbar-toggler" type="button" data-bs-toggle="collapse" data-bs-target="#navbarNav">
          <span class="navbar-toggler-icon"></span>
        </button>
        <div class="collapse navbar-collapse" id="navbarNav">
          <ul class="navbar-nav me-auto mb-2 mb-lg-0">
            <li class="nav-item"><a class="nav-link" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link active" href="/store">KV Store</a></li>
            <li class="nav-item"><a class="nav-link" href="/messages">Messages</a></li>
            <li class="nav-item"><a class="nav-link" href="/peers">Peers</a></li>
          </ul>
        </div>
      </div>
    </nav>
    <div class="container">
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-info text-white">Key-Value Store</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-hover">
              <thead>
                <tr>
                  <th>Key</th>
                  <th>Value</th>
                  <th>Type</th>
                  <th>Version</th>
                </tr>
              </thead>
              <tbody>`, shortName, shortName)

	for _, kv := range kvs {
		fmt.Fprintf(w, `
                <tr>
                  <td><code>%s</code></td>
                  <td><code>%s</code></td>
                  <td>%s</td>
                  <td>%d</td>
                </tr>`, kv.Key, string(kv.Value), kv.Type, kv.Version)
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
	
	key := r.FormValue("key")
	if key == "" {
		http.Error(w, "Key is required", http.StatusBadRequest)
		return
	}

	val := r.FormValue("value")
	if val == "" {
		http.Error(w, "Value is required", http.StatusBadRequest)
		return
	}

	glog.Infof("HTTP: Received proposal for key %s with value: %s", key, val)
	if err := s.cell.Propose(r.Context(), key, []byte(val)); err != nil {
		glog.Errorf("HTTP: Proposal failed: %v", err)
		http.Error(w, fmt.Sprintf("Paxos proposal failed: %v", err), http.StatusInternalServerError)
		return
	}
	
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>Synod Agent Proposal</title>
    <link href="/static/bootstrap.min.css" rel="stylesheet">
  </head>
  <body class="bg-light">
    <nav class="navbar navbar-expand-lg navbar-dark bg-dark mb-4">
      <div class="container-fluid">
        <span class="navbar-brand mb-0 h1">Synod Paxos Agent</span>
      </div>
    </nav>
    <div class="container">
      <div class="alert alert-success" role="alert">
        <h4 class="alert-heading">Success!</h4>
        <p>Consensus reached for key <code>%s</code> with value: <code>%s</code>.</p>
        <hr>
        <a href="/" class="btn btn-primary">Back to Status</a>
      </div>
    </div>
  </body>
</html>`, key, val)
}
