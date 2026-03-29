package server

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"net"
	"net/http"
	"net/url"
	"runtime"
	"runtime/pprof"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bazelbuild/rules_go/go/runfiles"
	"github.com/filmil/synod/internal/constants"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	"github.com/filmil/synod/internal/userapi"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

type OngoingRequest struct {
	ID        string
	Type      string
	Key       string
	StartTime time.Time
	Result    string
	Finished  bool
	Success   bool
}

type HTTPServer struct {
	store *state.Store
	cell  *paxos.Cell
	addr  string

	mu              sync.Mutex
	ongoingRequests []OngoingRequest
}

func NewHTTPServer(addr string, store *state.Store, cell *paxos.Cell) *HTTPServer {
	return &HTTPServer{
		addr:            addr,
		store:           store,
		cell:            cell,
		ongoingRequests: []OngoingRequest{},
	}
}

func (s *HTTPServer) addOngoingRequest(reqType, key string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	id := fmt.Sprintf("req-%d", len(s.ongoingRequests))
	s.ongoingRequests = append([]OngoingRequest{{
		ID:        id,
		Type:      reqType,
		Key:       key,
		StartTime: time.Now(),
	}}, s.ongoingRequests...) // Prepend
	return id
}

func (s *HTTPServer) completeOngoingRequest(id string, success bool, result string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i, r := range s.ongoingRequests {
		if r.ID == id {
			s.ongoingRequests[i].Finished = true
			s.ongoingRequests[i].Success = success
			s.ongoingRequests[i].Result = result
			return
		}
	}
}

func (s *HTTPServer) Run(lis net.Listener) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handleIndex)
	mux.HandleFunc("/messages", s.handleMessages)
	mux.HandleFunc("/peers", s.handlePeers)
	mux.HandleFunc("/store", s.handleStore)
	mux.HandleFunc("/system", s.handleSystem)
	mux.HandleFunc("/grpc", s.handleGRPC)
	mux.HandleFunc("/api/command", s.handleCommand)
	mux.HandleFunc("/userapi", s.handleUserAPI)
	mux.HandleFunc("/api/user/read", s.handleUserAPIRead)
	mux.HandleFunc("/api/user/write", s.handleUserAPIWrite)
	mux.HandleFunc("/api/user/lock/acquire", s.handleUserAPILockAcquire)
	mux.HandleFunc("/api/user/lock/release", s.handleUserAPILockRelease)
	mux.HandleFunc("/api/user/lock/renew", s.handleUserAPILockRenew)

	// Serve locally saved bootstrap via runfiles
	mux.HandleFunc("/static/bootstrap.min.css", func(w http.ResponseWriter, r *http.Request) {
		path, err := runfiles.Rlocation("_main/third_party/bootstrap/bootstrap.min.css")
		if err != nil {
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

func (s *HTTPServer) renderHeader(w http.ResponseWriter, title, agentName, activeNav string) {
	w.Header().Set("Content-Type", "text/html")
	fmt.Fprintf(w, `
<!doctype html>
<html lang="en">
  <head>
    <meta charset="utf-8">
    <meta name="viewport" content="width=device-width, initial-scale=1">
    <title>%s - %s</title>
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
            <li class="nav-item"><a class="nav-link %s" href="/">Status</a></li>
            <li class="nav-item"><a class="nav-link %s" href="/store">KV Store</a></li>
            <li class="nav-item"><a class="nav-link %s" href="/messages">Messages</a></li>
            <li class="nav-item"><a class="nav-link %s" href="/peers">Peers</a></li>
            <li class="nav-item"><a class="nav-link %s" href="/userapi">User API</a></li>
            <li class="nav-item"><a class="nav-link %s" href="/system">System</a></li>
            <li class="nav-item"><a class="nav-link %s" href="/grpc">gRPC Status</a></li>
          </ul>
          <div class="dropdown">
            <button class="btn btn-outline-light dropdown-toggle btn-sm" type="button" id="reloadDropdown" data-bs-toggle="dropdown" aria-expanded="false">
              Reload: <span id="reloadLabel">1m</span>
            </button>
            <ul class="dropdown-menu dropdown-menu-end dropdown-menu-dark" aria-labelledby="reloadDropdown">
              <li><a class="dropdown-item" href="#" onclick="setReload(1000)">1s</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(10000)">10s</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(30000)">30s</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(60000)">1m</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(300000)">5m</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(600000)">10m</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(1200000)">20m</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(1800000)">30m</a></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(3600000)">1h</a></li>
              <li><hr class="dropdown-divider"></li>
              <li><a class="dropdown-item" href="#" onclick="setReload(0)">Off</a></li>
            </ul>
          </div>
        </div>
      </div>
    </nav>
    <div class="container">`, title, agentName, agentName,
		s.activeClass(activeNav, "status"),
		s.activeClass(activeNav, "store"),
		s.activeClass(activeNav, "messages"),
		s.activeClass(activeNav, "peers"),
		s.activeClass(activeNav, "userapi"),
		s.activeClass(activeNav, "system"),
		s.activeClass(activeNav, "grpc"))
}

func (s *HTTPServer) activeClass(active, current string) string {
	if active == current {
		return "active"
	}
	return ""
}

func (s *HTTPServer) renderFooter(w http.ResponseWriter) {
	fmt.Fprintf(w, `
    </div>
    <script src="/static/bootstrap.bundle.min.js"></script>
    <script>
      function setReload(ms) {
        localStorage.setItem('reloadInterval', ms);
        location.reload();
      }
      (function() {
        const ms = parseInt(localStorage.getItem('reloadInterval')) ?? 60000;
        const labelMap = {
          1000: '1s', 10000: '10s', 30000: '30s', 60000: '1m',
          300000: '5m', 600000: '10m', 1200000: '20m', 1800000: '30m',
          3600000: '1h', 0: 'Off'
        };
        const label = labelMap[ms] || (ms > 0 ? (ms/1000 + 's') : 'Off');
        document.getElementById('reloadLabel').innerText = label;
        if (ms > 0) {
          setTimeout(() => location.reload(), ms);
        }
      })();
    </script>
  </body>
</html>`)
}

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	agentID, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}

	val, _, _, _, err := s.store.GetKVEntry(constants.PeersKey)
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

	s.renderHeader(w, "Synod Agent", shortName, "status")
	fmt.Fprintf(w, `
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
                  <tbody>`, agentID, shortName, len(kvs))

	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ephemeral := s.cell.GetEphemeralPeers()

	for _, id := range ids {
		label := id
		if id == agentID {
			label = fmt.Sprintf("%s <span class=\"badge bg-secondary\">self</span>", id)
		}
		info := members[id]
		eph := ephemeral[id]

		httpLink := ""
		if eph.HTTPURL != "" {
			httpLink = fmt.Sprintf("<a href=\"%s\" class=\"text-decoration-none\" target=\"_blank\">%s</a>", eph.HTTPURL, eph.HTTPURL)
		} else {
			httpLink = "<span class=\"text-muted\">N/A</span>"
		}
		grpcAddr := eph.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = "unknown"
		}
		fmt.Fprintf(w, "<tr><td><small>%s</small></td><td>%s</td><td><code>%s</code></td><td>%s</td></tr>", label, info.ShortName, grpcAddr, httpLink)
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
      </div>`)
	s.renderFooter(w)
}

func (s *HTTPServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	agentID, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	entries, err := s.store.GetRecentMessages(100)
	if err != nil {
		http.Error(w, "Failed to get recent messages", http.StatusInternalServerError)
		return
	}

	val, _, _, _, err := s.store.GetKVEntry(constants.PeersKey)
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

	s.renderHeader(w, "Messages", shortName, "messages")
	fmt.Fprintf(w, `
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-primary text-white">Peer Mapping</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-hover">
              <thead>
                <tr>
                  <th>Short Name</th><th>Agent ID</th><th>Endpoints</th>
                </tr>
              </thead>
              <tbody>`)

	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ephemeral := s.cell.GetEphemeralPeers()

	for _, id := range ids {
		info := members[id]
		eph := ephemeral[id]
		label := info.ShortName
		if id == agentID {
			label = fmt.Sprintf("%s <span class=\"badge bg-secondary\">self</span>", label)
		}

		grpcAddr := eph.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = "unknown"
		}
		endpoints := fmt.Sprintf("<code>%s</code>", grpcAddr)
		if eph.HTTPURL != "" {
			endpoints += fmt.Sprintf(" | <a href=\"%s\" target=\"_blank\">%s</a>", eph.HTTPURL, eph.HTTPURL)
		}

		fmt.Fprintf(w, `
                <tr>
                  <td>%s</td>
                  <td><small class="text-muted">%s</small></td>
                  <td>%s</td>
                </tr>`, label, id, endpoints)
	}

	fmt.Fprintf(w, `
              </tbody>
            </table>
          </div>
        </div>
      </div>

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
              <tbody>`)

	for _, e := range entries {
		senderName := e.Sender
		if info, ok := members[e.Sender]; ok {
			senderName = info.ShortName
		}
		receiverName := e.Receiver
		if info, ok := members[e.Receiver]; ok {
			receiverName = info.ShortName
		}

		fmt.Fprintf(w, `
                <tr>
                  <td>%d</td>
                  <td><small class="text-nowrap">%s</small></td>
                  <td><span class="badge bg-info text-dark">%s</span></td>
                  <td>%s</td>
                  <td>%s</td>
                  <td>%s</td>
                  <td>%s</td>
                </tr>`, e.ID, e.Timestamp, e.Type, senderName, receiverName, prototextToTable(e.Message), prototextToTable(e.Reply))
	}

	fmt.Fprintf(w, `
              </tbody>
            </table>
          </div>
        </div>
      </div>`)
	s.renderFooter(w)
}

func (s *HTTPServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	agentID, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}

	val, _, _, _, err := s.store.GetKVEntry(constants.PeersKey)
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

	ephemeral := s.cell.GetEphemeralPeers()

	s.renderHeader(w, "Peers", shortName, "peers")
	fmt.Fprintf(w, `
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
              <tbody>`)

	var ids []string
	for id := range members {
		if id != agentID { // Tabulate all *other* known peers
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	for _, id := range ids {
		info := members[id]
		eph := ephemeral[id]

		grpcAddr := eph.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = "unknown"
		}
		grpcURL := fmt.Sprintf("grpc://%s", grpcAddr)

		httpLink := ""
		if eph.HTTPURL != "" {
			httpLink = fmt.Sprintf("<a href=\"%s\" class=\"text-decoration-none\" target=\"_blank\">%s</a>", eph.HTTPURL, eph.HTTPURL)
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
      </div>`)
	s.renderFooter(w)
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

	s.renderHeader(w, "KV Store", shortName, "store")
	fmt.Fprintf(w, `
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
              <tbody>`)

	for _, kv := range kvs {
		fmt.Fprintf(w, `
                <tr>
                  <td><code>%s</code></td>
                  <td>%s</td>
                  <td>%s</td>
                  <td>%d</td>
                </tr>`, kv.Key, maybeJSONToTable(kv.Value), kv.Type, kv.Version)
	}

	fmt.Fprintf(w, `
              </tbody>
            </table>
          </div>
        </div>
      </div>`)
	s.renderFooter(w)
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

	// Wrap in timeout
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	if err := s.cell.Propose(ctx, key, []byte(val), paxos.QuorumMajority); err != nil {
		glog.Errorf("HTTP: Proposal failed: %v", err)
		http.Error(w, fmt.Sprintf("Paxos proposal failed: %v", err), http.StatusInternalServerError)
		return
	}

	_, shortName, _ := s.store.GetAgentID()
	s.renderHeader(w, "Proposal Success", shortName, "")
	fmt.Fprintf(w, `
      <div class="alert alert-success" role="alert">
        <h4 class="alert-heading">Success!</h4>
        <p>Consensus reached for key <code>%s</code> with value: <code>%s</code>.</p>
        <hr>
        <a href="/" class="btn btn-primary">Back to Status</a>
      </div>`, key, val)
	s.renderFooter(w)
}

func (s *HTTPServer) handleUserAPI(w http.ResponseWriter, r *http.Request) {
	_, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	ongoing := make([]OngoingRequest, len(s.ongoingRequests))
	copy(ongoing, s.ongoingRequests)
	s.mu.Unlock()

	s.renderHeader(w, "User API", shortName, "userapi")

	ongoingHTML := ""
	if len(ongoing) > 0 {
		ongoingHTML = `
		<div class="card shadow-sm mb-4">
			<div class="card-header bg-dark text-white">Recently Executed / Ongoing Requests</div>
			<div class="card-body">
				<div class="table-responsive">
					<table class="table table-sm table-hover">
						<thead><tr><th>Time</th><th>Type</th><th>Key</th><th>Status</th><th>Result</th></tr></thead>
						<tbody>`
		for _, r := range ongoing {
			status := "<span class=\"badge bg-secondary\">Ongoing...</span>"
			if r.Finished {
				if r.Success {
					status = "<span class=\"badge bg-success\">Success</span>"
				} else {
					status = "<span class=\"badge bg-danger\">Failed</span>"
				}
			}
			resultText := r.Result
			if len(resultText) > 100 {
				resultText = resultText[:97] + "..."
			}
			ongoingHTML += fmt.Sprintf("<tr><td>%s</td><td>%s</td><td><code>%s</code></td><td>%s</td><td><small>%s</small></td></tr>",
				r.StartTime.Format("15:04:05"), r.Type, html.EscapeString(r.Key), status, html.EscapeString(resultText))
		}
		ongoingHTML += "</tbody></table></div></div></div>"
	}

	// Check for result flash message
	statusMsg := ""
	if r.URL.Query().Get("success") == "true" {
		statusMsg = `<div class="alert alert-success mb-4" role="alert">Operation completed successfully.</div>`
	} else if errMsg := r.URL.Query().Get("error"); errMsg != "" {
		statusMsg = fmt.Sprintf(`<div class="alert alert-danger mb-4" role="alert">Operation failed: %s</div>`, html.EscapeString(errMsg))
	} else if resultMsg := r.URL.Query().Get("result"); resultMsg != "" {
		statusMsg = fmt.Sprintf(`<div class="alert alert-info mb-4" role="alert">Read Result: <code>%s</code></div>`, html.EscapeString(resultMsg))
	}

	fmt.Fprintf(w, `
	  %s
      <div class="row">
        <div class="col-md-4">
          <div class="card shadow-sm mb-4">
            <div class="card-header bg-primary text-white">Read Key</div>
            <div class="card-body">
              <form action="/api/user/read" method="post">
                <div class="mb-3">
                  <label class="form-label">Key path</label>
                  <input type="text" name="key" class="form-control" placeholder="/foo/bar" required>
                </div>
                <div class="mb-3">
                  <label class="form-label">Quorum Requirement</label>
                  <select name="quorum" class="form-select">
                    <option value="LOCAL">Local (Current Peer Only)</option>
                    <option value="MAJORITY" selected>Majority</option>
                    <option value="ALL">All Peers</option>
                  </select>
                </div>
                <button type="submit" class="btn btn-primary">Execute Read</button>
              </form>
            </div>
          </div>
        </div>
        
        <div class="col-md-4">
          <div class="card shadow-sm mb-4">
            <div class="card-header bg-success text-white">Compare And Write</div>
            <div class="card-body">
              <form action="/api/user/write" method="post">
                <div class="mb-3">
                  <label class="form-label">Key path</label>
                  <input type="text" name="key" class="form-control" placeholder="/foo/bar" required>
                </div>
                <div class="mb-3">
                  <label class="form-label">Expected Old Value</label>
                  <input type="text" name="old_value" class="form-control" placeholder="old value">
                </div>
                <div class="mb-3">
                  <label class="form-label">New Value</label>
                  <input type="text" name="new_value" class="form-control" placeholder="new value" required>
                </div>
                <button type="submit" class="btn btn-success">Execute CompareAndWrite</button>
              </form>
            </div>
          </div>
        </div>

        <div class="col-md-4">
          <div class="card shadow-sm mb-4">
            <div class="card-header bg-warning text-dark">Lock Management</div>
            <div class="card-body">
              <h6 class="card-title">Acquire Lock</h6>
              <form action="/api/user/lock/acquire" method="post" class="mb-3">
                <div class="mb-2">
                  <input type="text" name="key_path" class="form-control form-control-sm" placeholder="Key Path (/a/_lockable/b)" required>
                </div>
                <div class="mb-2">
                  <input type="number" name="duration" class="form-control form-control-sm" placeholder="Duration (ms)" required>
                </div>
                <button type="submit" class="btn btn-sm btn-warning w-100">Acquire</button>
              </form>
              
              <hr>
              <h6 class="card-title">Renew Lock</h6>
              <form action="/api/user/lock/renew" method="post" class="mb-3">
                <div class="mb-2">
                  <input type="text" name="key_path" class="form-control form-control-sm" placeholder="Key Path (/a/_lockable/b)" required>
                </div>
                <div class="mb-2">
                  <input type="number" name="duration" class="form-control form-control-sm" placeholder="Duration (ms)" required>
                </div>
                <button type="submit" class="btn btn-sm btn-info w-100">Renew</button>
              </form>
              
              <hr>
              <h6 class="card-title">Release Lock</h6>
              <form action="/api/user/lock/release" method="post">
                <div class="mb-2">
                  <input type="text" name="key_path" class="form-control form-control-sm" placeholder="Key Path (/a/_lockable/b)" required>
                </div>
                <button type="submit" class="btn btn-sm btn-danger w-100">Release</button>
              </form>
            </div>
          </div>
        </div>
      </div>
	  %s
	`, statusMsg, ongoingHTML)

	s.renderFooter(w)
}

func (s *HTTPServer) handleUserAPIRead(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}

	key := r.FormValue("key")
	qStr := r.FormValue("quorum")

	reqID := s.addOngoingRequest("Read ("+qStr+")", key)

	quorum := paxosv1.ReadQuorum_READ_QUORUM_UNSPECIFIED
	switch qStr {
	case "LOCAL":
		quorum = paxosv1.ReadQuorum_READ_QUORUM_LOCAL
	case "MAJORITY":
		quorum = paxosv1.ReadQuorum_READ_QUORUM_MAJORITY
	case "ALL":
		quorum = paxosv1.ReadQuorum_READ_QUORUM_ALL
	}

	// 2 minute timeout
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.Read(ctx, key, quorum)

	if err != nil {
		s.completeOngoingRequest(reqID, false, err.Error())
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	if resp == nil || resp.Value == nil {
		s.completeOngoingRequest(reqID, false, "key not found")
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape("key not found"), http.StatusSeeOther)
		return
	}

	resStr := string(resp.Value)
	s.completeOngoingRequest(reqID, true, resStr)
	http.Redirect(w, r, "/userapi?result="+url.QueryEscape(resStr), http.StatusSeeOther)
}

func (s *HTTPServer) handleUserAPIWrite(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}

	key := r.FormValue("key")
	oldVal := r.FormValue("old_value")
	newVal := r.FormValue("new_value")

	reqID := s.addOngoingRequest("CompareAndWrite", key)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.CompareAndWrite(ctx, key, []byte(oldVal), []byte(newVal), paxos.QuorumMajority)

	if err != nil {
		s.completeOngoingRequest(reqID, false, err.Error())
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	if !resp.Success {
		s.completeOngoingRequest(reqID, false, resp.Message)
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(resp.Message), http.StatusSeeOther)
		return
	}

	s.completeOngoingRequest(reqID, true, "Success (Version "+strconv.FormatUint(resp.Version, 10)+")")
	http.Redirect(w, r, "/userapi?success=true", http.StatusSeeOther)
}

func (s *HTTPServer) handleUserAPILockAcquire(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}
	keyPath := r.FormValue("key_path")
	durationStr := r.FormValue("duration")
	dur, err := strconv.ParseInt(durationStr, 10, 64)
	if err != nil {
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape("invalid duration format"), http.StatusSeeOther)
		return
	}

	reqID := s.addOngoingRequest("AcquireLock", keyPath)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.AcquireLock(ctx, &paxosv1.AcquireLockRequest{KeyPath: keyPath, DurationMs: dur})
	if err != nil {
		s.completeOngoingRequest(reqID, false, err.Error())
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	if !resp.Success {
		s.completeOngoingRequest(reqID, false, resp.Message)
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(resp.Message), http.StatusSeeOther)
		return
	}
	s.completeOngoingRequest(reqID, true, "Lock Acquired")
	http.Redirect(w, r, "/userapi?success=true", http.StatusSeeOther)
}

func (s *HTTPServer) handleUserAPILockRelease(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}
	keyPath := r.FormValue("key_path")

	reqID := s.addOngoingRequest("ReleaseLock", keyPath)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.ReleaseLock(ctx, &paxosv1.ReleaseLockRequest{KeyPath: keyPath})
	if err != nil {
		s.completeOngoingRequest(reqID, false, err.Error())
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	if !resp.Success {
		s.completeOngoingRequest(reqID, false, resp.Message)
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(resp.Message), http.StatusSeeOther)
		return
	}
	s.completeOngoingRequest(reqID, true, "Lock Released")
	http.Redirect(w, r, "/userapi?success=true", http.StatusSeeOther)
}

func (s *HTTPServer) handleUserAPILockRenew(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}
	keyPath := r.FormValue("key_path")
	durationStr := r.FormValue("duration")
	dur, err := strconv.ParseInt(durationStr, 10, 64)
	if err != nil {
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape("invalid duration format"), http.StatusSeeOther)
		return
	}

	reqID := s.addOngoingRequest("RenewLock", keyPath)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.RenewLock(ctx, &paxosv1.RenewLockRequest{KeyPath: keyPath, DurationMs: dur})
	if err != nil {
		s.completeOngoingRequest(reqID, false, err.Error())
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}
	if !resp.Success {
		s.completeOngoingRequest(reqID, false, resp.Message)
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(resp.Message), http.StatusSeeOther)
		return
	}
	s.completeOngoingRequest(reqID, true, "Lock Renewed")
	http.Redirect(w, r, "/userapi?success=true", http.StatusSeeOther)
}

// maybeJSONToTable attempts to parse a string/byte slice as JSON.
// If it's a valid JSON object or array, it renders it as a nested HTML table.
// Otherwise, it returns the content wrapped in standard <pre><code> blocks.
func maybeJSONToTable(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		glog.V(2).Infof("maybeJSONToTable: Not JSON, returning as plain text")
		return fmt.Sprintf(`<pre class="small mb-0" style="max-height: 100px; overflow: auto;"><code>%s</code></pre>`, html.EscapeString(string(data)))
	}

	return renderJSONNode(obj)
}

func renderJSONNode(node interface{}) string {
	switch v := node.(type) {
	case map[string]interface{}:
		if len(v) == 0 {
			return "{}"
		}
		var sb strings.Builder
		sb.WriteString(`<table class="table table-sm table-bordered mb-0" style="font-size: 0.85em; width: auto; max-width: 100%;"><tbody>`)

		var keys []string
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)

		for _, key := range keys {
			val := v[key]
			sb.WriteString(fmt.Sprintf(`<tr><th class="table-light align-top" style="width: 1%%; white-space: nowrap;">%s</th><td>%s</td></tr>`, html.EscapeString(key), renderJSONNode(val)))
		}
		sb.WriteString(`</tbody></table>`)
		return sb.String()
	case []interface{}:
		if len(v) == 0 {
			return "[]"
		}
		var sb strings.Builder
		sb.WriteString(`<table class="table table-sm table-bordered mb-0" style="font-size: 0.85em; width: auto; max-width: 100%;"><tbody>`)
		for i, val := range v {
			sb.WriteString(fmt.Sprintf(`<tr><th class="table-light align-top" style="width: 1%%; white-space: nowrap;">[%d]</th><td>%s</td></tr>`, i, renderJSONNode(val)))
		}
		sb.WriteString(`</tbody></table>`)
		return sb.String()
	case string:
		return html.EscapeString(v)
	case float64:
		return fmt.Sprintf("%v", v)
	case bool:
		return fmt.Sprintf("%t", v)
	case nil:
		return "<em>null</em>"
	default:
		return html.EscapeString(fmt.Sprintf("%v", v))
	}
}

// ptField represents a node in a parsed Protobuf TextFormat AST.
type ptField struct {
	Key      string
	Val      string
	IsBlock  bool
	Children []*ptField
}

func parsePT(lines []string, start int) ([]*ptField, int) {
	var fields []*ptField
	i := start
	for i < len(lines) {
		line := strings.TrimSpace(lines[i])
		if line == "" {
			i++
			continue
		}
		if line == "}" {
			i++
			return fields, i
		}
		parts := strings.SplitN(line, ":", 2)
		if len(parts) == 1 {
			if strings.HasSuffix(line, "{") {
				key := strings.TrimSpace(strings.TrimSuffix(line, "{"))
				var children []*ptField
				children, i = parsePT(lines, i+1)
				fields = append(fields, &ptField{Key: key, IsBlock: true, Children: children})
			} else {
				fields = append(fields, &ptField{Key: "", Val: line})
				i++
			}
		} else {
			key := strings.TrimSpace(parts[0])
			val := strings.TrimSpace(parts[1])
			if strings.HasSuffix(val, "{") {
				key2 := strings.TrimSpace(strings.TrimSuffix(val, "{"))
				if key2 != "" {
					key = key + " " + key2
				}
				var children []*ptField
				children, i = parsePT(lines, i+1)
				fields = append(fields, &ptField{Key: key, IsBlock: true, Children: children})
			} else {
				fields = append(fields, &ptField{Key: key, Val: val})
				i++
			}
		}
	}
	return fields, i
}

func sortPTFields(fields []*ptField) {
	sort.SliceStable(fields, func(i, j int) bool {
		return fields[i].Key < fields[j].Key
	})
	for _, f := range fields {
		if f.IsBlock {
			sortPTFields(f.Children)
		}
	}
}

func renderPTFields(fields []*ptField) string {
	if len(fields) == 0 {
		return ""
	}
	var sb strings.Builder
	sb.WriteString(`<table class="table table-sm table-bordered mb-0" style="font-size: 0.85em; width: auto; max-width: 100%;"><tbody>`)
	for _, f := range fields {
		if f.Key == "" {
			sb.WriteString(fmt.Sprintf(`<tr><td colspan="2">%s</td></tr>`, html.EscapeString(f.Val)))
			continue
		}
		if f.IsBlock {
			sb.WriteString(fmt.Sprintf(`<tr><th class="table-light align-top" style="width: 1%%; white-space: nowrap;">%s</th><td>%s</td></tr>`, html.EscapeString(f.Key), renderPTFields(f.Children)))
		} else {
			// Decode embedded JSON on the 'value' field exclusively
			if f.Key == "value" {
				if unquoted, err := strconv.Unquote(f.Val); err == nil {
					var obj interface{}
					if err := json.Unmarshal([]byte(unquoted), &obj); err == nil {
						sb.WriteString(fmt.Sprintf(`<tr><th class="table-light align-top" style="width: 1%%; white-space: nowrap;">%s</th><td>%s</td></tr>`, html.EscapeString(f.Key), renderJSONNode(obj)))
						continue
					}
				}
			}
			val := f.Val
			if strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`) {
				val = val[1 : len(val)-1]
			}
			sb.WriteString(fmt.Sprintf(`<tr><th class="table-light align-top" style="width: 1%%; white-space: nowrap;">%s</th><td>%s</td></tr>`, html.EscapeString(f.Key), html.EscapeString(val)))
		}
	}
	sb.WriteString(`</tbody></table>`)
	return sb.String()
}

// prototextToTable parses a simple Protobuf text format string into a hierarchical HTML table.
// It explicitly parses the AST structure, orders all fields lexicographically,
// and maps embedded JSON 'value' payloads into tables recursively.
func prototextToTable(text string) string {
	if len(text) == 0 {
		return ""
	}
	lines := strings.Split(text, "\n")
	fields, _ := parsePT(lines, 0)
	sortPTFields(fields)
	return renderPTFields(fields)
}

func (s *HTTPServer) handleSystem(w http.ResponseWriter, r *http.Request) {
	_, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	numGoroutines := runtime.NumGoroutine()
	numCPU := runtime.NumCPU()

	s.renderHeader(w, "System Introspection", shortName, "system")

	// Render runtime stats
	fmt.Fprintf(w, `
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-info text-white">Go Runtime Statistics</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-hover">
              <tbody>
                <tr><th class="w-25">Available CPUs</th><td>%d</td></tr>
                <tr><th>Active Goroutines</th><td>%d</td></tr>
                <tr><th>Allocated Memory (Alloc)</th><td>%d bytes (%.2f MB)</td></tr>
                <tr><th>Total Allocated (TotalAlloc)</th><td>%d bytes (%.2f MB)</td></tr>
                <tr><th>System Memory (Sys)</th><td>%d bytes (%.2f MB)</td></tr>
                <tr><th>Number of GCs</th><td>%d</td></tr>
              </tbody>
            </table>
          </div>
        </div>
      </div>
	`,
		numCPU,
		numGoroutines,
		m.Alloc, float64(m.Alloc)/(1024*1024),
		m.TotalAlloc, float64(m.TotalAlloc)/(1024*1024),
		m.Sys, float64(m.Sys)/(1024*1024),
		m.NumGC)

	// Check if a stack dump was requested
	if r.URL.Query().Get("dump") == "goroutines" {
		fmt.Fprintf(w, `
      <div class="card shadow-sm mb-4">
        <div class="card-header bg-warning text-dark">
          Goroutine Stack Dump
          <a href="/system" class="btn btn-sm btn-outline-dark float-end">Clear Dump</a>
        </div>
        <div class="card-body">
          <pre class="small bg-dark text-light p-3 rounded" style="max-height: 500px; overflow-y: auto;"><code>`)

		pprof.Lookup("goroutine").WriteTo(w, 1)

		fmt.Fprintf(w, `</code></pre>
        </div>
      </div>`)
	} else {
		fmt.Fprintf(w, `
      <div class="card shadow-sm mb-4 border-warning">
        <div class="card-body text-center">
          <p class="mb-3">Need to debug a deadlock or performance issue?</p>
          <a href="/system?dump=goroutines" class="btn btn-warning">Generate Goroutine Stack Dump</a>
        </div>
      </div>`)
	}

	s.renderFooter(w)
}
