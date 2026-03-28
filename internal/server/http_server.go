package server

import (
	"encoding/json"
	"fmt"
	"html"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"

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
		s.activeClass(activeNav, "peers"))
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

	for _, id := range ids {
		info := members[id]
		label := info.ShortName
		if id == agentID {
			label = fmt.Sprintf("%s <span class=\"badge bg-secondary\">self</span>", label)
		}

		endpoints := fmt.Sprintf("<code>%s</code>", info.GRPCAddr)
		if info.HTTPURL != "" {
			endpoints += fmt.Sprintf(" | <a href=\"%s\" target=\"_blank\">%s</a>", info.HTTPURL, info.HTTPURL)
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
	if err := s.cell.Propose(r.Context(), key, []byte(val)); err != nil {
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

// maybeJSONToTable attempts to parse a string/byte slice as JSON.
// If it's a valid JSON object or array, it renders it as a nested HTML table.
// Otherwise, it returns the content wrapped in standard <pre><code> blocks.
func maybeJSONToTable(data []byte) string {
	if len(data) == 0 {
		return ""
	}

	var obj interface{}
	if err := json.Unmarshal(data, &obj); err != nil {
		// Not JSON, return as plain text
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
