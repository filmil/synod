package server

import (
	"context"
	"embed"
	"encoding/json"
	"fmt"
	"html"
	"html/template"
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
	"github.com/filmil/synod/internal/identity"
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	"github.com/filmil/synod/internal/userapi"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)

// OngoingRequest tracks the state of a User API request initiated via the web dashboard.
type OngoingRequest struct {
	// ID is a unique identifier for the request.
	ID        string
	// Type indicates the kind of request (e.g., Read, CompareAndWrite).
	Type      string
	// Key is the target key path for the request.
	Key       string
	// StartTime records when the request was initiated.
	StartTime time.Time
	// Result contains the outcome message or value as a string.
	Result    string
	// Finished indicates whether the request has completed.
	Finished  bool
	// Success indicates if a finished request was successful.
	Success   bool
}

//go:embed templates/*.html
var templateFS embed.FS

var templates = template.Must(template.ParseFS(templateFS, "templates/*.html"))

// HTTPServer provides a web dashboard for inspecting agent state and issuing commands.
type BaseData struct {
	Title     string
	AgentName string
	ActiveNav string
}

type Participant struct {
	IDLabel   template.HTML
	ShortName string
	GRPCAddr  string
	HTTPLink  template.HTML
}

type IndexData struct {
	BaseData
	AgentID      template.HTML
	AgentCert    string
	KeysCount    int
	Participants []Participant
}

type Peer struct {
	Label     template.HTML
	AgentID   string
	Endpoints template.HTML
}

type Message struct {
	ID        int64
	Timestamp string
	Type      string
	Sender    string
	Receiver  string
	Request   template.HTML
	Reply     template.HTML
}

type MessagesData struct {
	BaseData
	Peers    []Peer
	Messages []Message
}

type KnownPeer struct {
	ID       string
	Name     string
	GRPCURL  string
	HTTPLink template.HTML
	HasCert  bool
	CertPEM  string
}

type PeersData struct {
	BaseData
	Peers []KnownPeer
}

type KV struct {
	Key     template.HTML
	Value   template.HTML
	Type    string
	Version uint64
}

type StoreData struct {
	BaseData
	KVs []KV
}

type CommandData struct {
	BaseData
	Key   string
	Value string
}

type UserAPIData struct {
	BaseData
	StatusMsg   template.HTML
	OngoingHTML template.HTML
}

type SystemData struct {
	BaseData
	NumCPU        int
	NumGoroutines int
	Alloc         uint64
	AllocMB       float64
	TotalAlloc    uint64
	TotalAllocMB  float64
	Sys           uint64
	SysMB         float64
	NumGC         uint32
	ShowDump      bool
	Dump          string
}

type GRPCServerData struct {
	ServerID       int64
	CallsStarted   int64
	CallsSucceeded int64
	CallsFailed    int64
	LastCall       string
}

type GRPCChannelData struct {
	ChannelID      int64
	Target         string
	State          string
	CallsStarted   int64
	CallsSucceeded int64
	CallsFailed    int64
}

type GRPCStatusData struct {
	BaseData
	ErrorMsg template.HTML
	Servers  []GRPCServerData
	Channels []GRPCChannelData
}

const (
	jsonMapTmpl = `
<table class="table table-sm table-bordered mb-0" style="font-size: 0.85em; width: auto; max-width: 100%;">
  <tbody>
    {{range .}}
    <tr>
      <th class="table-light align-top" style="width: 1%; white-space: nowrap;">{{.Key}}</th>
      <td>{{.Value}}</td>
    </tr>
    {{end}}
  </tbody>
</table>`

	jsonArrayTmpl = `
<table class="table table-sm table-bordered mb-0" style="font-size: 0.85em; width: auto; max-width: 100%;">
  <tbody>
    {{range .}}
    <tr>
      <th class="table-light align-top" style="width: 1%; white-space: nowrap;">[{{.Index}}]</th>
      <td>{{.Value}}</td>
    </tr>
    {{end}}
  </tbody>
</table>`

	wrappedValueTmpl = `<div style="white-space: pre-wrap; font-family: monospace; word-break: break-all;">{{.}}</div>`

	ptTableTmpl = `
<table class="table table-sm table-bordered mb-0" style="font-size: 0.85em; width: auto; max-width: 100%;">
  <tbody>
    {{range .}}
    <tr>
      {{if .Key}}
      <th class="table-light align-top" style="width: 1%; white-space: nowrap;">{{.Key}}</th>
      <td>{{.Value}}</td>
      {{else}}
      <td colspan="2">{{.Value}}</td>
      {{end}}
    </tr>
    {{end}}
  </tbody>
</table>`

	ongoingRequestsTmpl = `
<div class="card shadow-sm mb-4">
  <div class="card-header bg-dark text-white">Recently Executed / Ongoing Requests</div>
  <div class="card-body">
    <div class="table-responsive">
      <table class="table table-sm table-hover">
        <thead><tr><th>Time</th><th>Type</th><th>Key</th><th>Status</th><th>Result</th></tr></thead>
        <tbody>
          {{range .}}
          <tr>
            <td>{{.Time}}</td>
            <td>{{.Type}}</td>
            <td><code>{{.Key}}</code></td>
            <td>{{.StatusHTML}}</td>
            <td><small>{{.Result}}</small></td>
          </tr>
          {{end}}
        </tbody>
      </table>
    </div>
  </div>
</div>`

	prefixResultsTmpl = `
<div class="table-responsive">
  <table class="table table-sm table-bordered bg-white mt-2 mb-0" style="font-size: 0.85em;">
    <thead><tr><th>Key</th><th>Value</th></tr></thead>
    <tbody>
      {{range .}}
      <tr><td><code>{{.Key}}</code></td><td>{{.Value}}</td></tr>
      {{end}}
    </tbody>
  </table>
</div>`

	fallbackPlainTmpl = `<pre class="small mb-0" style="max-height: 100px; overflow: auto;"><code>{{.}}</code></pre>`
)

// HTTPServer provides a web dashboard for inspecting agent state and issuing commands.
type HTTPServer struct {
	store *state.Store
	cell  *paxos.Cell
	addr  string

	mu              sync.Mutex
	ongoingRequests []OngoingRequest
}

// NewHTTPServer initializes a new HTTPServer.
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

// Run starts the HTTP server on the provided listener.
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
	mux.HandleFunc("/api/user/readprefix", s.handleUserAPIReadPrefix)
	mux.HandleFunc("/api/user/write", s.handleUserAPIWrite)
	mux.HandleFunc("/api/user/lock/acquire", s.handleUserAPILockAcquire)
	mux.HandleFunc("/api/user/lock/release", s.handleUserAPILockRelease)
	mux.HandleFunc("/api/user/lock/renew", s.handleUserAPILockRenew)
	mux.HandleFunc("/api/user/shutdown", s.handleUserAPIShutdown)

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

func (s *HTTPServer) handleIndex(w http.ResponseWriter, r *http.Request) {
	agentID, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	shortName, err := s.store.GetShortName()
	if err != nil {
		http.Error(w, "Failed to get Short Name", http.StatusInternalServerError)
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

	data := IndexData{
		BaseData: BaseData{
			Title:     "Status",
			AgentName: shortName,
			ActiveNav: "status",
		},
		AgentID:   template.HTML(fmt.Sprintf(`<div style="white-space: pre-wrap; font-family: monospace; word-break: break-all;">%s</div>`, html.EscapeString(wrapString(agentID, 40)))),
		KeysCount: len(kvs),
	}

	if ident, err := s.store.GetIdentity(""); err == nil {
		data.AgentCert = string(identity.MarshalCertificate(ident.Certificate))
	}


	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	for _, id := range ids {
		idWrapped := fmt.Sprintf(`<div style="white-space: pre-wrap; font-family: monospace; word-break: break-all;">%s</div>`, html.EscapeString(wrapString(id, 40)))
		label := idWrapped
		if id == agentID {
			label = fmt.Sprintf("%s <span class=\"badge bg-secondary\">self</span>", idWrapped)
		}
		info := members[id]

		httpLink := ""
		if info.HTTPURL != "" {
			httpLink = fmt.Sprintf("<a href=\"%s\" class=\"text-decoration-none\" target=\"_blank\">%s</a>", info.HTTPURL, info.HTTPURL)
		} else {
			httpLink = "<span class=\"text-muted\">N/A</span>"
		}
		grpcAddr := info.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = "unknown"
		}
		data.Participants = append(data.Participants, Participant{
			IDLabel:   template.HTML(label),
			ShortName: info.ShortName,
			GRPCAddr:  grpcAddr,
			HTTPLink:  template.HTML(httpLink),
		})
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "index.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) handleMessages(w http.ResponseWriter, r *http.Request) {
	agentID, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	shortName, err := s.store.GetShortName()
	if err != nil {
		http.Error(w, "Failed to get Short Name", http.StatusInternalServerError)
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

	data := MessagesData{
		BaseData: BaseData{
			Title:     "Messages",
			AgentName: shortName,
			ActiveNav: "messages",
		},
	}

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

		grpcAddr := info.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = "unknown"
		}
		endpoints := fmt.Sprintf("<code>%s</code>", grpcAddr)
		if info.HTTPURL != "" {
			endpoints += fmt.Sprintf(" | <a href=\"%s\" target=\"_blank\">%s</a>", info.HTTPURL, info.HTTPURL)
		}

		data.Peers = append(data.Peers, Peer{
			Label:     template.HTML(label),
			AgentID:   id,
			Endpoints: template.HTML(endpoints),
		})
	}

	for _, e := range entries {
		senderName := e.Sender
		if info, ok := members[e.Sender]; ok {
			senderName = info.ShortName
		}
		receiverName := e.Receiver
		if info, ok := members[e.Receiver]; ok {
			receiverName = info.ShortName
		}

		data.Messages = append(data.Messages, Message{
			ID:        e.ID,
			Timestamp: e.Timestamp,
			Type:      e.Type,
			Sender:    senderName,
			Receiver:  receiverName,
			Request:   template.HTML(prototextToTable(e.Message)),
			Reply:     template.HTML(prototextToTable(e.Reply)),
		})
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "messages.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) handlePeers(w http.ResponseWriter, r *http.Request) {
	agentID, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}
	shortName, err := s.store.GetShortName()
	if err != nil {
		http.Error(w, "Failed to get Short Name", http.StatusInternalServerError)
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

	data := PeersData{
		BaseData: BaseData{
			Title:     "Peers",
			AgentName: shortName,
			ActiveNav: "peers",
		},
	}

	var ids []string
	for id := range members {
		if id != agentID { // Tabulate all *other* known peers
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	for _, id := range ids {
		info := members[id]

		grpcAddr := info.GRPCAddr
		if grpcAddr == "" {
			grpcAddr = "unknown"
		}
		grpcURL := fmt.Sprintf("grpc://%s", grpcAddr)

		httpLink := ""
		if info.HTTPURL != "" {
			httpLink = fmt.Sprintf("<a href=\"%s\" class=\"text-decoration-none\" target=\"_blank\">%s</a>", info.HTTPURL, info.HTTPURL)
		} else {
			httpLink = "<span class=\"text-muted\">N/A</span>"
		}
		certPEM := ""
		if len(info.Certificate) > 0 {
			if cert, err := identity.UnmarshalCertificate(info.Certificate); err == nil {
				certPEM = string(identity.MarshalCertificate(cert))
			}
		}

		data.Peers = append(data.Peers, KnownPeer{
			ID:       id,
			Name:     info.ShortName,
			GRPCURL:  grpcURL,
			HTTPLink: template.HTML(httpLink),
			HasCert:  len(info.Certificate) > 0,
			CertPEM:  certPEM,
		})
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "peers.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) handleStore(w http.ResponseWriter, r *http.Request) {
	shortName, err := s.store.GetShortName()
	if err != nil {
		http.Error(w, "Failed to get Short Name", http.StatusInternalServerError)
		return
	}
	kvs, err := s.store.GetAllKVs()
	if err != nil {
		http.Error(w, "Failed to get KV store", http.StatusInternalServerError)
		return
	}

	data := StoreData{
		BaseData: BaseData{
			Title:     "KV Store",
			AgentName: shortName,
			ActiveNav: "store",
		},
	}

	for _, kv := range kvs {
		data.KVs = append(data.KVs, KV{
			Key:     template.HTML(fmt.Sprintf(`<div style="white-space: pre-wrap; font-family: monospace; word-break: break-all;">%s</div>`, html.EscapeString(wrapString(kv.Key, 40)))),
			Value:   template.HTML(maybeJSONToTable(kv.Value)),
			Type:    kv.Type,
			Version: kv.Version,
		})
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "store.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
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

	shortName, _ := s.store.GetShortName()

	data := CommandData{
		BaseData: BaseData{
			Title:     "Proposal Success",
			AgentName: shortName,
			ActiveNav: "",
		},
		Key:   key,
		Value: val,
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "command.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) handleUserAPI(w http.ResponseWriter, r *http.Request) {
	shortName, err := s.store.GetShortName()
	if err != nil {
		http.Error(w, "Failed to get Short Name", http.StatusInternalServerError)
		return
	}

	s.mu.Lock()
	ongoing := make([]OngoingRequest, len(s.ongoingRequests))
	copy(ongoing, s.ongoingRequests)
	s.mu.Unlock()

	data := UserAPIData{
		BaseData: BaseData{
			Title:     "User API",
			AgentName: shortName,
			ActiveNav: "userapi",
		},
	}

	ongoingHTML := ""
	if len(ongoing) > 0 {
		type renderEntry struct {
			Time       string
			Type       string
			Key        string
			StatusHTML template.HTML
			Result     string
		}
		var entries []renderEntry
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
			entries = append(entries, renderEntry{
				Time:       r.StartTime.Format("15:04:05"),
				Type:       r.Type,
				Key:        r.Key,
				StatusHTML: template.HTML(status),
				Result:     resultText,
			})
		}
		var sb strings.Builder
		t := template.Must(template.New("ongoing").Parse(ongoingRequestsTmpl))
		t.Execute(&sb, entries)
		ongoingHTML = sb.String()
	}
	data.OngoingHTML = template.HTML(ongoingHTML)

	// Check for result flash message
	statusMsg := ""
	if r.URL.Query().Get("success") == "true" {
		statusMsg = `<div class="alert alert-success mb-4" role="alert">Operation completed successfully.</div>`
	} else if errMsg := r.URL.Query().Get("error"); errMsg != "" {
		statusMsg = fmt.Sprintf(`<div class="alert alert-danger mb-4" role="alert">Operation failed: %s</div>`, html.EscapeString(errMsg))
	} else if resultMsg := r.URL.Query().Get("result"); resultMsg != "" {
		statusMsg = fmt.Sprintf(`<div class="alert alert-info mb-4" role="alert">Read Result: <code>%s</code></div>`, html.EscapeString(resultMsg))
	} else if prefixResult := r.URL.Query().Get("prefix_result"); prefixResult != "" {
		statusMsg = fmt.Sprintf(`<div class="alert alert-info mb-4" role="alert"><h6 class="alert-heading">Prefix Read Result:</h6>%s</div>`, html.EscapeString(prefixResult))
	} else if prefixResultsJSON := r.URL.Query().Get("prefix_results"); prefixResultsJSON != "" {
		type entry struct {
			Key   string
			Value string
		}
		var resEntries []entry
		if err := json.Unmarshal([]byte(prefixResultsJSON), &resEntries); err == nil {
			var sb strings.Builder
			t := template.Must(template.New("prefixRes").Parse(prefixResultsTmpl))
			t.Execute(&sb, resEntries)
			statusMsg = fmt.Sprintf(`<div class="alert alert-info mb-4" role="alert"><h6 class="alert-heading">Prefix Read Result:</h6>%s</div>`, sb.String())
		} else {
			glog.Errorf("Failed to unmarshal prefix_results JSON: %v", err)
			statusMsg = `<div class="alert alert-danger mb-4" role="alert">Failed to display prefix read results.</div>`
		}
	}
	data.StatusMsg = template.HTML(statusMsg)

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "userapi.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
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

func (s *HTTPServer) handleUserAPIReadPrefix(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}

	prefix := r.FormValue("prefix")
	reqID := s.addOngoingRequest("ReadPrefix", prefix)

	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.ReadPrefix(ctx, prefix)

	if err != nil {
		s.completeOngoingRequest(reqID, false, err.Error())
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape(err.Error()), http.StatusSeeOther)
		return
	}

	if len(resp.Entries) == 0 {
		s.completeOngoingRequest(reqID, true, "no keys found")
		http.Redirect(w, r, "/userapi?prefix_result="+url.QueryEscape("No keys found with this prefix."), http.StatusSeeOther)
		return
	}

	// Format results as a small table
	type entry struct {
		Key   string
		Value string
	}
	var resEntries []entry
	for _, e := range resp.Entries {
		resEntries = append(resEntries, entry{
			Key:   e.Key,
			Value: string(e.Value),
		})
	}

	resJSON, err := json.Marshal(resEntries)
	if err != nil {
		s.completeOngoingRequest(reqID, false, "failed to marshal results")
		http.Redirect(w, r, "/userapi?error="+url.QueryEscape("failed to marshal results"), http.StatusSeeOther)
		return
	}

	s.completeOngoingRequest(reqID, true, fmt.Sprintf("found %d entries", len(resp.Entries)))
	http.Redirect(w, r, "/userapi?prefix_results="+url.QueryEscape(string(resJSON)), http.StatusSeeOther)
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
		var sb strings.Builder
		t := template.Must(template.New("plain").Parse(fallbackPlainTmpl))
		t.Execute(&sb, string(data))
		return sb.String()
	}

	return renderJSONNode(obj, "")
}

func wrapString(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	var sb strings.Builder
	for i, r := range s {
		if i > 0 && i%limit == 0 {
			sb.WriteRune('\n')
		}
		sb.WriteRune(r)
	}
	return sb.String()
}

func renderWrapped(s string, limit int) template.HTML {
	t := template.Must(template.New("wrapped").Parse(wrappedValueTmpl))
	var sb strings.Builder
	t.Execute(&sb, wrapString(s, limit))
	return template.HTML(sb.String())
}

func renderJSONNode(node interface{}, keyName string) string {
	var sb strings.Builder
	switch v := node.(type) {
	case map[string]interface{}:
		if len(v) == 0 {
			return "{}"
		}

		type entry struct {
			Key   string
			Value template.HTML
		}
		var entries []entry
		var keys []string
		for k := range v {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		for _, k := range keys {
			entries = append(entries, entry{
				Key:   k,
				Value: template.HTML(renderJSONNode(v[k], k)),
			})
		}
		t := template.Must(template.New("jsonMap").Parse(jsonMapTmpl))
		t.Execute(&sb, entries)
		return sb.String()

	case []interface{}:
		if len(v) == 0 {
			return "[]"
		}
		type entry struct {
			Index int
			Value template.HTML
		}
		var entries []entry
		for i, val := range v {
			entries = append(entries, entry{
				Index: i,
				Value: template.HTML(renderJSONNode(val, "")),
			})
		}
		t := template.Must(template.New("jsonArray").Parse(jsonArrayTmpl))
		t.Execute(&sb, entries)
		return sb.String()

	case string:
		if strings.ToLower(keyName) == "certificate" {
			return string(renderWrapped(v, 40))
		}
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

	type entry struct {
		Key     string
		Value   template.HTML
		IsBlock bool
	}
	var entries []entry
	for _, f := range fields {
		e := entry{
			Key:     f.Key,
			IsBlock: f.IsBlock,
		}
		if f.IsBlock {
			e.Value = template.HTML(renderPTFields(f.Children))
		} else if f.Key == "value" {
			// Decode embedded JSON on the 'value' field exclusively
			if unquoted, err := strconv.Unquote(f.Val); err == nil {
				var obj interface{}
				if err := json.Unmarshal([]byte(unquoted), &obj); err == nil {
					e.Value = template.HTML(renderJSONNode(obj, f.Key))
				} else {
					e.Value = template.HTML(html.EscapeString(unquoted))
				}
			} else {
				e.Value = template.HTML(html.EscapeString(f.Val))
			}
		} else {
			val := f.Val
			if strings.HasPrefix(val, `"`) && strings.HasSuffix(val, `"`) {
				val = val[1 : len(val)-1]
			}
			e.Value = template.HTML(html.EscapeString(val))
		}
		entries = append(entries, e)
	}

	var sb strings.Builder
	t := template.Must(template.New("ptTable").Parse(ptTableTmpl))
	t.Execute(&sb, entries)
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
	shortName, err := s.store.GetShortName()
	if err != nil {
		http.Error(w, "Failed to get Short Name", http.StatusInternalServerError)
		return
	}

	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	numGoroutines := runtime.NumGoroutine()
	numCPU := runtime.NumCPU()

	data := SystemData{
		BaseData: BaseData{
			Title:     "System Introspection",
			AgentName: shortName,
			ActiveNav: "system",
		},
		NumCPU:        numCPU,
		NumGoroutines: numGoroutines,
		Alloc:         m.Alloc,
		AllocMB:       float64(m.Alloc) / (1024 * 1024),
		TotalAlloc:    m.TotalAlloc,
		TotalAllocMB:  float64(m.TotalAlloc) / (1024 * 1024),
		Sys:           m.Sys,
		SysMB:         float64(m.Sys) / (1024 * 1024),
		NumGC:         m.NumGC,
	}

	if r.URL.Query().Get("dump") == "goroutines" {
		data.ShowDump = true

		// Capture stack dump
		var sb strings.Builder
		pprof.Lookup("goroutine").WriteTo(&sb, 1)
		data.Dump = sb.String()
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "system.html", data); err != nil {
		glog.Errorf("Template execution failed: %v", err)
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) handleUserAPIShutdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Redirect(w, r, "/userapi", http.StatusSeeOther)
		return
	}

	reqID := s.addOngoingRequest("Shutdown", "node")
	ctx, cancel := context.WithTimeout(r.Context(), 2*time.Minute)
	defer cancel()

	userAPI := userapi.New(s.cell)
	resp, err := userAPI.Shutdown(ctx, &paxosv1.ShutdownRequest{})

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

	s.completeOngoingRequest(reqID, true, resp.Message)
	http.Redirect(w, r, "/userapi?success=true", http.StatusSeeOther)
}
