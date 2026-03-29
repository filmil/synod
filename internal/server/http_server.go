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
	"github.com/filmil/synod/internal/paxos"
	"github.com/filmil/synod/internal/state"
	"github.com/filmil/synod/internal/userapi"
	paxosv1 "github.com/filmil/synod/proto/paxos/v1"
	"github.com/golang/glog"
)


type HeaderData struct {
	Title     string
	AgentName string
	ActiveNav string
}

type ParticipantData struct {
	Label     template.HTML
	ShortName string
	GRPCAddr  string
	HTTPLink  template.HTML
}

type IndexData struct {
	HeaderData
	AgentID      string
	KeysCount    int
	Participants []ParticipantData
}

type PeerData struct {
	Label     template.HTML
	ID        string
	Name      string
	Endpoints template.HTML
	GRPCURL   string
	HTTPLink  template.HTML
}

type MessageData struct {
	ID       uint64
	Time     string
	Type     string
	Sender   string
	Receiver string
	Request  template.HTML
	Reply    template.HTML
}

type MessagesData struct {
	HeaderData
	Peers    []PeerData
	Messages []MessageData
}

type PeersData struct {
	HeaderData
	Peers []PeerData
}

type KVData struct {
	Key     string
	Value   template.HTML
	Type    string
	Version uint64
}

type StoreData struct {
	HeaderData
	KVs []KVData
}

type CommandData struct {
	HeaderData
	Key   string
	Value string
}

type UserAPIData struct {
	HeaderData
	HasOngoing bool
	Ongoing    []OngoingRequestTemplate
	HasSuccess bool
	ErrorMsg   string
	ResultMsg  string
}

type OngoingRequestTemplate struct {
	Time   string
	Type   string
	Key    string
	Status template.HTML
	Result string
}

type SystemData struct {
	HeaderData
	NumCPU         int
	NumGoroutines  int
	Alloc          uint64
	AllocMB        float64
	TotalAlloc     uint64
	TotalAllocMB   float64
	Sys            uint64
	SysMB          float64
	NumGC          uint32
	DumpGoroutines bool
	Goroutines     string
}

type ServerData struct {
	ServerID  int64
	Started   int64
	Succeeded int64
	Failed    int64
	LastCall  string
}

type ChannelData struct {
	ChannelID int64
	Target    string
	State     string
	Started   int64
	Succeeded int64
	Failed    int64
}

type GRPCData struct {
	HeaderData
	SelfError     bool
	ConnectError  error
	ServersError  error
	ChannelsError error
	Servers       []ServerData
	Channels      []ChannelData
}


type OngoingRequest struct {
	ID        string
	Type      string
	Key       string
	StartTime time.Time
	Result    string
	Finished  bool
	Success   bool
}

//go:embed templates/*.html
var templatesFS embed.FS

type HTTPServer struct {
	store *state.Store
	cell  *paxos.Cell
	addr  string

	mu              sync.Mutex
	ongoingRequests []OngoingRequest

	templates *template.Template
}

func NewHTTPServer(addr string, store *state.Store, cell *paxos.Cell) *HTTPServer {
	return &HTTPServer{
		addr:            addr,
		store:           store,
		cell:            cell,
		ongoingRequests: []OngoingRequest{},
		templates:       template.Must(template.ParseFS(templatesFS, "templates/*.html")),
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


	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ephemeral := s.cell.GetEphemeralPeers()

	data := IndexData{
		HeaderData: HeaderData{
			Title:     "Synod Agent",
			AgentName: shortName,
			ActiveNav: "status",
		},
		AgentID:      agentID,
		KeysCount:    len(kvs),
		Participants: []ParticipantData{},
	}

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
		data.Participants = append(data.Participants, ParticipantData{
			Label:     template.HTML(label),
			ShortName: info.ShortName,
			GRPCAddr:  grpcAddr,
			HTTPLink:  template.HTML(httpLink),
		})
	}

	s.templates.ExecuteTemplate(w, "index.html", data)
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


	var ids []string
	for id := range members {
		ids = append(ids, id)
	}
	sort.Strings(ids)

	ephemeral := s.cell.GetEphemeralPeers()

	data := MessagesData{
		HeaderData: HeaderData{
			Title:     "Messages",
			AgentName: shortName,
			ActiveNav: "messages",
		},
		Peers:    []PeerData{},
		Messages: []MessageData{},
	}

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
		data.Peers = append(data.Peers, PeerData{
			Label:     template.HTML(label),
			ID:        id,
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
		data.Messages = append(data.Messages, MessageData{
			ID:       uint64(e.ID),
			Time:     e.Timestamp,
			Type:     e.Type,
			Sender:   senderName,
			Receiver: receiverName,
			Request:  template.HTML(prototextToTable(e.Message)),
			Reply:    template.HTML(prototextToTable(e.Reply)),
		})
	}

	s.templates.ExecuteTemplate(w, "messages.html", data)
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


	var ids []string
	for id := range members {
		if id != agentID { // Tabulate all *other* known peers
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)


	data := PeersData{
		HeaderData: HeaderData{
			Title:     "Peers",
			AgentName: shortName,
			ActiveNav: "peers",
		},
		Peers: []PeerData{},
	}

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

		data.Peers = append(data.Peers, PeerData{
			ID:       id,
			Name:     info.ShortName,
			GRPCURL:  grpcURL,
			HTTPLink: template.HTML(httpLink),
		})
	}
	s.templates.ExecuteTemplate(w, "peers.html", data)
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

	data := StoreData{
		HeaderData: HeaderData{
			Title:     "KV Store",
			AgentName: shortName,
			ActiveNav: "store",
		},
		KVs: []KVData{},
	}

	for _, kv := range kvs {
		data.KVs = append(data.KVs, KVData{
			Key:     kv.Key,
			Value:   template.HTML(maybeJSONToTable(kv.Value)),
			Type:    kv.Type,
			Version: kv.Version,
		})
	}
	s.templates.ExecuteTemplate(w, "store.html", data)
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
	data := CommandData{
		HeaderData: HeaderData{
			Title:     "Proposal Success",
			AgentName: shortName,
			ActiveNav: "",
		},
		Key:   key,
		Value: val,
	}
	s.templates.ExecuteTemplate(w, "command.html", data)
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


	hasSuccess := r.URL.Query().Get("success") == "true"
	errorMsg := r.URL.Query().Get("error")
	resultMsg := r.URL.Query().Get("result")

	data := UserAPIData{
		HeaderData: HeaderData{
			Title:     "User API",
			AgentName: shortName,
			ActiveNav: "userapi",
		},
		HasOngoing: len(ongoing) > 0,
		Ongoing:    []OngoingRequestTemplate{},
		HasSuccess: hasSuccess,
		ErrorMsg:   errorMsg,
		ResultMsg:  resultMsg,
	}

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
		data.Ongoing = append(data.Ongoing, OngoingRequestTemplate{
			Time:   r.StartTime.Format("15:04:05"),
			Type:   r.Type,
			Key:    r.Key,
			Status: template.HTML(status),
			Result: resultText,
		})
	}

	s.templates.ExecuteTemplate(w, "userapi.html", data)
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

	data := SystemData{
		HeaderData: HeaderData{
			Title:     "System Introspection",
			AgentName: shortName,
			ActiveNav: "system",
		},
		NumCPU:         numCPU,
		NumGoroutines:  numGoroutines,
		Alloc:          m.Alloc,
		AllocMB:        float64(m.Alloc) / (1024 * 1024),
		TotalAlloc:     m.TotalAlloc,
		TotalAllocMB:   float64(m.TotalAlloc) / (1024 * 1024),
		Sys:            m.Sys,
		SysMB:          float64(m.Sys) / (1024 * 1024),
		NumGC:          m.NumGC,
		DumpGoroutines: false,
	}

	if r.URL.Query().Get("dump") == "goroutines" {
		data.DumpGoroutines = true
		var sb strings.Builder
		pprof.Lookup("goroutine").WriteTo(&sb, 1)
		data.Goroutines = sb.String()
	}

	s.templates.ExecuteTemplate(w, "system.html", data)
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
