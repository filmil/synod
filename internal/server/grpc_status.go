package server

import (
	"context"
	"fmt"
	"html/template"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/grpc/credentials/insecure"
)

func (s *HTTPServer) handleGRPC(w http.ResponseWriter, r *http.Request) {
	agentID, shortName, err := s.store.GetAgentID()
	if err != nil {
		http.Error(w, "Failed to get Agent ID", http.StatusInternalServerError)
		return
	}

	data := GRPCStatusData{
		BaseData: BaseData{
			Title:     "gRPC Introspection",
			AgentName: shortName,
			ActiveNav: "grpc",
		},
	}

	members, err := s.store.GetMembers()
	if err != nil {
		data.ErrorMsg = template.HTML("<div class='alert alert-danger'>Failed to retrieve members</div>")
		w.Header().Set("Content-Type", "text/html")
		templates.ExecuteTemplate(w, "grpc.html", data)
		return
	}

	selfInfo, ok := members[agentID]
	if !ok || selfInfo.GRPCAddr == "" {
		data.ErrorMsg = template.HTML("<div class='alert alert-danger'>gRPC address not found for self</div>")
		w.Header().Set("Content-Type", "text/html")
		templates.ExecuteTemplate(w, "grpc.html", data)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(selfInfo.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		data.ErrorMsg = template.HTML(fmt.Sprintf("<div class='alert alert-danger'>Failed to connect to local gRPC channelz: %v</div>", err))
		w.Header().Set("Content-Type", "text/html")
		templates.ExecuteTemplate(w, "grpc.html", data)
		return
	}
	defer conn.Close()

	client := grpc_channelz_v1.NewChannelzClient(conn)

	// Get Servers
	serversResp, err := client.GetServers(ctx, &grpc_channelz_v1.GetServersRequest{})
	if err != nil {
		data.ErrorMsg = template.HTML(fmt.Sprintf("<div class='alert alert-warning'>Failed to get servers: %v</div>", err))
	}

	if serversResp != nil && len(serversResp.Server) > 0 {
		for _, srv := range serversResp.Server {
			var started, succeeded, failed int64
			var lastCall string
			if srv.Data != nil {
				started = srv.Data.CallsStarted
				succeeded = srv.Data.CallsSucceeded
				failed = srv.Data.CallsFailed
				if srv.Data.LastCallStartedTimestamp != nil && srv.Data.LastCallStartedTimestamp.IsValid() {
					lastCall = srv.Data.LastCallStartedTimestamp.AsTime().Format(time.RFC3339)
				}
			}
			data.Servers = append(data.Servers, GRPCServerData{
				ServerID:       srv.Ref.ServerId,
				CallsStarted:   started,
				CallsSucceeded: succeeded,
				CallsFailed:    failed,
				LastCall:       lastCall,
			})
		}
	}

	// Get Top Channels
	channelsResp, err := client.GetTopChannels(ctx, &grpc_channelz_v1.GetTopChannelsRequest{})
	if err != nil {
		errMsg := fmt.Sprintf("<div class='alert alert-warning'>Failed to get top channels: %v</div>", err)
		if data.ErrorMsg != "" {
			data.ErrorMsg = template.HTML(string(data.ErrorMsg) + errMsg)
		} else {
			data.ErrorMsg = template.HTML(errMsg)
		}
	}

	if channelsResp != nil && len(channelsResp.Channel) > 0 {
		for _, ch := range channelsResp.Channel {
			var state, target string
			var started, succeeded, failed int64
			var chId int64
			if ch.Ref != nil {
				target = ch.Ref.Name
				chId = ch.Ref.ChannelId
			}
			if ch.Data != nil {
				if ch.Data.State != nil {
					state = ch.Data.State.State.String()
				}
				started = ch.Data.CallsStarted
				succeeded = ch.Data.CallsSucceeded
				failed = ch.Data.CallsFailed
			}
			data.Channels = append(data.Channels, GRPCChannelData{
				ChannelID:      chId,
				Target:         target,
				State:          state,
				CallsStarted:   started,
				CallsSucceeded: succeeded,
				CallsFailed:    failed,
			})
		}
	}

	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "grpc.html", data); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}
