// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"html"
	"html/template"
	"net/http"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/channelz/grpc_channelz_v1"
	"google.golang.org/grpc/credentials/insecure"
	"html"
)

func (s *HTTPServer) handleGRPC(w http.ResponseWriter, r *http.Request) {
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
		s.renderGRPCStatus(w, data)
		return
	}

	selfInfo, ok := members[agentID]
	if !ok || selfInfo.GRPCAddr == "" {
		data.ErrorMsg = template.HTML("<div class='alert alert-danger'>gRPC address not found for self</div>")
		s.renderGRPCStatus(w, data)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(selfInfo.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		data.ErrorMsg = template.HTML(fmt.Sprintf("<div class='alert alert-danger'>Failed to connect to local gRPC channelz: %v</div>", html.EscapeString(err.Error())))
		s.renderGRPCStatus(w, data)
		return
	}
	defer conn.Close()

	client := grpc_channelz_v1.NewChannelzClient(conn)

	// Get Servers
	servers, err := s.fetchGRPCServers(ctx, client)
	if err != nil {
		data.ErrorMsg = template.HTML(fmt.Sprintf("<div class='alert alert-warning'>Failed to get servers: %v</div>", html.EscapeString(err.Error())))
	}
	data.Servers = servers

	// Get Top Channels
	channels, err := s.fetchGRPCChannels(ctx, client)
	if err != nil {
		errMsg := fmt.Sprintf("<div class='alert alert-warning'>Failed to get top channels: %v</div>", html.EscapeString(err.Error()))
		if data.ErrorMsg != "" {
			data.ErrorMsg = template.HTML(string(data.ErrorMsg) + errMsg)
		} else {
			data.ErrorMsg = template.HTML(errMsg)
		}
	}
	data.Channels = channels

	s.renderGRPCStatus(w, data)
}

func (s *HTTPServer) renderGRPCStatus(w http.ResponseWriter, data GRPCStatusData) {
	w.Header().Set("Content-Type", "text/html")
	if err := templates.ExecuteTemplate(w, "grpc.html", data); err != nil {
		http.Error(w, "Internal Server Error", http.StatusInternalServerError)
	}
}

func (s *HTTPServer) fetchGRPCServers(ctx context.Context, client grpc_channelz_v1.ChannelzClient) ([]GRPCServerData, error) {
	resp, err := client.GetServers(ctx, &grpc_channelz_v1.GetServersRequest{})
	if err != nil {
		return nil, err
	}

	var servers []GRPCServerData
	if resp != nil {
		for _, srv := range resp.Server {
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
			servers = append(servers, GRPCServerData{
				ServerID:       srv.Ref.ServerId,
				CallsStarted:   started,
				CallsSucceeded: succeeded,
				CallsFailed:    failed,
				LastCall:       lastCall,
			})
		}
	}
	return servers, nil
}

func (s *HTTPServer) fetchGRPCChannels(ctx context.Context, client grpc_channelz_v1.ChannelzClient) ([]GRPCChannelData, error) {
	resp, err := client.GetTopChannels(ctx, &grpc_channelz_v1.GetTopChannelsRequest{})
	if err != nil {
		return nil, err
	}

	var channels []GRPCChannelData
	if resp != nil {
		for _, ch := range resp.Channel {
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
			channels = append(channels, GRPCChannelData{
				ChannelID:      chId,
				Target:         target,
				State:          state,
				CallsStarted:   started,
				CallsSucceeded: succeeded,
				CallsFailed:    failed,
			})
		}
	}
	return channels, nil
}
