// SPDX-License-Identifier: Apache-2.0

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

func formatErrorHTML(format string, args ...any) template.HTML {
	msg := fmt.Sprintf(format, args...)
	return template.HTML(fmt.Sprintf("<div class='alert alert-danger'>%s</div>", template.HTMLEscapeString(msg)))
}

func formatWarningHTML(format string, args ...any) template.HTML {
	msg := fmt.Sprintf(format, args...)
	return template.HTML(fmt.Sprintf("<div class='alert alert-warning'>%s</div>", template.HTMLEscapeString(msg)))
}

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
		data.ErrorMsg = formatErrorHTML("Failed to retrieve members")
		s.renderGRPCStatus(w, data)
		return
	}

	selfInfo, ok := members[agentID]
	if !ok || selfInfo.GRPCAddr == "" {
		data.ErrorMsg = formatErrorHTML("gRPC address not found for self")
		s.renderGRPCStatus(w, data)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	servers, channels, errorMsg := s.fetchChannelzData(ctx, selfInfo.GRPCAddr)
	data.Servers = servers
	data.Channels = channels
	data.ErrorMsg = errorMsg

	s.renderGRPCStatus(w, data)
}

func (s *HTTPServer) fetchChannelzData(ctx context.Context, grpcAddr string) ([]GRPCServerData, []GRPCChannelData, template.HTML) {
	conn, err := grpc.NewClient(grpcAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, nil, formatErrorHTML("Failed to connect to local gRPC channelz: %v", err)
	}
	defer conn.Close()

	client := grpc_channelz_v1.NewChannelzClient(conn)
	var errorMsg template.HTML

	servers, err := s.fetchGRPCServers(ctx, client)
	if err != nil {
		errorMsg = formatWarningHTML("Failed to get servers: %v", err)
	}

	channels, err := s.fetchGRPCChannels(ctx, client)
	if err != nil {
		errMsg := formatWarningHTML("Failed to get top channels: %v", err)
		if errorMsg != "" {
			errorMsg = template.HTML(string(errorMsg) + string(errMsg))
		} else {
			errorMsg = errMsg
		}
	}

	return servers, channels, errorMsg
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
