package server

import (
	"context"
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

	data := GRPCData{
		HeaderData: HeaderData{
			Title:     "gRPC Introspection",
			AgentName: shortName,
			ActiveNav: "grpc",
		},
	}

	eph, ok := s.cell.GetEphemeralPeer(agentID)
	if !ok || eph.GRPCAddr == "" {
		data.SelfError = true
		s.templates.ExecuteTemplate(w, "grpc.html", data)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(eph.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		data.ConnectError = err
		s.templates.ExecuteTemplate(w, "grpc.html", data)
		return
	}
	defer conn.Close()

	client := grpc_channelz_v1.NewChannelzClient(conn)

	// Get Servers
	serversResp, err := client.GetServers(ctx, &grpc_channelz_v1.GetServersRequest{})
	if err != nil {
		data.ServersError = err
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
			data.Servers = append(data.Servers, ServerData{
				ServerID:  srv.Ref.ServerId,
				Started:   started,
				Succeeded: succeeded,
				Failed:    failed,
				LastCall:  lastCall,
			})
		}
	}

	// Get Top Channels
	channelsResp, err := client.GetTopChannels(ctx, &grpc_channelz_v1.GetTopChannelsRequest{})
	if err != nil {
		data.ChannelsError = err
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
			data.Channels = append(data.Channels, ChannelData{
				ChannelID: chId,
				Target:    target,
				State:     state,
				Started:   started,
				Succeeded: succeeded,
				Failed:    failed,
			})
		}
	}

	s.templates.ExecuteTemplate(w, "grpc.html", data)
}
