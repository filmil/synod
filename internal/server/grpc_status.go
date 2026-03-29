package server

import (
	"context"
	"fmt"
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

	s.renderHeader(w, "gRPC Introspection", shortName, "grpc")

	eph, ok := s.cell.GetEphemeralPeer(agentID)
	if !ok || eph.GRPCAddr == "" {
		fmt.Fprintf(w, "<div class='alert alert-danger'>gRPC address not found for self</div>")
		s.renderFooter(w)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	conn, err := grpc.NewClient(eph.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintf(w, "<div class='alert alert-danger'>Failed to connect to local gRPC channelz: %v</div>", err)
		s.renderFooter(w)
		return
	}
	defer conn.Close()

	client := grpc_channelz_v1.NewChannelzClient(conn)

	// Get Servers
	serversResp, err := client.GetServers(ctx, &grpc_channelz_v1.GetServersRequest{})
	if err != nil {
		fmt.Fprintf(w, "<div class='alert alert-warning'>Failed to get servers: %v</div>", err)
	}

	fmt.Fprintf(w, `<div class="card shadow-sm mb-4">
        <div class="card-header bg-primary text-white">gRPC Servers</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-hover">
              <thead>
                <tr>
                  <th>Server ID</th>
                  <th>Calls Started</th>
                  <th>Calls Succeeded</th>
                  <th>Calls Failed</th>
                  <th>Last Call Started</th>
                </tr>
              </thead>
              <tbody>`)
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
			fmt.Fprintf(w, "<tr><td>%d</td><td>%d</td><td>%d</td><td>%d</td><td>%s</td></tr>",
				srv.Ref.ServerId, started, succeeded, failed, lastCall)
		}
	} else {
		fmt.Fprintf(w, "<tr><td colspan='5'>No servers found</td></tr>")
	}
	fmt.Fprintf(w, `</tbody></table></div></div></div>`)

	// Get Top Channels
	channelsResp, err := client.GetTopChannels(ctx, &grpc_channelz_v1.GetTopChannelsRequest{})
	if err != nil {
		fmt.Fprintf(w, "<div class='alert alert-warning'>Failed to get top channels: %v</div>", err)
	}

	fmt.Fprintf(w, `<div class="card shadow-sm mb-4">
        <div class="card-header bg-success text-white">gRPC Top Channels</div>
        <div class="card-body">
          <div class="table-responsive">
            <table class="table table-sm table-hover">
              <thead>
                <tr>
                  <th>Channel ID</th>
                  <th>Target</th>
                  <th>State</th>
                  <th>Calls Started</th>
                  <th>Calls Succeeded</th>
                  <th>Calls Failed</th>
                </tr>
              </thead>
              <tbody>`)

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
			fmt.Fprintf(w, "<tr><td>%d</td><td>%s</td><td>%s</td><td>%d</td><td>%d</td><td>%d</td></tr>",
				chId, target, state, started, succeeded, failed)
		}
	} else {
		fmt.Fprintf(w, "<tr><td colspan='6'>No channels found</td></tr>")
	}
	fmt.Fprintf(w, `</tbody></table></div></div></div>`)

	s.renderFooter(w)
}
