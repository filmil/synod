package server

import (
	"fmt"
	"net"
	"time"

	"github.com/golang/glog"
)

// ListenWithRetry attempts to listen on the given address.
// If the port is occupied, it tries to find any free port for up to 1 minute.
func ListenWithRetry(addr string) (net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err == nil {
		glog.Infof("Listening on %v", lis.Addr())
		return lis, nil
	}

	glog.Warningf("Failed to listen on %s: %v. Retrying for 1 minute to find a free port...", addr, err)

	start := time.Now()
	for time.Since(start) < time.Minute {
		// Try a random port
		lis, err = net.Listen("tcp", ":0")
		if err == nil {
			glog.Infof("Successfully found a free port. Now listening on %v", lis.Addr())
			return lis, nil
		}
		time.Sleep(100 * time.Millisecond)
	}

	return nil, fmt.Errorf("could not find a free port after 1 minute: %w", err)
}
