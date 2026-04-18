// SPDX-License-Identifier: Apache-2.0

package server

import (
	"context"
	"fmt"
	"net"
	"time"

	"github.com/filmil/synod/internal/backoff"
	"github.com/golang/glog"
)

// ListenWithRetry attempts to listen on the given address.
// If the port is occupied, it tries to find any free port using an exponential backoff strategy for up to 1 minute.
func ListenWithRetry(addr string) (net.Listener, error) {
	lis, err := net.Listen("tcp", addr)
	if err == nil {
		glog.Infof("Listening on %v", lis.Addr())
		return lis, nil
	}

	glog.Warningf("Failed to listen on %s: %v. Retrying for 1 minute to find a free port...", addr, err)

	bo := backoff.New()
	bo.MaxElapsedTime = time.Minute
	bo.InitialInterval = 100 * time.Millisecond
	bo.MaxInterval = 2 * time.Second

	var finalLis net.Listener
	err = bo.Retry(context.Background(), "ListenWithRetry", func() error {
		// Try a random port
		tempLis, err := net.Listen("tcp", ":0")
		if err == nil {
			finalLis = tempLis
			return nil
		}
		return err
	})

	if err != nil {
		return nil, fmt.Errorf("could not find a free port after 1 minute: %w", err)
	}

	glog.Infof("Successfully found a free port. Now listening on %v", finalLis.Addr())
	return finalLis, nil
}
