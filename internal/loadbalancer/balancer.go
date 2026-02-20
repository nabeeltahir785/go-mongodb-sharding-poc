package loadbalancer

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
)

// DefaultServiceConfig returns the gRPC service config JSON that enables
// round-robin load balancing and health checking.
//
// How it works:
//   - loadBalancingConfig: tells the gRPC client to distribute RPCs across
//     all resolved endpoints using round-robin (not pin to one connection)
//   - healthCheckingConfig: the client probes each endpoint via the standard
//     grpc.health.v1.Health service and stops routing RPCs to unhealthy ones
func DefaultServiceConfig(serviceName string) string {
	config := map[string]interface{}{
		"loadBalancingConfig": []map[string]interface{}{
			{"round_robin": map[string]interface{}{}},
		},
		"healthCheckConfig": map[string]interface{}{
			"serviceName": serviceName,
		},
	}

	raw, err := json.Marshal(config)
	if err != nil {
		// Fallback to minimal config — this should never fail
		return `{"loadBalancingConfig":[{"round_robin":{}}]}`
	}
	return string(raw)
}

// DialOptions returns gRPC dial options configured for client-side load balancing.
// These should be used instead of manual connection pools.
func DialOptions(serviceName string) []grpc.DialOption {
	return []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),

		// Service config: round-robin LB + health checking
		grpc.WithDefaultServiceConfig(DefaultServiceConfig(serviceName)),

		// Message size limits (16MB for bulk payloads)
		grpc.WithDefaultCallOptions(
			grpc.MaxCallRecvMsgSize(16*1024*1024),
			grpc.MaxCallSendMsgSize(16*1024*1024),
		),

		// Keepalive: detect dead connections early
		grpc.WithKeepaliveParams(keepalive.ClientParameters{
			Time:                30 * time.Second, // Ping every 30s
			Timeout:             10 * time.Second, // Wait 10s for pong
			PermitWithoutStream: true,             // Keep probing even when idle
		}),
	}
}

// NewClientConn creates a gRPC client connection with client-side load balancing.
//
// Target formats:
//   - Local:  "static:///localhost:50051,localhost:50052"
//   - K8s:    "dns:///grpc-server-headless.sharding-poc.svc.cluster.local:50051"
//
// The connection uses round-robin to distribute RPCs across all resolved endpoints.
// Combined with gRPC health checking, unhealthy endpoints are automatically excluded.
func NewClientConn(target string) (*grpc.ClientConn, error) {
	RegisterResolvers()

	opts := DialOptions("sharding.v1.ShardingService")
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, fmt.Errorf("grpc dial %s: %v", target, err)
	}

	log.Printf("[loadbalancer] connected: target=%s policy=round_robin health=enabled", target)
	return conn, nil
}
