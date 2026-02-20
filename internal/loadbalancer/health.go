package loadbalancer

import (
	"log"

	"google.golang.org/grpc"
	"google.golang.org/grpc/health"
	healthpb "google.golang.org/grpc/health/grpc_health_v1"
)

// RegisterHealthServer registers the gRPC health checking service on the server.
// This enables clients using health-check-aware LB policies to detect and
// route around unhealthy pods automatically.
//
// Service status is set to SERVING for both the empty service (overall server)
// and the specific ShardingService — clients can probe either.
func RegisterHealthServer(server *grpc.Server) *health.Server {
	healthServer := health.NewServer()

	// Overall server health
	healthServer.SetServingStatus("", healthpb.HealthCheckResponse_SERVING)

	// Per-service health — matches the serviceName in client's healthCheckConfig
	healthServer.SetServingStatus(
		"sharding.v1.ShardingService",
		healthpb.HealthCheckResponse_SERVING,
	)

	healthpb.RegisterHealthServer(server, healthServer)

	log.Println("[health] gRPC health service registered (status=SERVING)")
	return healthServer
}
