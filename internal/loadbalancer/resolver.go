package loadbalancer

import (
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"google.golang.org/grpc/resolver"
)

const (
	// StaticScheme is used for comma-separated address lists (local/docker-compose).
	// Usage: static:///host1:50051,host2:50051
	StaticScheme = "static"

	// Re-resolve interval for DNS-based discovery (pick up pod scale events).
	defaultReResolveInterval = 30 * time.Second
)

// staticResolverBuilder resolves a fixed list of addresses from the target URL.
// Designed for local development and docker-compose where pod discovery isn't needed.
type staticResolverBuilder struct{}

func (b *staticResolverBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	endpoint := target.Endpoint()
	if endpoint == "" {
		return nil, fmt.Errorf("static resolver: empty endpoint in target %q", target.URL.String())
	}

	hosts := strings.Split(endpoint, ",")
	addrs := make([]resolver.Address, 0, len(hosts))
	for _, h := range hosts {
		h = strings.TrimSpace(h)
		if h == "" {
			continue
		}
		addrs = append(addrs, resolver.Address{Addr: h})
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("static resolver: no valid addresses in %q", endpoint)
	}

	log.Printf("[loadbalancer] static resolver: %d endpoints -> %v", len(addrs), hosts)

	if err := cc.UpdateState(resolver.State{Addresses: addrs}); err != nil {
		return nil, fmt.Errorf("static resolver: update state: %v", err)
	}

	return &staticResolver{}, nil
}

func (b *staticResolverBuilder) Scheme() string { return StaticScheme }

// staticResolver is a no-op resolver — addresses are fixed at build time.
type staticResolver struct{}

func (r *staticResolver) ResolveNow(resolver.ResolveNowOptions) {}
func (r *staticResolver) Close()                                {}

// PeriodicDNSResolver wraps the built-in DNS resolver with periodic re-resolution.
// In Kubernetes, headless services resolve to individual pod IPs. As pods scale
// up/down, re-resolution discovers the new set of endpoints.
type PeriodicDNSResolver struct {
	cc       resolver.ClientConn
	target   resolver.Target
	inner    resolver.Resolver
	interval time.Duration
	done     chan struct{}
	once     sync.Once
}

// periodicDNSBuilder wraps the default DNS resolver with timed re-resolution.
type periodicDNSBuilder struct {
	interval time.Duration
}

func (b *periodicDNSBuilder) Build(target resolver.Target, cc resolver.ClientConn, opts resolver.BuildOptions) (resolver.Resolver, error) {
	// Build the default DNS resolver
	dnsBuilder := resolver.Get("dns")
	if dnsBuilder == nil {
		return nil, fmt.Errorf("dns resolver not found (this should never happen)")
	}

	inner, err := dnsBuilder.Build(target, cc, opts)
	if err != nil {
		return nil, fmt.Errorf("dns resolver build: %v", err)
	}

	r := &PeriodicDNSResolver{
		cc:       cc,
		target:   target,
		inner:    inner,
		interval: b.interval,
		done:     make(chan struct{}),
	}

	go r.refreshLoop()

	log.Printf("[loadbalancer] DNS resolver with %s re-resolution: %s", b.interval, target.Endpoint())

	return r, nil
}

func (b *periodicDNSBuilder) Scheme() string { return "dns" }

// refreshLoop triggers periodic re-resolution to pick up pod changes.
func (r *PeriodicDNSResolver) refreshLoop() {
	ticker := time.NewTicker(r.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			r.inner.ResolveNow(resolver.ResolveNowOptions{})
		case <-r.done:
			return
		}
	}
}

func (r *PeriodicDNSResolver) ResolveNow(opts resolver.ResolveNowOptions) {
	r.inner.ResolveNow(opts)
}

func (r *PeriodicDNSResolver) Close() {
	r.once.Do(func() {
		close(r.done)
		r.inner.Close()
	})
}

// RegisterResolvers registers the custom static resolver globally.
// Call this once at application startup before dialing.
func RegisterResolvers() {
	resolver.Register(&staticResolverBuilder{})
	log.Println("[loadbalancer] registered static:/// resolver")
}
