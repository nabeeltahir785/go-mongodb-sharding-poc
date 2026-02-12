package config

import "os"

// ClusterConfig holds all settings for the MongoDB sharded cluster.
type ClusterConfig struct {
	AdminUser        string
	AdminPassword    string
	AppUser          string
	AppPassword      string
	ReadOnlyUser     string
	ReadOnlyPassword string
	AppDatabase      string
	ConfigRS         ReplicaSet
	Shards           []ReplicaSet
	MongosHosts      []string
}

// ReplicaSet represents a named set of MongoDB members.
type ReplicaSet struct {
	Name    string
	Members []Member
}

// Member represents a single mongod node.
type Member struct {
	Host string
	Port string
}

// Addr returns host:port for this member.
func (m Member) Addr() string {
	return m.Host + ":" + m.Port
}

// Load builds cluster config from environment variables with defaults.
func Load() *ClusterConfig {
	return &ClusterConfig{
		AdminUser:        env("MONGO_ADMIN_USER", "clusterAdmin"),
		AdminPassword:    env("MONGO_ADMIN_PASSWORD", "admin123"),
		AppUser:          env("MONGO_APP_USER", "appUser"),
		AppPassword:      env("MONGO_APP_PASSWORD", "app123"),
		ReadOnlyUser:     env("MONGO_READONLY_USER", "readOnlyUser"),
		ReadOnlyPassword: env("MONGO_READONLY_PASSWORD", "read123"),
		AppDatabase:      env("MONGO_APP_DATABASE", "sharding_poc"),

		ConfigRS: ReplicaSet{
			Name: "configrs",
			Members: []Member{
				{Host: "cfg-1", Port: "27019"},
				{Host: "cfg-2", Port: "27020"},
				{Host: "cfg-3", Port: "27021"},
			},
		},

		Shards: []ReplicaSet{
			{
				Name: "shard1rs",
				Members: []Member{
					{Host: "shard1-1", Port: "27022"},
					{Host: "shard1-2", Port: "27023"},
					{Host: "shard1-3", Port: "27024"},
				},
			},
			{
				Name: "shard2rs",
				Members: []Member{
					{Host: "shard2-1", Port: "27025"},
					{Host: "shard2-2", Port: "27026"},
					{Host: "shard2-3", Port: "27027"},
				},
			},
			{
				Name: "shard3rs",
				Members: []Member{
					{Host: "shard3-1", Port: "27028"},
					{Host: "shard3-2", Port: "27029"},
					{Host: "shard3-3", Port: "27030"},
				},
			},
		},

		MongosHosts: []string{
			"localhost:27017",
			"localhost:27018",
		},
	}
}

func env(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
