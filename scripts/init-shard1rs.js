// Shard 1 Replica Set initialization
// Run against shard1-1:27022

rs.initiate({
    _id: "shard1rs",
    members: [
        { _id: 0, host: "shard1-1:27022" },
        { _id: 1, host: "shard1-2:27023" },
        { _id: 2, host: "shard1-3:27024" }
    ]
});
