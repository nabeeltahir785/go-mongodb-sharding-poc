// Shard 3 Replica Set initialization
// Run against shard3-1:27028

rs.initiate({
    _id: "shard3rs",
    members: [
        { _id: 0, host: "shard3-1:27028" },
        { _id: 1, host: "shard3-2:27029" },
        { _id: 2, host: "shard3-3:27030" }
    ]
});
