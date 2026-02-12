// Shard 2 Replica Set initialization
// Run against shard2-1:27025

rs.initiate({
    _id: "shard2rs",
    members: [
        { _id: 0, host: "shard2-1:27025" },
        { _id: 1, host: "shard2-2:27026" },
        { _id: 2, host: "shard2-3:27027" }
    ]
});
