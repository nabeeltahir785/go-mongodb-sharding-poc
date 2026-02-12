// Config Server Replica Set initialization
// Run against cfg-1:27019

rs.initiate({
  _id: "configrs",
  configsvr: true,
  members: [
    { _id: 0, host: "cfg-1:27019" },
    { _id: 1, host: "cfg-2:27020" },
    { _id: 2, host: "cfg-3:27021" }
  ]
});
