// Replica set configuration
var config = {
    "_id": "rs0",
    "members": [
        { "_id": 0, "host": "localhost:27017" }
    ]
}

rs.initiate(config)

while (!rs.isMaster().ismaster) {
    sleep(1000)
}

// Create the admin user
var adminDb = db.getSiblingDB('admin');
adminDb.createUser({
    user: process.env["MONGO_INITDB_ROOT_USERNAME"],
    pwd: process.env["MONGO_INITDB_ROOT_PASSWORD"],
    roles: [{ role: 'root', db: 'admin' }]
});
