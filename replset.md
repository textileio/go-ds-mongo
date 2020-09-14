# Dockerized replica-set for Mongo
For running TestDxnDiscard and TestTxnCommit Mongo should run in a 
replica-set. 
Below is a modified script from https://gist.github.com/oleurud/d9629ef197d8dab682f9035f4bb29065.

# create network
docker network create my-mongo-cluster

# create mongos
docker run -d --net my-mongo-cluster -p 27017:27017 --name mongo1 mongo mongod --replSet my-mongo-set
docker run -d --net my-mongo-cluster -p 27018:27017 --name mongo2 mongo mongod --replSet my-mongo-set
docker run -d --net my-mongo-cluster -p 27019:27017 --name mongo3 mongo mongod --replSet my-mongo-set

# add hosts
127.0.0.1       mongo1 mongo2 mongo3

# setup replica set
docker exec -it mongo1 mongo
db = (new Mongo('localhost:27017')).getDB('test')
config={"_id":"my-mongo-set","members":[{"_id":0,"host":"mongo1:27017"},{"_id":1,"host":"mongo2:27017"},{"_id":2,"host":"mongo3:27017"}]}
rs.initiate(config)

# connection URI
mongodb://localhost:27017,localhost:27018,localhost:27019/?replicaSet=my-mongo-set

# Run tests!
You can run these tests sub-group.
