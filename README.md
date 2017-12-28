### Fun-with-raft 

This is a copy/port of https://github.com/coreos/etcd/tree/master/contrib/raftexample 

Working towards understanding the raft library/transport layer for the etcd raft implementation. 

Attempting to create packages around the example code to understand it a little better.

### starting a cluster
```
fwr --id 1 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 12380
fwr --id 2 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 22380
fwr --id 3 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379 --port 32380
```


### Sending a value 
`curl -L http://127.0.0.1:12380/my-key -XPUT -d bar`

### Getting a value 
`curl -L http://127.0.0.1:12380/my-key`

### Deleting a node
`curl -L http://127.0.0.1:12380/3 -XDELETE`

### Adding a node 
`curl -L http://127.0.0.1:12380/4 -XPOST -d http://127.0.0.1:42379`
`fwr --id 4 --cluster http://127.0.0.1:12379,http://127.0.0.1:22379,http://127.0.0.1:32379,http://127.0.0.1:42379 --port 42380 --join` 


### More info 
 http://raftconsensus.github.io/
 
