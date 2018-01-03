#!/bin/sh

echo "sending to peer 1" 
for i in {1..50}
do 
	curl -L http://127.0.0.1:12380/tx-$i -XPUT -d "{ \"key\" : \"test\", \"value\" : \"something cool\", \"tx\" : "$i" }" 
done


