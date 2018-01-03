#!/bin/sh

echo "checking peer 1"
for i in {1..50}
do
        curl -L http://127.0.0.1:12380/tx-$i
        echo ' recv from peer 1 \n'
done

echo "checking peer 2"
for i in {1..50}
do
        curl -L http://127.0.0.1:22380/tx-$i
        echo ' recv from peer 2 \n'
done

echo '\n'
echo "checking peer 3"
for i in {1..50}
do
        curl -L http://127.0.0.1:32380/tx-$i
	echo ' recv from peer 3 \n'        
done
