#!/bin/bash
# Jonathan Haller & Meir Jacobs
mvn clean compile
echo "Running the demo before my tests because my tests take a really long time and if they fail/stall I don't want that to affect my demo performance."
echo "NOTE: Sometimes the code may appear to stall, but usually after a minute or two max it picks up again on it's own."
java -cp target/classes edu.yu.cs.com3800.stage5.GatewayServer 8 & PID8=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 1 & PID1=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 2 & PID2=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 3 & PID3=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 4 & PID4=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 5 & PID5=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 6 & PID6=$!
java -cp target/classes edu.yu.cs.com3800.stage5.ZooKeeperPeerServerImpl 7 & PID7=$!
sleep 3
NODES=$(curl -s 'http://localhost:8080/getNodes')
LEADER=${NODES:0:1}
echo $NODES
echo $LEADER
list=()
echo 'Sending 9 requests and waiting for responses'
for i in {1..9}; do
{
    curl -s 'http://localhost:8080/compileandrun' -H "Content-Type: text/x-java-source" -d $'package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world #'$i$'!\\n\";\n    }\n}\n'
} &
list+=($!)
done
for pid in "${list[@]}";
do
     wait $pid
done
echo ""
echo "Killing server 1 and waiting for gateway to notice..."
kill -9 $PID1
sleep 35
curl -s 'http://localhost:8080/isDead' -d '1'
NODES2=$(curl -s 'http://localhost:8080/getNodes')
echo $NODES2
#6#### Kill Leader #####
if [ $LEADER == '7' ]; then
  echo killing 7
	kill -9 $PID7
fi

if [ $LEADER == '6' ]; then
  echo killing 6
	kill -9 $PID6
fi

if [ $LEADER == '5' ]; then
	echo killing 5
	kill -9 $PID5
fi

if [ $LEADER == '4' ]; then
	echo killing 4
	kill -9 $PID4
fi
sleep 1
#####Send 9 requests#####
echo ""
echo 'Sending 9 requests...'
list=()
for i in {10..18}; do
{
    curl -s 'http://localhost:8080/compileandrun' -H "Content-Type: text/x-java-source" -d $'package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world #'$i$'!\\n\";\n    }\n}\n'
} &
list+=($!)
done
#####Wait for gateway to get leader#####
echo 'Waiting for gateway to get new leader...'
curl -s 'http://localhost:8080/isDead' -d $LEADER
NODES3=$(curl -s 'http://localhost:8080/getNodes')
LEADER=${NODES3:0:1}
echo 'Current leader: '$LEADER
#####Wait for responses#####
echo 'Printing responses...'
for pid in "${list[@]}";
do
     wait $pid
done
#####Send Last Request#####
echo ""
echo "Sending last request..."
curl -s 'http://localhost:8080/compileandrun' -H "Content-Type: text/x-java-source" -d $'package edu.yu.cs.fall2019.com3800.stage1;\n\npublic class HelloWorld\n{\n    public String run()\n    {\n        return \"Hello world from last request!\";\n    }\n}\n'
echo ""
echo "Gossip messages are contained in ./gossipLogs. The first log file for e.g. server0 is the one from the demo, the rest are from my test methods"
##########
kill -9 $PID2
kill -9 $PID3
kill -9 $PID4
kill -9 $PID5
kill -9 $PID6
kill -9 $PID7
kill -9 $PID8
echo ""
mvn test
