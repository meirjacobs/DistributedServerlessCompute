# DistributedServerlessCompute
A distributed, fault-tolerant system which provides serverless execution of java code.
- Implements Zookeeper Algorithm to elect a cluster leader
- Has gateway for incoming HTTP requests
- Models producer-consumer behavior, with the leader node delegating work to follower nodes via TCP in a round-robin fashion
- Uses gossip-style UDP heartbeats to detect any down server, and self-adjusts accordingly. (For example, if the leader goes down it triggers a new Zookeeper Election, 
       if a follower goes down the leader re-delegates all of its uncompleted work, etc.)
- Detailed logging for easy debugging
- Demo script for the service is contained in the stage5 directory
