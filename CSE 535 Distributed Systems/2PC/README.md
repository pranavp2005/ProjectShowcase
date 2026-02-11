Implementtion of scalable fault-tolerant transaction processing system in golang:

Goal of Project:
Create a banking application capable of handling balance tranfer and balance read operations.
Provide fault tolerance using Multi-Paxos(Stable leader paxos).
Provide scalability through sharding, use two phase commit protocol to support intra shard operations.

Consistency levels supported:

Read Consistency:
1. Linearizable
2. Eventual
3. Quorum
