package main

//NOTE: Coordinator will start/stop and flush the cluster
//

func ComputeNodeID(clusterNumber, clusterSize int) int {
	return (clusterNumber-1)*clusterSize + 1
}


