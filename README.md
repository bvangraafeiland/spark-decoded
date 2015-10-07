# Spark Decoded
## Introduction
Apache Spark is a cluster computing framework, designed for processing large amounts of data. It was created at the University of California, accompanied by [this paper](http://www.eecs.berkeley.edu/Pubs/TechRpts/2014/EECS-2014-12.html). Later, it was moved to the Apache Software Foundation. 

Due to its impressive performance, it rapidly gained an audience, and several organizations use Spark for (parts of) their applications, including Amazon, eBay and Yahoo! ([source](https://cwiki.apache.org/confluence/display/SPARK/Powered+By+Spark)).

In spite of all the attention Spark received, its inner workings are rather hard to grasp. For starters, this is due to the paper not explaining it very thoroughly. Furthermore, although it is open sourced, the core functionality is hidden away rather deep down the rabbit hole, in high-level components such as schedulers and wrapper classes. The documentation is focused on how to use it, rather than what happens under the hood. 

The goal of this project is to provide a simplified implementation of Spark's core that is easily understood, along with proper documentation that is considerably shorter than the paper. This is achieved by digging through the source code (which contains plenty of comments) bit by bit and stripping out several layers of abstraction.

First, I will introduce the different kinds of components used in Spark. Then, in the next section, the interaction between the components will be explained.

## Components

### Resilient Distributed Datasets
Resilient Distributed Datasets, or RDDs, are the most important part of Spark. They are read-only wrappers around datasets that are created from Hadoop, text files, or any user-defined data source. By applying an operation to an RDD, a new RDD is formed rather than mutating the original one.

A traditional issue is fault tolerance. This is generally solved by replicating data or logging updates, while operations allow for fine-grained operations on mutable data state. RDDs on the other hand only provide coarse grained transformations, where the same operation is applied to the entire dataset. This way, fault tolerance is easily achieved by logging only the operations performed on the data. On data loss, only the lost partitions need to be recalculated. When a node fails, another node can take over its partitions.

A major selling point for RDDs is that they are very general, so any distributed system can be emulated.

Formally, an RDD is defined by five properties:
- A list of partitions
- A function to compute a given partition, returning it as an Iterator
- A list of dependencies on other RDDs
- A partitioner, an object that assigns data to partitions
- A function that returns a list of preferred locations for a given partition, allowing faster access due to data locality

### Partitions
A partition represents a subset of the data of an RDD. When running computations, the partitions are distributed among the available nodes in the cluster, after which each node computes only their partitions.

### Partitioners
A partitioner assigns elements of an RDD consisting of key-value pairs to a partition by mapping a key value to a valid partition ID.

## Concepts

### Operations

### Dependencies