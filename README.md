# Spark Decoded
## Introduction
Apache Spark is a cluster computing framework, designed for processing large amounts of data. It was created at the University of California, accompanied by [this paper](http://www.eecs.berkeley.edu/Pubs/TechRpts/2014/EECS-2014-12.html). Later, it was moved to the Apache Software Foundation. 

Due to its impressive performance, it rapidly gained an audience, and several organizations use Spark for (parts of) their applications, including Amazon, eBay and Yahoo! ([source](https://cwiki.apache.org/confluence/display/SPARK/Powered+By+Spark)).

In spite of all the attention Spark received, its inner workings are rather hard to grasp. For starters, this is due to the paper not explaining it very thoroughly. Furthermore, although it is open sourced, the core functionality is hidden away rather deep down the rabbit hole, in high-level components such as schedulers and wrapper classes. The documentation is focused on how to use it, rather than what happens under the hood. 
  
The goal of this project is to provide a simplified implementation of Spark's core that is easily understood, along with proper documentation that is considerably shorter than the paper. This is achieved by digging through the source code (which contains plenty of comments) bit by bit and stripping out several layers of abstraction.

First, I will introduce the different kinds of components used in Spark. Then, in the next section, the interaction between the components will be explained.

## Components

### RDDs

### Partitions

### Partitioners

## Interaction
