# Hadoop Ecosystem

[[_TOC_]]

## Big Data
3V's of big data
1. Volume
2. Variety
3. Velocity
4. Veracity is the 4th V which indicates the uncertainty in the data

### Hadoop:
- Runs on commodity hardware
- Low cost per GB
- Ability to store Petabytes of data in a single cluster
- Hadoop was invented at Yahoo, and inspired by Google file system and Google's MapReduce papers

![Hadoop Ecosystem](https://github.com/prashantfb65/spark-project/blob/implementation_zero/images/hdp_eco.png?raw=true)

### What is Hadoop?
- Hadoop is a distributed system
- The data nodes and worker nodes can scale horizontally ( The data is stored and processes run over the data nodes)
- In Hadoop we bring the computation to where the data is stored.
- Namenode stores the metadata for the files in hadoop, it also runs a yarn resource manager service which is responsible for giving our application necessary resources on the cluster.
- There is a standby namenode and standby yarn resource manager in case the the main ones fails.
- Hadoop will automatically recover from a node failure. This means that code within the application possibly gets executed twice. Developers will need to handle that.

![Hadoop Model](https://github.com/prashantfb65/spark-project/blob/implementation_zero/images/name_data_node.png?raw=true)

![Hadoop Distributions](https://github.com/prashantfb65/spark-project/blob/implementation_zero/images/distributions.png?raw=true)

### HDFS and MapReduce
- HDFS will use blocks of data with a size of 128 MB by default
- Each of these blocks is replocated 3 times
- The blocks are distributed in a way to support fault tolerance
-  MapReduce is a way of splitting a computation task to a distributed set of files
![MapReduce](https://github.com/prashantfb65/spark-project/blob/implementation_zero/images/name_node.png?raw=true)
- MapReduce consists of a Job Tracker and multiple task trackers
![MapReduce](https://github.com/prashantfb65/spark-project/blob/implementation_zero/images/task_node.png?raw=true)
- The task trackers allocate CPU and memory for the tasks and monitor the tasks on the worker nodes
- The Job tracker sends the code to run on the task trackers

### Spark
We can think of Spark as a flexible alternative of MapReduce. 


