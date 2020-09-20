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
- We can think of Spark as a flexible alternative of MapReduce. 
- MapReduce requires files to be stored in HDFS, Spark doesn't
- Spark can also perform operations up to 100% faster than MapReduce. How does it do it though ?
    - MapReduce writes mosr data to disk after every map and reduce operation
    - Spark keeps most of the data in memory after each transformation
    - Spark can spill over to disk if the memory is filled.
- At the coore of the Spark is the idea of a Resilient Distributed Dataset (RDD)
- RDD has 4 main features:
    - Distributed Collection of Data
    - Fault tolerant
    - Parallel operation - partioned 
    - Ability to use many data sources

![MapReduce](https://github.com/prashantfb65/spark-project/blob/implementation_zero/images/spark_1.png?raw=true)

- RDDs are immutable, lazily evaluated, and cacheable. There are two types of Spark operations:
    - Transformations: are basically a recipe to follow.
    - Actions: actually perform what the recipe says to do and returns something back
- This behaviour carries over to the syntax when coding. A lot of times you will write a method call, but won’t see anything as a result until you call the action. This makes sense because with a large dataset, you don’t want to calculate all the transformations until you are sure you want to perform them
