# Hadoop Ecosystem

<!-- [[_TOC_]] -->

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

![Hadoop Ecosystem](https://github.com/prashantfb65/spark-project/blob/master/images/hdp_eco.png?raw=true)

### What is Hadoop?
- Hadoop is a distributed system
- The data nodes and worker nodes can scale horizontally ( The data is stored and processes run over the data nodes)
- In Hadoop we bring the computation to where the data is stored.
- Namenode stores the metadata for the files in hadoop, it also runs a yarn resource manager service which is responsible for giving our application necessary resources on the cluster.
- There is a standby namenode and standby yarn resource manager in case the the main ones fails.
- Hadoop will automatically recover from a node failure. This means that code within the application possibly gets executed twice. Developers will need to handle that.

![Hadoop Model](https://github.com/prashantfb65/spark-project/blob/master/images/name_data_node.png?raw=true)

![Hadoop Distributions](https://github.com/prashantfb65/spark-project/blob/master/images/distributions.png?raw=true)

### HDFS and MapReduce
- HDFS will use blocks of data with a size of 128 MB by default
- Each of these blocks is replocated 3 times
- The blocks are distributed in a way to support fault tolerance
-  MapReduce is a way of splitting a computation task to a distributed set of files
![MapReduce](https://github.com/prashantfb65/spark-project/blob/master/images/name_node.png?raw=true)
- MapReduce consists of a Job Tracker and multiple task trackers
![MapReduce](https://github.com/prashantfb65/spark-project/blob/master/images/task_node.png?raw=true)
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

![MapReduce](https://github.com/prashantfb65/spark-project/blob/master/images/spark_1.png?raw=true)

- RDDs are immutable, lazily evaluated, and cacheable. There are two types of Spark operations:
    - Transformations: are basically a recipe to follow.
    - Actions: actually perform what the recipe says to do and returns something back
- This behaviour carries over to the syntax when coding. A lot of times you will write a method call, but won’t see anything as a result until you call the action. This makes sense because with a large dataset, you don’t want to calculate all the transformations until you are sure you want to perform them

### Setup Spark
1. Install Java
2. Install Python
3. Install Scala
4. Set environment variables
```bash
# Settings for PySpark
export SPARK_HOME='{path where spark package is downloaded}/spark-3.0.1-bin-hadoop2.7'
export PYTHONPATH=$SPARK_HOME/python:$PYTHONPATH
export PYSPARK_DRIVER_PYTHON="jupyter"
export PYSPARK_DRIVER_PYTHON_OPTS="notebook"
export PYSPARK_PYTHON=python3
export PATH=$SPARK_HOME:$PATH:~/.local/bin:$JAVA_HOME/bin:$JAVA_HOME/jre/bin
```
5. Activate Python virtual environment
```bash
. ./setup-env.sh
pip install -r requirements-spark.txt
```
6. Start Jupyter notebook
```bash
jupyter notebook
```
![Spark structure](https://github.com/prashantfb65/spark-project/blob/master/images/spark_2.png?raw=true)

### Spark MLlib
Spark’s MLlib is mainly designed for Supervised and Unsupervised Learning tasks, with most of its algorithms falling under those two categories.
1. Supervised learning algorithms are trained using labeled examples, such as an input where the desired output is known.
2. Unsupervised learning is used against data that has no historical labels. 
One of the main “quirks” of using MLlib is that you need to format your data so that eventually  it just has one or two columns:
- Features, Labels (Supervised)
- Features (Unsupervised)

### Confusion Matrix

![Confusion Matrix](https://github.com/prashantfb65/spark-project/blob/master/images/cm_1.png?raw=true)

![Confusion Matrix](https://github.com/prashantfb65/spark-project/blob/master/images/cm_2.png?raw=true)

### Natural Language processing

1. Tokenization
2. Stop world removal
3. Stemming and Lemmatization
3. Count vectorization - BOW
4. TF-IDF

![TF-IDF](https://github.com/prashantfb65/spark-project/blob/master/images/tf-idf.png?raw=true)

### Spark Streaming
Spark Streaming is an extension of the core Spark API that enables scalable, high-throughput, fault-tolerant stream processing of live data streams. 

Data can be ingested from many sources like Kafka, Flume, Kinesis, or TCP sockets, and can be processed using complex algorithms expressed with high-level functions like map, reduce, join and window.

![Spark Streaming](https://github.com/prashantfb65/spark-project/blob/master/images/spark-streaming.png?raw=true)

Internally, Spark Streaming receives live input data streams and divides the data into batches, which are then processed by the Spark engine to generate the final stream of results in batches.

![Spark Streaming processing](https://github.com/prashantfb65/spark-project/blob/master/images/spark-streaming2.png?raw=true)

The steps for streaming are:
1. Create a SparkContext
2. Create a StreamingContext
3. Create a Socket Text Stream
4. Read in the lines as a “DStream”

The steps for working with the data:
1. Split the input line into a list of words
2. Map each word to a tuple: (word,1)
3. Then group (reduce)  the tuples by the word (key) and sum up the second argument (the number one)

Command on the terminal
```bash
nc -lk 9999
```