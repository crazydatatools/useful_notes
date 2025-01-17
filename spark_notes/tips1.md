# Some Importantan points in Spark
-  Narrow Transformations
  - filters rows where cityr=ny
  - add a new column: adding first_name and last_name 
  - alter an existing column:adding 5 to age column
  - select relavant columns

### code snippet
``` df_narrow_transform={
    df_cutomers.filter(F.col("city")=="ny")
    .withColumn("first_name",F.split("name," ").getItem(0))
    .withColumn("last_name",F.split("name," ").getItem(1))
    .withColumn("age",F.col("age"+F.lit95))
    .select("cust_id","age")
}
df_narrow_transform.show(5,False)
df_narrow_transform.explain(True)
```
- Wide Transformations
  - Repartition -either increase or decrease ur partitions by re-distributing your data
  - Coalesce
  - joins-- SortMerge Join & Broadcast Join
  - group bys
    - count
    - countDistinct
    - sum

### code snippet
```
df_wide_transform= df.rdd.getNumPartitions()
df.repartition(24).explain(True)  ---AdaptiveSparkPlan (AQE-Adaptive QueyExecution--3.0)--runtime plan,round robin-partition scheme

df.rdd.getNumPartitions()
df.coalesce(1).
```
- coalesce doesnt show the partiting scheme-roundrobin
- this operation minimizes the data movement by merging the into fewer partitions,it doesnt do any shuffling

# joins
```
spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
df_joined=(
    df.join(
        df_customers,how="inner",on="cust_id"
    )
)

df.joined.explain(True)

```
- hashpartiting--same keys end-up in same partitions
- hashAggregate - partial aggregate within local executor
- push filter doesnot work for complex data types--like properties or key-values paird
- psuh filter not work for cast operations
# Group By

```
df_city_counts = (
    df_transactions
    .groupBy("city")
    .count()
)
df_txn_amt_city = (
    df_transactions
    .groupBy("city")
    .agg(F.sum("amt").alias("txn_amt"))
)
df_txn_amt_city = (
    df_transactions
    .groupBy("city")
    .agg(F.countDistinct("city").alias("city_cnt"))
)
```

# Spark Execution Container
- On-Heap Memory (spark.exetor.memory)
- Execution Memory( joins,shuffles,Sorting,GroupBy)
- Strorage Memory (RDD,Dataframes,Caching,Broadcast variables)
- User Memory(user defined data structures,variables,objects)
- Reserved Memory (300 Mb)
- Addtions
    - Off-Heap Memory
    - Overhead


# Spark Executors Types
- Think executor (each machine will have more executors eclusing 1 core and 1 gb ram for os)
- fat Executors ( each machine will have min executor and more memory leaving 1 core and 1gb ram to os)
- optimal executors settings
    - hadoop/yarn/os deamons -node level-- leave 1 core and 1gb ram
    - yarn application(am) -cluster level-- leave 1 core and 1 gb ram
    - hdfs throughpu--rate at which data wrte and read can happen, so ideal setting wud be 3-5 cores per executor,i.e 3-5 tasks per executor only
    - memory overhead-- leave memory overhead from executor memory--384 mb or 10% of em

- example (5 nodes, 12 cores, 48 gb ram)
    -  cluster level resources ( 11 cores and 47 gb)
        - cluseter level total memory =47*5= 235-1(AM)=234 gb
        - cluster level total cores=11 *5 =55-1(AM)= 54 cores
        - executor - cores => 5 cores per executor,so total executors =54/5= ~10 executors
        - executor -memory => memrory per executor = 234/10=23 gb,so 23-2.3(10%)=20 gb
        -  --num_executors 10 , --executor-cores 5 , --executor-memory 20 gb

- example (3 nodes, 16 cores, 48 gb ram)
    -  cluster level resources ( 15 cores and 47 gb) per each node
        - cluseter level total memory =47*3= 141-1(AM)=140 gb
        - cluster level total cores=15 *3 =45-1(AM)= 44 cores
        - executor - cores => 5 cores per executor,so total executors =44/5= ~8 executors
        - executor -memory => memrory per executor = 140/8=15 gb,so 15-1.5(10%)=13 gb    
    -  --num_executors 8 , --executor-cores 5 , --executor-memory 13 gb      


## Shuffle Partitions:- spark.sql.shuffle.partition=200 --adjusted to 1500
-- Optimal size of shuffle partiton size  is 1 to 200 MB only
- Senarios1-- data per shuffle partition is large (if we consider shuffle write partition size=300 gb)
   - total core= 5*4 =20
   - default SP = 200
   - data size =300 gb
   - size per shuffle partition ==>
   - size per SP==>00 * 100 mb/ 200 (sp) = 1.5gb
   - nos. of SP = 300 * 1000 mb/ 200 mb= 1500 SP

## Shuffle Partitions:- spark.sql.shuffle.partition=200-- adjusted to 12
-- Optimal size of shuffle partiton size  is 1 to 200 MB only
- Senarios2-- data per shuffle partition is very small (if we consider shuffle write partition size=50 mb)
   - total core= 4cores*3 executor =12
   - default SP = 200
   - data size =50 mb
   - size per shuffle partition ==>
   - size per SP==>50mb / 200 (sp) = 250 kb
   - nos. of SP = 50 mb / 10 mb= 5 SP (based on optimal size of partion 1-200 we choose 10 mb) 
   - only 5 used out of 12 and rest sitting idle
   - no of sp = 50mb/12 cores= 4 .2 mb    


## Hos to choose which column as partition column
    - column with high cardinality like --cust_id
    - low cardinality like state 
    - low to medium cardinality
    - filter column column

##  How to control the no of partion while reading as file
    ``` spark.conf.set("spark.sql.files.maxPartitionBytes","1000") --1kb size
    df_mod= spark.read.csv("../data/file1.csv", header=True,InferSchema=True)
    mod_part=df_mod.rdd.getNumPartitions()
    if the file fize= 444kb then it reads 1kb files to total 448 partitions
    ```

## Bucketing
    - operation like filter, groupby and join will be beneficial
    -- if join then it will have shuffle/sort/merge
    -- if column is high cardinality then partioning is not option it will create small file problem
    --if bucketing then the join wud be efficient
# how to decide optimal number of buckets?
    - size of the dataset=x
    - optimal bucket size =128-200 mb
    - nod of buckets = size fo dataset/optimal bucket size==> 1000 MB/200 MB= 5

# how to estimate the size of a dataset
- number of megabytes=M=(nof of recrods(N)* number of variables(V)*avg width in bytes of avariable(W) /1024 pow 2 )
```
df_prducts.write.bucketBy(4,col='product_id")
.mode("overwrite"
.saveAsTable("products_bucketed"))
```

## RDD's
- rdd are immutable,once created can't be modifed, operations on RDD's generate new RDD's,also heps data consistency and simlified fault recovery.
- transformation on RDD's are not executed immediately,it execute only when actions are called.
## transformations
--Trasformation in spark refer to process of creating new RDD's from existing one.
--Transformation do not alter the original RDD's and follow lazy evaluation.
- transform a Spark DataFrame into a new DataFrame.
## Actions
- operation that actually trigger the exection of actions and transformation on RDDs.
-Actions are any other operations that do not return a Spark DataFrame.like displaying a DataFrame on the screen, writing it to storage, or triggering an intentional computation on a DataFrame and returning the result.

## SparkSession
- Its an entrypoint to the functionality provided by spark, it will give interface to interact with spark distributed data processing capabilities

## Data Skew
- Simulating  uniform Dataset
import pyspark.sql.functions as F
```
df_uniform= spark.range(100000)
df_uniform.show(3,False)
df_uniform
    .withColumn("partition",F.spark_partition_id())
    .groupBy("partition")
    .count()
    .orderBy("partition")
    .show()

```

Best Link--https://urlit.me/blog/pyspark-estimate-partition-count-for-file-read/

- Skewed Dataset

```
df0= spark.range(0,1000000).repartition(1)
df1= spark.range(0,10).repartition(1)
df2= spark.range(0,10).repartition(1)
df_skew= df0.union(df1).union(df2)
df_skew.show(3,False)
(
    df_skew 
    .withColumn("partition",F.spark_partition_id())
    .groupBy("partition")
    .count()
    .orderBy("partition")
    .show()
)
```
##Use partitioning when:

You have very large amounts of data.
Tables can be split into a few large partitions.

##Don't use partitioning when:
    - Data volumes are small.
    -  A partitioning column has high cardinality, as this creates a large number of partitions.
    -  A partitioning column would result in multiple levels.
    Partitions are a fixed data layout and don't adapt to different query patterns. When considering how to use partitioning, think about how your data is used, and its granularity.
    ```
    from pyspark.sql.functions import col,expr
    transformed_df=df.filter(col("price").isNotNull()) \
                    .withColumn("IsBike",expr("INSTR(Product,'Bike')>0").cast('int')) \
                    .withColumn("Total",expr("Quantity * Price").cast('decimal')) 

    df.write.format("delta").partitionBy("products").saveAsTable("partitioned_products", path="abfs_path/partitioned_products")
    ```

## Spark EMR Settings
- Support for cross account and federated role to access data in s3
- Support multi tenancy with multiple applciations and parallelism with multiple jobs
- Pre-initialized capacity for faster job starts and fine-graned scaling at job level.

```
aws emr-serverless --region us-east-1 create-application \
--type "SPARK" \
--name emr-disney-engagement-domain \
--release-label emr-6.8.0
--initial-capacity '{
        "DRIVER" :{
        "workerCount": 3,
        "workerConfiguration": {
            "cpu": 4vCPU",
            "memory" : "30GB"        
        }
},
    "EXECUTOR":{
        "workerCount": 100,
        "workerConfiguration": {
            "cpu": 4vCPU",
            "memory" : "30GB"        
        }

    }
}
```
# Tips for Shuffle managment
- Fewer Partitions like val keysMoved=KeysToMove.repartition(numPartitions).map{}
- Always fewer n/w requests and better compression
- Larger Tasks that leads to more memory needs
- approx 100 Mib per partition is ideal
- set default partition settings for RDD(Reselient Distribution Datasets)-- sqlContext.sparkContext.getConf.set("spark.default.parallelism","1024")
- set default parition setting for Dataset --sqlContext.sparkContext.getConf.set("spark.sql.shuffle.partitions","1024")
- more partition means more shuffle blocks and more n/w requests

# Shuffle with Coalesce
- Shuffle with coalesce --doesn't suffle data --reduces number of partitions but require large memory,it good to apply aggregation using coalesce.
- df.coalesce(numPartitions).combineByKey()
- Fewer shuffle partitions and doesn't shuffle data
-Larger task and it require more memory and it only descrese the partitions

# Perfoemance depends on below factors
- CPU Cores-If an Executor is allocated fewer CPU cores than it needs, it may become a bottleneck and slow down processing.
- Memory- If an Executor runs out of memory, it may need to spill data to disk, which can slow down processing.
- N/W-Slow network connections can cause delays in data transfer, which can slow down processing.
- Data Distribution-- If data is skewed, meaning that some nodes have more data to process than others, it can cause some Executors to become bottlenecks and slow down processing.
- Task Granularity --If tasks are too small, there may be too much overhead associated with task scheduling and data transfer, which can slow down processing. Conversely, if tasks are too large, they may take longer to complete, which can also slow down processing.
optimize the performance of Spark Executors, it’s important to balance the resources allocated to each Executor, tune the application to minimize data skew and optimize the task granularity, and choose the appropriate type of Executor based on the specific requirements of the application

- Always keep in mind, that the number of Spark jobs is equal to the number of actions in the application and each Spark job should have at least one Stage.
- The number of tasks you can see in each stage is the number of partitions that Spark is going to work on and each task inside a stage is the same work that will be done by Spark but on a different partition of data.

- RDD transformation are lazy,none of the transformation get executed until you call the action on RDD. as those immuatable any transformation will result in new RDD
# Two types of  Tranformations :-
    ##  Narrow transformations 
    - are result of map() and filter() functions and these compute data that live on a single partition meaning there will not be any data movement between partitions to execute narrow transformations. other map(),mapPartition(),filter(),flatMap(),union()

    -Wide Transformations are result of groupByKey(),reduceByKey() and CombineByKey() functions and these compute data that live on many partitions meaning there will be data movements between partitions to execute wide transformations. Since these shuffles the data, they also called shuffle transformations.
    other aggregateByKey(), aggregate(), join(), repartition() wide transformations are expensive operations due to shuffling.

/*
When the Spark application is executed, it creates a DAG. Then the DAG Scheduler creates stages, which are further divided into several tasks. A task is a unit of work that sends to the executor. The Task Scheduler schedules task execution on executors. Each stage has some task, one task per shuffle partition. The Same task is done over different partitions of RDD. Each task processes each shuffle partition.

The number of shuffle partitions depends on two properties.

spark.default.parallelism
spark.sql.shuffle.partitions
The property spark.default.parallelism was introduced with RDD hence this property is only applicable to RDD. This property defines the initial number of partitions created when creating an RDD. The default value for this configuration is set to the number of all cores on all nodes in a cluster. If you are running Spark locally, it is set to the number of cores on your system. You can set this property as below:

spark.default.parallelism = <<integer value>>

The property spark.sql.shuffle.partitions controls the number of shuffle partitions created after wide transformation for RDD and dataframe. Whenever wide transformations such as join(), agg(), etc., are executed by a Spark application, it generates N shuffle partitions where N is the value set by the spark.sql.shuffle.partitions property. Spark can potentially create N files in the output directory. If you are storing a dataframe in a partitioned table, the total number of files will equal N * <potential table partitions> in the output directory. The default value for this property is 200. You should set the number of shuffle partitions according to your cluster's data size and available resources. The number of partitions should be multiple of the number of executors you have so that partitions can be equally distributed across tasks.
*/
```rdd= spark.sparkContext.parallelize(range(0,20))
#rdd.collect()
print("no of partitions"+str(rdd.getNumPartitions()))
#print("spark.default.parallelism"+str(spark.default.parallelism))
#print("spark.sql.shuffle.partitions"+spark.conf("spark.sql.shuffle.partitions")
for i ,data in enumerate(rdd.glom().collect()):
    print("Partition{}:{}".format(i,data))

rdd2=rdd.repartition(4)
print("after no of partitions"+str(rdd2.getNumPartitions()))
for i ,data in enumerate(rdd2.glom().collect()):
    print("Partition{}:{}".format(i,data))
rdd3 = rdd.coalesce(4)
print("Number of partitions after coalesce: " + str(rdd3.getNumPartitions()))
for i ,data in enumerate(rdd3.glom().collect()):
        print("Partition{}:{}".format(i,data))

https://github.com/subhamkharwal/ease-with-apache-spark/blob/master/09_salting_technique.ipynb
```
In the PySpark example given below, I am processing 2GB of data. I have a dataframe called df. I want to repartition it and then store it on a table. This can be achieved in three steps, as given below.
Create a variable number_of_files and assign an integer value to it. Depending on the data, you need to tweak the value of this variable. I have set number_of_files = 10. Create a column having unique values, as shown below. I created the _unique_id column having unique values by using the monotonically_increasing_id() function.
``from pyspark.sql import functions as F
number_of_files = 10
df = df.withColumn('_unique_id', F.monotonically_increasing_id())```

2. After this, apply the salting technique to distribute records across partitions using a random value. In Spark, salting is a technique that adds random values to distribute Spark partition data evenly. For this, we need to create a derived column by taking the modulo value of the _unique_id column created above; not really a random number, but it works well. I have created a column called _salted_key in this example.

df = df.withColumn('_salted_key', F.col('_unique_id') % number_of_files)

3. Repartition dataframe based on _salted_key column.

df = df.repartition(number_of_files, '_salted_key') \                  
                           .drop('_unique_id', '_salted_key')

from pyspark.sql.functions import broadcast
small_df = spark.read.parquet("small_dataset.parquet")
result_df = large_df.join(broadcast(small_df), "common_column")

# Even with 2 dataframes of 10 rows each, you would end up having 64 number of partitions after a cross join.
from pyspark.sql.types import *
number of partitions (cross-joined df) =number of partitions(df1) x number of partitions(df2)
def beautiful_func(range1, range2):
    count1 = list(range(range1))
    count2 = list(range(range2))
    df1 = spark.createDataFrame(count1,IntegerType())
    df2 = spark.createDataFrame(count2,IntegerType())
    df1_num_partitions = df1.rdd.getNumPartitions() #getting the number of partitions of df1
    df2_num_partitions = df2.rdd.getNumPartitions() #getting the number of partitions of df2
    df1 = df1.repartition(df1_num_partitions) #repartitioning df1 with same number of partitions as earlier
    df2 = df2.repartition(df2_num_partitions) #repartitioning df2 with same number of partitions as earlier
    cj = df1.crossJoin(df2)
    print("number of partition of df1: " + str(df1.rdd.getNumPartitions()))
    print("number of partition of df2: " + str(df2.rdd.getNumPartitions()))
    print("number of partition after cross join: " + str(cj.rdd.getNumPartitions()))

### What AQE (Adaptive Query Execution) does is, it:
    Dynamically coalesce shuffle partitions
    Dynamically switching join strategies (changing physical plan midway!)
    Dynamically optimizing skew join
        Note: AQE cannot avoid shuffle because AQE gets statistics of data from output exchange which is after shuffling (end of that stage)

##  Driver Memory Allocation
The driver memory itself has 2 kinds of memories again:
Heap memory: Here all the JVM processes runs (and that’ll be your spark code).
Overhead memory: Here all the non JVM processes runs. This memory is used for shuffles, exchanges and network buffer.


## Executor Memory Allocation
Coming to executor memory, we have 4 kinds of memories:

Heap memory
Overhead memory
Offheap memory
pyspark memory
Note1: Say you ask for 8GBs of executor memory → 800MBs of overhead memory. If container size itself < 8GBs then YRM (YARN Resource Manager or Cluster Manager) cannot assign the asked executor memory (isn’t it!). So, always make sure you know about the cluster configuration when you go around manually specifying memories (just kidding guys 🤗)
Note2: pyspark is NOT a JVM process, so you get only the overhead memory for running pyspark code. If pyspark memory > overhead memory, then you will end up getting OOM Error (yeah, now you know why you get this error. Thank me later 😉)        

Reserved memory: 300MB and is fixed for spark engine.
Spark memory: 60% of (8GB-300MB). This memory is used for dataframe operations and caching. (This is where the magic happens ✨)
User memory: 40% of (8GB-300MB). This memory is used for user defined functions, spark internal metadata, RDD conversion operations and RDD lineage and dependencies.
sPARK MEMORY DIVIDED INTO TWO:-
    Storage memory pool: cache memory for dataframes
    Executor memory pool: This memory is used as buffer memory for dataframe operations.
    Note: Executor memory pool is short lived and is freed immediately as soon as execution is completed.
    The executor memory pool is divided further in slots which gets the tasks. Task distribution is done by Unified Memory Manager (UMM). The process is something like this:
    slots get the tasks >> tasks ask for memory from UMM >> UMM assigns some % of total executor pool memory to tasks.

rECURSIVE qUERY
with recursive nums as (
select 1 as num --base query
union All
select num+1 as num from nums where num < 10 --recursive query
)
select * from nums


WITH data AS (
    SELECT  *
    FROM
    (
        VALUES  (N'Alice', 250, 1, 250)
        ,   (N'Bob', 170, 2, 420)
        ,   (N'Alex', 350, 3, 770)
        ,   (N'John', 400, 4, 1170)
        ,   (N'Winston', 500, 5, 1670)
        ,   (N'Marie', 200, 6, 1870)
    ) t (name,weight,turn,weight_cumulative)

)
, cte_elevator AS (
    SELECT  name, weight, turn, weight AS total, 1 AS trip
    FROM    data d
    WHERE   turn = 1
    UNION ALL
    SELECT  d.name, d.weight, d.turn
    ,   CASE WHEN e.total + d.weight > 1000 THEN d.weight ELSE e.total + d.weight END as weight
    ,   CASE WHEN e.total + d.weight > 1000 THEN trip + 1 ELSE trip END as trip
    FROM    cte_elevator e
    INNER JOIN data d
        ON  d.turn = e.turn + 1
    )

select name from 
data d join (SELECT max(turn) as turn from cte_elevator group by trip) sq
on d.turn = sq.turn

# there are 2 kinds of actions:

Which brings data to driver node: collect(), take(), show(), count(), reduce() etc.
Which performs the actions on each partition/RDD: saveAsTextFile(), saveAsTable(), write(), foreach()
only foreach() is an action which allows you to perform transformation on RDDs before saving

read.csv() is NOT always a transformation. If you “inferSchema”, then its an Action
pivot() is not a transformation. Tt triggers a new job to “collect” the distinct values of column it needs to pivot. Pass the second parameter of pivot() to skip the collect() call.
foreach() is a neat-and-clean action which allows you to iterate over the dataset row by row to perform an IO operation
- Files Partition Size is a well known configuration which is configured through — spark.sql.files.maxPartitionBytes. The default value is set to 128 MB since Spark Version ≥ 2.0.0. 
# Check the default partition size
partition_size = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")
print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")
# Check the default parallelism available
print(f"Parallelism : {spark.sparkContext.defaultParallelism}")

# File size that we are going to import
import os
file_size = os.path.getsize('dataset/sales_combined_2.csv')
print(f"""Data File Size: 
            {file_size} in bytes 
            {int(file_size) / 1024 / 1024} in MB
            {int(file_size) / 1024 / 1024 / 1024} in GB""")
def get_time(func):n t
    def inner_get_time() -> str:
        start_time = time.time()
        func()
        end_time = time.time()
        print("-"*80)
        return (f"Execution time: {(end_time - start_time)*1000} ms")
    print(inner_get_time())
    print("-"*80)            

# Lets read the file and write in noop format for Performance Benchmarking
@get_time
def x():
    df = spark.read.format("csv").option("header", True).load("dataset/sales_combined_2.csv")
    print(f"Number of Partition -> {df.rdd.getNumPartitions()}")
    df.write.format("noop").mode("overwrite").save()            
The data was divided into 20 partitions (which is not factor of cores) and took around 11.5 seconds to complete the execution.    

- Execution 2: Increase the Partition Size to 3 times i.e. 384 MB
Lets increase the partition size to 3 times in order to see the effect.Copy
# Change the default partition size to 3 times to decrease the number of partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", str(128 * 3 * 1024 * 1024)+"b")

# Verify the partition size
partition_size = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")
print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")
# Lets read the file again with new partition size and write in noop format for Performance Benchmarking
@get_time
def x():
    df = spark.read.format("csv").option("header", True).load("dataset/sales_combined_2.csv")
    print(f"Number of Partition -> {df.rdd.getNumPartitions()}")
    df.write.format("noop").mode("overwrite").save()
    OK, it seems the number of partitions now decreased to 8 (factor of cores) and the time reduced to 9.5 seconds.
- Execution 3: Set the partition size to 160 MB    
# Change the default partition size to 160 MB to decrease the number of partitions
spark.conf.set("spark.sql.files.maxPartitionBytes", str(160 * 1024 * 1024)+"b")
# Verify the partition size
partition_size = spark.conf.get("spark.sql.files.maxPartitionBytes").replace("b","")
print(f"Partition Size: {partition_size} in bytes and {int(partition_size) / 1024 / 1024} in MB")
the job timing further reduced to 7 seconds and the partition is increased to 16 which is factor of cores.

In case of count(1) and count(*) we have almost same performance (as explain plans are same) but with count(col_name) we can have slight performance hit, but that’s OK as sometimes we need to get the correct count based on columns.
If you notice the highlighted segment in the explain plans, count(1) and count(*) has the same plan (function = [count(1)]) with no change at all, whereas in count(city_id) — Spark applies function=[count(city_id)], which has to iterate over column to check for null values.https://urlit.me/blog/