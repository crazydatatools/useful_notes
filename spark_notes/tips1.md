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
