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

