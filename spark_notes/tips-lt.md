# Tips refresh
- Joins
  - Shuffle Hash Join--Two small datasets then
  - BoradcastJoin --One Big and one small under 10MB
  - SortMergeJoin-- Two Big datasets--Bucketing Data is the right Way
  - Notes: 1) Joining Column different han Bucket column, same Bucket Size-shufle on Both table
           2) Join Column Same,one table in bucket-shuffle on non bucket table
           3) Join column Same,differen bucket size- shuffle on smaller bucket side
           4) Join column same,same bucket size-No shuffle (faster join)

```
sales.write.format("csv").mode("overwrite").bucketBy(4, "city_id").option("header", True).option("path", "/data/input/datasets/sales_bucket.csv").saveAsTable("sales_bucket")
city.write.format("csv").mode("overwrite").bucketBy(4, "city_id").option("header", True).option("path", "/data/input/datasets/city_bucket.csv").saveAsTable("city_bucket")
sales_bucket = spark.read.table("sales_bucket")
df_joined_bucket = sales_bucket.join(city_bucket, on=sales_bucket.city_id==city_bucket.city_id, how="left_outer")
df_joined_bucket.write.format("noop").mode("overwrite").save()
```

# Transformation
- Narrow-- if one partition is contribution to only at-most one partition
- Wide-- if one partition is contributing to more than one partition , this type operation result into shuffle.

# Dataframe
emp_data=[
    [1,"ravi",40],
    [2,"jannu",70],
]
emp_schema="emp_no integer,emp_name string,salary double"
- spark.createDataFrame(data=emp_data,schema=emp_schema)
- df.rdd.getNumPartitions()
- emp_final=df.where("salary >10")
- ravi=spark.getActiveSession()

# define schema
- pyspark.sql.types import StructType,StructField,StringType,IntergerType
- schema_cust=StructType[
    StructField("name",StringType(),True),
    StructField("sal",IntegerType(),True)
]
# using select, selectExpr,col
- pyspark.sql.functions import col,expr
- df.select("emp_no",col("salary"),df.emp_name)
- emp_casted=emp_sel.select(expr("emp_no as emp_number"))
- emp_casted2=emp_sel.selectExpr("cast(salary as string)")
- emp_casted23=emp_sel.select("emp_no","emp_name").where("salary>10")
- from pyspark.sql.types import _parse_datatype_string //implicit datatype conversion
- schema_str="name string, age int"
- schema_spark= _parse_datatype_string(schema_str)
- df.select("emp_no",col("salary).cast("double"))
- df.withColumn("tax",col("sal")*0.2)
- df.withColumnRenamed("test1","test2")
- columns ={ "tax":col("sal")*0.2, "onecol":lit(1)}
- df.withColumns(columns) # to Add multiple columns
- df.withColumn("new_gender",expr("case when gender='M' then 'MM' when gender='F' then 'FF' else null end"))
- df.withcolum("newload",current_date()).withcolum("newload2",to_date(col("datecol"),"yyyy-MM-dd"))
- df.withColumn("date_string",date_format(col("hire_date"),"yyyy-dd-MM")).withColumn("date_year,date_format(col("hire_date"),"yyyy"))
- df=emp.orderBy(col("salary").asc())
- emp_cnt=emp.groupBy("dept_id").agg(count("emp_id").alias("count"))
- emp_dist=df.distinct() or emp.select("dept_id").distinct()

# window functions
- pyspark.sql.window import Window
- pyspark.sql.functions import col,asc,desc,row_number
- windowspec=Window.partitionBy(col("dept_id")).orderBy(col("sal").desc())
- maxfunc=max(col("sal")).over(windowspec)
- emp_1=emp.withColumn("max_sal",maxfunc)
- windowspec=Window.partitionBy(col("dept_id")).orderBy(col("sal").desc())
- rn=row_number().over(windowspec)
- emp_1=emp.withColumn("rn",rn).where("rn=2")
- df.repartition(4,"dept-id")
- df.coalesce(2)
# Find the partition infor for paritions
- from pyspark.sql.functions import spark_parition_id
- emp_1=emp.withColumn("parition_id",spark_partition_id())

# read modes
read_csv=spark.read.format("csv").option("inferSchema",True).option("header",True).load("")
- df_p.where("_corrupt_record is null").show()
```emp = spark.read.format(file_type) \
  .option("inferSchema", infer_schema) \
  .option("header", first_row_is_header) \
  .option("columnNameOfCorruptRecord","bad_record") \
  .option("mode","DROPMALFORMED") \ drops the bad record
  .option("mode","FAILFAST") \ fail the app as soon as it encounters the error
  .option("sep", delimiter) \
  .load(file_location)

  bonus
  _options={
    "header"="True",
    "mode"="DROPMALFORMED"
    "inferSchema"="True"
  }
  spark.read.format("csv").options(**_options).load("data/test.csv")
  ---------|-parquet-------|- orc-------|---avro----|
  compression|better------|-----Best----|--Good-----|
  R/W-------|-read------|-----heavy read-|--write-----|
  row/col----|-Columanr--|-----Columnar-|--Row-----|
  schema evol----|-good------|-----better-|--best-----|
  usage-------|-delta------|-----Hive-|--Kafka-----|

  ```


  ```

import time

def time_it(func):
    def inner_get_time()-> str:
        start_st=time.time()
        func()
        end_et=time.time()
        return f"Total it took for execution: {(end_et - start_st) * 1000} ms"
    print(inner_get_time())

@time_it
def x():
    ept=spark.read.format("csv").option("inferSchema",True).option("header",True).load("dbfs:/FileStore/tables/department_data.txt")
    ept.count()    
  ```