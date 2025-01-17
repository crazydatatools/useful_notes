STEP 1: Number of executor cores required?
We start by deciding how many executor cores we need 🤔

One partition is of 128MB of size by default — important to remember
To calculate number of cores required, you have to calculate total number of partitions you will end up having
100GB = 100*1024 MB = 102400MB
Number of partitions = 102400/128 = 800
Therefore, 800 executor cores in total are required

STEP 2: Number of executors required?
Now that we know how many cores, next we need to find how many executors are required.

On an average, its recommended to have 2–5 executor cores in one executor
If we take number of executors cores in one executor = 4 then, total number of executors = 800/4 = 200
Therefore, 200 executors are required to perform this task
Obviously, the number will vary depending on how many executor cores do you keep in one executor 😉

STEP 3: Total executor memory required?
Important step! how much memory to be assigned to each executor 🤨

By default, total memory of an executor core should be
4 times the (default partition memory) = 4*128 = 512 MB

Therefore, total executor memory = number of cores*512 = 4*512 = 2GB
SUMMARIZE: Total memory required to process 100GB of data
We are here! 🥳lets finalize on total memory to be required to process 100GBs of data

Each executor has 2GB of memory
We have total of 200 executors
Therefore, 400GB of total minimum memory required to process 100GB of data completely in parallel.

BONUS STEP: What should be the driver memory?
This depends on your use case.
If you do df.collect(), then 100Gbs of driver memory would be required since all of the executors will send their data to driver
If you just export the output somewhere in cloud/disk, then driver memory can be ideally 2 times the executor memory = 4GB
And that my friend, is how you efficiently process 100GBs of data 😉

# SCD Types
### First way — Type1
Diary entry:

Initial: Perry wore an orange hat
Overwrite: Perry wore a red hat
Example: Your record only shows Perry’s hat is red now. The old record showing the orange hat is gone.

Summary: SCD Type 1 updates the existing record with the most recent information, losing the previous data.

### Second way — Type 2 (The best way?)
Initial State: Perry’s hat is orange. (day1)
Change: Perry’s hat is now red. (day2)
How It Works: With Type 2, you add a new record (a new row) for each change, keeping all historical data.
Example:

Record 1: Perry’s hat is orange (old information).
Record 2: Perry’s hat is red (current information).
Summary: SCD Type 2 keeps all versions of the data, so you can see how it has changed over time. 😮
In real world, you add 2 new columns: “start_date” and “end_date” which states that the particular record was valid from this date to this date.

### Third way — Type 3
Scenario: Here, Ferb wants to track Perry’s current hat color and also keep a record of the previous color.

Initial State: Perry’s hat is orange.
Change: Perry’s hat is now red.
How It Works: With Type 3, you add a new column to your record to keep the previous value along with the current value.
ummary: SCD Type 3 keeps both the current and the previous value in the same record, providing a snapshot of the most recent change along with the last known value.

In real world, you would create two new columns which would save “current” and “previous” hat colors. 😵

One major disadvantage of type3 is that for day3, we’ll lose day1’s data since we are only having current and previous data entry