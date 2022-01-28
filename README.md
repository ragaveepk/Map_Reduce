# Map_Reduce

## Introduction
This project consists in creating different map/reduce jobs in order to produce various statistics . The hadoop framework has been extensively used as part of this work in Scala language.

## Documentation
After Cloning the project , please find the Scaladoc documentation in docs folder.

## Setup Requirements
- HDP Sandbox
- VM Ware Workstation Pro
- A copy of 2 LogfileGenerator files (present under src/main/resources of this project)
- ssh and scp installed and working on the user's system.

## How to Install

Please follow the following steps to run the project
- Use IntelliJ IDEA with the Scala plugin installed along with sbt.
- Enter the following URL and click “Clone”: https://github.com/ragaveepk/cs441_homework2.git

- When prompted click “Yes” in the dialog box

- The SBT import screen will appear, so proceed with the default options and click on “OK”

- Go to src/main/scala/ and open the MapperReducer class.

- An IntelliJ run configuration is automatically created when you click the green arrow next to the main method of the driver class.

- run sbt clean  and sbt compile in the terminal

## Steps to  create Jar File

A Jar file needs to be created so you can run the map/reduce jobs on a virtual machine that provides an Hadoop installation.
Please follow these steps:

1) Enter git clone https://github.com/ragaveepk/Map_Reduce.git

2) Run the following command: "sbt clean compile assembly"

3) A Jar will be created under the target/scala-2.13/ folder with the following name: Homework2.jar
 
## Run the TEST file
- Open Terminal in your Intellij and type 'sbt  clean compile test' and run

## Deployment instructions (HDP Sandbox)

1) Make sure the HDP Sandbox is running and it has fully started. This make take a while.

2) You may check by logging in the web panel and checking the status on the "Start services" task.

3) Copy the jar file to the Sandbox by issuing the following command: "scp -P 2222 target/scala-2.13/Map_reduce.jar root@192.168.245.129:" 
- This IP address is basically to SSH into the active Hortonworks Sandbox session ,you can find it in the VMWare Workstation once the virtual machine with the sandbox is powered up

4) Copy the LogfileGenerator to the Sandbox: "scp -P 2222 LogFileGenerator.2021-10-19.log root@192.168.245.129:"

5) Login into the Sandbox: ssh -p 2222 root@192.168.245.129. You may be asked for a password if you have not set up SSH keys. The default password is: hadoop

6) Create the input directory on HDFS: hdfs dfs -mkdir input_directory

7) Load the dataset on HDFS: hdfs dfs -put LogFileGenerator.2021-10-19.log input_directory/
  - Load the dataset on HDFS: hdfs dfs -put LogFileGenerator.2021-10-17.log input_directory/

8) Now run: hadoop jar Map_reduce.jar

9) After completion the results are saved in a folder named output_directory on HDFS.

10) To copy output to local storage by giving the following command: "hdfs dfs -get output_directory output"

11) You can convert tht output files to CSV format  using the following command :  hadoop fs -cat output_directory/job1/* > output/job1.csv

12) Similarly, you can create csv files for the remaining jobs by changing the job names

13) csv files will be created under "output" folder

14) Exit from the SSH terminal, "exit"

15) To view the output of CVS file in terminal use "cat job1.csv"

16) You can download the output files to your local system using scp -P 2222 -r root@192.168.245.129:/root/output <local_path>

## Map-Reduce Jobs

As part of this homework a total of  map/reduce jobs were created. The association between the classes involved and each job is clearly listed in the MapReduceJobsDriver class.

1) Job 1: To find distribution of different types of messages across predefined time intervals and injected string instances of the designated regex pattern for these log message types
      - The Mapper outputs the list of number messages which matches the injected regex pattern and time lies between the predefined time interval
      - In Reducer, reduce function basically the reduces the key value pair by checking whether the log message matches the injected string pattern if yes then checks the time is between the start and end time
        and then writes message type as key, total number of messages.

2) Job 2: To find List of the most contained log messages of type ERROR which matches the injected regex pattern in the specified time interval. 
    Mapper Class - Map function gets the input data shards as input and records the occurences of recorded ERROR type messages for every each hour in the log file
    Reducer class - Reducer reduces the key-value pair by adding the values of same keys to get aggregated result of key value pairs
    Mapper1 Class - Mapper1 takes the output of the reducer which is unsorted order and sorts based on the values in descending order by multiplying the values by -1 and switching the key and values pairs in the list
    Reducer1 class - Reducer1 takes the output of the mapper1 where values are  keys here which will be sorted and finally switching the key values pairs by multiplying the values again by -1

3) Job 3: To find the List of  number log messages generated in each message type.
    Mapper Class -   Map functions counts the occurrence of each message type - INFO, WARN, DEBUG, ERROR and creates list of key value pairs message type as key and occurrence of each message type.
    Reducer Class -  Reduce function combines the output of mapper class by adding the number messages for each type.

4) Job 4: To find the List the number of characters in each log message for each log message type that contain the highest number of characters in the detected instances of the designated regex pattern.
    Mapper Class -  This map function takes the log message which matches the injected pattern and writes key value pair with key as type of message and value as the length of each message
    ReducerClass -  This reducer function combines output of mapper function by finding the max of the values list of each message type. 

The Partitioner class  has been written each job class which takes place after Map phase and before reduce phase.
- It divides the data according to the number of partitioner (# of partitioner  = # of reducers)
-In this project, Input key value paired data can be divided into 2 parts based on Message type 


## Output

Please find the output files located in this project under the folder named output_directory. This folder contains output files of specific job folders and also csv files for the respective jobs.

