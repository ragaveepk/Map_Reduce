import com.typesafe.config.{Config, ConfigFactory}
import com.ragavee.Job1.{Job1Mapper, Job1Partitioner, Job1Reducer}
import com.ragavee.{Job1, Job2, Job3, Job4}
import com.ragavee.Job2.{Job2Partitioner, Job2Mapper, Job2Mapper1, Job2Reducer, Job2Reducer1}
import com.ragavee.Job3.{Job3Mapper, Job3Partitioner, Job3Reducer}
import com.ragavee.Job4.{Job4Mapper, Job4Partitioner, Job4Reducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}



/**
 *
 * This class contains the Map/Reduce driver for all the jobs assigned.
 *
 * The jobs implemented are as follows:
 * - Job1 - the distribution of different types of messages across predefined time intervals
 *            and injected string instances of the designated regex pattern for these log message types
 * - Job2 -  time intervals sorted in the descending order that
 *          contained most log messages of the type ERROR with injected regex pattern string instances.
 * - Job3 -  each message type you will produce the number of the generated log messages.
 * - Job4 - List the number of characters in each log message for each log message type that contain
 *          the highest number of characters in the detected instances of the designated regex pattern.
 */
class MapperReducer

object MapperReducer {

  val logger: Logger = LoggerFactory.getLogger(getClass)
  val conf: Config = ConfigFactory.load("application.conf")

  val inputFile: String = conf.getString("configuration.inputFile")
  val outputFile: String = conf.getString("configuration.outputFile")
  val verbose: Boolean = true

  def main(args: Array[String]): Unit = {
    val startTime = System.nanoTime
    logger.info("--- Starting MapReduceJobsDriver ---")


    val configure: Configuration = new Configuration()

    //Format as CSV output
    configure.set("mapred.textoutputformat.separator", ",")

    val job1Name = conf.getString("configuration.job1")
    val job2Name = conf.getString("configuration.job2")
    val job3Name = conf.getString("configuration.job3")
    val job4Name = conf.getString("configuration.job4")
    val job2aName = conf.getString("configuration.job2a")

    /**
     * Job 1 - spreadsheet or an CSV file that list the number of type of messages in the predefined time interval
     */
    logger.info("--- Job 1 Starting---")
    val job1: Job = Job.getInstance(configure, job1Name)
    job1.setJarByClass(classOf[Job1])
    job1.setMapperClass(classOf[Job1Mapper])
    job1.setCombinerClass(classOf[Job1Reducer])
    job1.setPartitionerClass(classOf[Job1Partitioner])
    job1.setNumReduceTasks(2)
    job1.setReducerClass(classOf[Job1Reducer])
    job1.setOutputKeyClass(classOf[Text]);
    job1.setOutputValueClass(classOf[IntWritable]);
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job1, new Path(inputFile))
    FileOutputFormat.setOutputPath(job1, new Path((outputFile + "/" + job1Name)))
    job1.waitForCompletion(true)

    /**
     * Job 2 - CSV file which displays list of number of ERROR type messages in the descending order
     */
    val job2: Job = Job.getInstance(configure, job2Name)
    logger.info("--- JOb1 Completed Successfully--")

    logger.info("--- Job 2 Starting---")

    job2.setJarByClass(classOf[Job2])
    job2.setMapperClass(classOf[Job2Mapper])
    job2.setCombinerClass(classOf[Job2Reducer])
    job2.setPartitionerClass(classOf[Job2Partitioner])
    job2.setNumReduceTasks(2)
    job2.setReducerClass(classOf[Job2Reducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job2, new Path(inputFile))
    FileOutputFormat.setOutputPath(job2, new Path((outputFile + "/" + job2Name)))
    job2.waitForCompletion(true)

    val configuration1 = new Configuration
    configuration1.set("mapred.textoutputformat.separator", ",")
    val job2a = Job.getInstance(configuration1,job2Name)
    logger.info("--- Completed First MapReduce of JOb 2--Unsorted list---")

    logger.info("--- Started Second MapReduce of Job 2---")

    job2a.setJarByClass(classOf[Job2])
    job2a.setMapperClass(classOf[Job2Mapper1])
    job2a.setReducerClass(classOf[Job2Reducer1])
    job2a.setMapOutputKeyClass(classOf[IntWritable])
    job2a.setMapOutputValueClass(classOf[Text])
    job2a.setOutputKeyClass(classOf[Text])
    job2a.setOutputValueClass(classOf[IntWritable]);
    FileInputFormat.addInputPath(job2a, new Path(outputFile + "/" + job2Name))
    FileOutputFormat.setOutputPath(job2a, new Path(outputFile + "/" + job2aName))
    job2a.waitForCompletion(true)
    logger.info("--- Job 2 Completed Successfully--")

    /**
     * Job 3 - each message type you will produce the number of the generated log messages.
     */

    logger.info("--- Job 3 Starting---")
    val job3: Job = Job.getInstance(configure, job3Name)
    job3.setJarByClass(classOf[Job3])
    job3.setMapperClass(classOf[Job3Mapper])
    job3.setCombinerClass(classOf[Job3Reducer])
    job3.setPartitionerClass(classOf[Job3Partitioner])
    job3.setNumReduceTasks(2)
    job3.setReducerClass(classOf[Job3Reducer])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job3, new Path(inputFile))
    FileOutputFormat.setOutputPath(job3, new Path((outputFile + "/" + job3Name)))
    job3.waitForCompletion(true)
    logger.info("--- Job 3 Completed Successfully--")
    /**
     * Job 4 - List the number of characters in each message for each type that contain
     *          the highest number of characters in the detected instances of the designated regex pattern.
     */
    logger.info("--- Job 4 Starting---")
    val job4: Job = Job.getInstance(configure, job4Name)
    job4.setJarByClass(classOf[Job4])
    job4.setMapperClass(classOf[Job4Mapper])
    job4.setCombinerClass(classOf[Job4Reducer])
    job4.setPartitionerClass(classOf[Job4Partitioner])
    job4.setNumReduceTasks(2)
    job4.setReducerClass(classOf[Job4Reducer])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job4, new Path(inputFile))
    FileOutputFormat.setOutputPath(job4, new Path((outputFile + "/" + job4Name)))
    job4.waitForCompletion(true)
    logger.info("--- Job 4 Completed Successfully--")

      val endTime = System.nanoTime
      val totalTime = endTime - startTime
      logger.info("--- SUCCESSFULLY COMPLETED (Execution completed in: " + totalTime / 1_000_000_000 + " sec) ---")
  }
}