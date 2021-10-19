import com.typesafe.config.{Config, ConfigFactory}
import com.ragavee.Job1.{Job1Mapper, Job1Reducer}
import com.ragavee.{Job1, Job2, Job3, Job4}
import com.ragavee.Job2.{Task2Mapper, Task2Mapper1, Task2Reducer, Task2Reducer1}
import com.ragavee.Job3.{Job3Mapper, Job3Reducer}
import com.ragavee.Job4.{Job4Mapper, Job4Reducer}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{DoubleWritable, IntWritable, Text}
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.{FileOutputFormat, TextOutputFormat}
import org.slf4j.{Logger, LoggerFactory}



/**
 *
 * This class contains the Map/Reduce driver for all the tasks assigned.
 *
 * The jobs implemented are as follows:
 * - Top 10 publishers at each venue (job1)
 * - List of authors who published without interruption for 10 consecutiveYears (job2)
 * - List of publications that contains only one author (job3)
 * - List of publications for each venue that contain the highest number of authors for each of the venues (job4)
 * - Top 100 authors with most number of Co-Authors (job5)
 * - Top 100 authors with most number of Co-Authors in descending order(job5a)
 * - Top 100 authors with no Co-Authors (job5b)
 *
 */
class MapperReducer

object MapperReducer {

  val logger: Logger = LoggerFactory.getLogger(getClass)
//  val logger : Logger = LoggerFactory
  val conf: Config = ConfigFactory.load("application.conf")

  val inputFile: String = conf.getString("configuration.inputFile")
  val outputFile: String = conf.getString("configuration.outputFile")
  val verbose: Boolean = true
//  val consecutiveYearsOfPublishing: Int = conf.getInt("configuration.consecutiveYears")

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

    /**
     * Job 1 - spreadsheet or an CSV file that shows top ten published authors at each venue
     */
    val job1: Job = Job.getInstance(configure, job1Name)
    job1.setJarByClass(classOf[Job1])
    job1.setMapperClass(classOf[Job1Mapper])
    job1.setCombinerClass(classOf[Job1Reducer])
    job1.setReducerClass(classOf[Job1Reducer])
    job1.setOutputKeyClass(classOf[Text]);
    job1.setOutputValueClass(classOf[IntWritable]);
    job1.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job1, new Path(inputFile))
    FileOutputFormat.setOutputPath(job1, new Path((outputFile + "/" + job1Name)))


    /**
     * Job 2 - list of authors who published without interruption for N consecutiveYears where 10 <= N
     */
    val job2: Job = Job.getInstance(configure, job2Name)
    job2.setJarByClass(classOf[Job2])
    job2.setMapperClass(classOf[Task2Mapper])
    job2.setCombinerClass(classOf[Task2Reducer])
    job2.setReducerClass(classOf[Task2Reducer])
    job2.setOutputKeyClass(classOf[Text])
    job2.setOutputValueClass(classOf[IntWritable])
    job2.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job2, new Path(inputFile))
    FileOutputFormat.setOutputPath(job2, new Path((outputFile + "/" + job2Name)))
    if(job2.waitForCompletion(true)){
      val configuration1 = new Configuration
      val job2a = Job.getInstance(configuration1,"word count")
      job2a.setJarByClass(classOf[Job2])
      job2a.setMapperClass(classOf[Task2Mapper1])
      job2a.setReducerClass(classOf[Task2Reducer1])
      job2a.setOutputKeyClass(classOf[Text])
      job2a.setOutputValueClass(classOf[IntWritable]);
      FileInputFormat.addInputPath(job2, new Path(outputFile + "/" + job2Name))
      FileOutputFormat.setOutputPath(job2, new Path(outputFile + "/" + job2Name+"a"))

    }
    /**
     * Job 3 - List of publications that contains only one author
     */
    val job3: Job = Job.getInstance(configure, job3Name)
    job3.setJarByClass(classOf[Job3])
    job3.setMapperClass(classOf[Job3Mapper])
    job3.setCombinerClass(classOf[Job3Reducer])
    job3.setReducerClass(classOf[Job3Reducer])
    job3.setOutputKeyClass(classOf[Text])
    job3.setOutputValueClass(classOf[IntWritable])
    job3.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job3, new Path(inputFile))
    FileOutputFormat.setOutputPath(job3, new Path((outputFile + "/" + job3Name)))

    /**
     * Job 4 - List of publications for each venue that contain the highest number of authors for each of these venues
     */
    val job4: Job = Job.getInstance(configure, job4Name)
    job4.setJarByClass(classOf[Job4])
    job4.setMapperClass(classOf[Job4Mapper])
    job4.setCombinerClass(classOf[Job4Reducer])
    job4.setReducerClass(classOf[Job4Reducer])
    job4.setOutputKeyClass(classOf[Text])
    job4.setOutputValueClass(classOf[IntWritable])
    job4.setOutputFormatClass(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.addInputPath(job4, new Path(inputFile))
    FileOutputFormat.setOutputPath(job4, new Path((outputFile + "/" + job4Name)))


    if (job1.waitForCompletion(verbose) && job2.waitForCompletion(verbose) && job3.waitForCompletion(verbose) && job4.waitForCompletion(verbose)) {
      val endTime = System.nanoTime
      val totalTime = endTime - startTime
      logger.info("--- SUCCESSFULLY COMPLETED (Execution completed in: " + totalTime / 1_000_000_000 + " sec) ---")
    }
    else {
      logger.info("--- UNFORTUNATELY FAILED ---")
    }
  }
}