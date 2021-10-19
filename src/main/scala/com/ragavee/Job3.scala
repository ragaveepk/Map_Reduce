package com.ragavee


import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Reducer}
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.lang.Iterable

class Job3

object Job3
{
  class Job3Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = "(^\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s\\[([^\\]]*)\\]\\s(ERROR)\\s+([A-Z][A-Za-z\\.]+)\\$\\s-\\s(.*)".r
      val patternMatch =  keyValPattern.findFirstMatchIn(value.toString)
      patternMatch.toList.map(x => {
        word.set(x.group(2))
        context.write(word, one)
      })
    }
  }

  class Job3Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration()
//    val job = Job.getInstance(configuration, "job3")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass    (classOf[Job3Mapper])
//    job.setCombinerClass(classOf[Job3Reducer])
//    job.setReducerClass(classOf[Job3Reducer])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable])
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    System.exit(if(job.waitForCompletion(true)) 0 else 1)
//  }

}