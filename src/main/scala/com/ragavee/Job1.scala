package com.ragavee

import org.apache.hadoop.io.{IntWritable, Text}

import org.apache.hadoop.mapreduce.{Mapper, Reducer}

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.lang.Iterable

class Job1

object Job1 {
  class Job1Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object,
                     value: Text,
                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val keyValPattern: Regex = "(^\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s\\[([^\\]]*)\\]\\s(WARN|INFO|DEBUG|ERROR)\\s+([A-Z][A-Za-z\\.]+)\\$\\s-\\s(.*)".r
      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")

      val startTime = LocalTime.parse("19:55:33.412", formatter)
      val endTime = LocalTime.parse("20:11:06.281", formatter)

      val patternMatch = keyValPattern.findFirstMatchIn(value.toString)
      patternMatch.toList.map(x => {
        val time = LocalTime.parse(x.group(1), formatter)
        if (startTime.isBefore(time) && endTime.isAfter(time)) {
          word.set(x.group(3))
          context.write(word, one)
        }
      })
    }
  }

  class Job1Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }

//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration()
//    val job = Job.getInstance(configuration, "job1")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass(classOf[Job1Mapper])
//    job.setCombinerClass(classOf[Job1Reducer])
//    job.setReducerClass(classOf[Job1Reducer])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable])
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    System.exit(if (job.waitForCompletion(true)) 0 else 1)
//  }
}

