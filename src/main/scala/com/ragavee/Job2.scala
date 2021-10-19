package com.ragavee

import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Reducer}

import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.lang.Iterable

class Job2

object Job2 {

  class Task2Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
      val keyValPattern: Regex = "(^\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s\\[([^\\]]*)\\]\\s(WARN|INFO|DEBUG|ERROR)\\s+([A-Z][A-Za-z\\.]+)\\$\\s-\\s(.*)".r
      val p = keyValPattern.findAllMatchIn(value.toString)
      p.toList.map((pattern) => {

        word.set(pattern.group(1).split(":")(0))
        context.write(word,one)
      })

    }
  }

  class Task2Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      var sum = values.asScala.foldLeft(0)(_ + _.get())
      context.write(key, new IntWritable(sum))
    }
  }

  class Task2Mapper1 extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {

      val line = value.toString.split("\t")
      val result = line(1).toInt * -1
      context.write(new IntWritable(result), new Text(line(0)))

    }
  }

  class Task2Reducer1 extends Reducer[IntWritable,Text,Text,IntWritable] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {
      values.asScala.foreach(value => context.write(value, new IntWritable(key.get() * -1)))
    }
  }


//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration
//    val job = Job.getInstance(configuration,"word count")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass(classOf[Task2Mapper])
//    job.setCombinerClass(classOf[Task2Reducer])
//    job.setReducerClass(classOf[Task2Reducer])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable]);
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    if(job.waitForCompletion(true)){
//      val configuration1 = new Configuration
//      val job1 = Job.getInstance(configuration1,"word count")
//      job1.setJarByClass(this.getClass)
//      job1.setMapperClass(classOf[Task2Mapper1])
//      job1.setReducerClass(classOf[Task2Reducer1])
//      job1.setOutputKeyClass(classOf[Text])
//      job1.setOutputValueClass(classOf[IntWritable]);
//      FileInputFormat.addInputPath(job, new Path(args(1)))
//      FileOutputFormat.setOutputPath(job, new Path(args(2)))
//      System.exit(if(job1.waitForCompletion(true))  0 else 1)
//
//    }
//  }

}

//object com.ragavee.Job2
//{
//  class Job2Mapper extends Mapper[Object, Text, Text, IntWritable] {
//
//    val one = new IntWritable(1)
//    val word = new Text()
//
//    override def map(key: Object,
//                     value: Text,
//                     context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {
//
//      val keyValPattern: Regex = "(^\\d{2}:\\d{2}:\\d{2}\\.\\d{3})\\s\\[([^\\]]*)\\]\\s(ERROR)\\s+([A-Z][A-Za-z\\.]+)\\$\\s-\\s(.*)".r
//      val patternMatch =  keyValPattern.findFirstMatchIn(value.toString)
//      patternMatch.toList.map(x => {
//        val time = x.group(1).split(":")(0)
//        word.set(time)
//        context.write(word, one)
//      })
//    }
//  }
//
//  class Job2Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
//    override def reduce(key: Text, values: Iterable[IntWritable],
//                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
//      var sum = values.asScala.foldLeft(0)(_ + _.get)
//      context.write(key, new IntWritable(sum))
//    }
//  }
//
//  def main(args: Array[String]): Unit = {
//    val configuration = new Configuration()
//    val job = Job.getInstance(configuration, "job2")
//    job.setJarByClass(this.getClass)
//    job.setMapperClass    (classOf[Job2Mapper])
//    job.setCombinerClass(classOf[Job2Reducer])
//    job.setReducerClass(classOf[Job2Reducer])
//    job.setOutputKeyClass(classOf[Text])
//    job.setOutputValueClass(classOf[IntWritable])
//    FileInputFormat.addInputPath(job, new Path(args(0)))
//    FileOutputFormat.setOutputPath(job, new Path(args(1)))
//    System.exit(if(job.waitForCompletion(true)) 0 else 1)
//  }
//
//}