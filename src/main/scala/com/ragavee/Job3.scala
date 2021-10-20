package com.ragavee


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.mapreduce.{Job, Mapper, Partitioner, Reducer}

import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.lang.Iterable

class Job3

object Job3
{
  class Job3Mapper extends Mapper[Object, Text, Text, IntWritable] {
    val conf: Config = ConfigFactory.load("application.conf")
    val one = new IntWritable(1)
    val word = new Text()

    override def map(key: Object,value: Text,context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val RegexPattern: Regex = conf.getString("configuration.keyValPattern").r
      val matchPattern =  RegexPattern.findFirstMatchIn(value.toString)

      matchPattern.toList.map(x => {
        word.set(x.group(3))
        context.write(word, one)
      })
    }
  }

  class Job3Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val sum = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(sum))
    }
  }
  class Job3Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {

      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }
}