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

/**
 * This class denotes the mapper  and reducer classes which outputs
 * the number of generated log messages for each message type.
 *
 **/

class Job3

object Job3
{
  class Job3Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val conf: Config = ConfigFactory.load("application.conf")
    val GROUP_THREE = conf.getInt("configuration.GROUP_THREE")
    val key_value = new IntWritable(1)
    val KEY = new Text()

// Map functions counts the occurrence of each message type -
// INFO, WARN, DEBUG, ERROR and creates list of key value pairs
    override def map(key: Object,value: Text,context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val regexPattern: Regex = conf.getString("configuration.keyValPattern").r
      val matchPattern =  regexPattern.findFirstMatchIn(value.toString)

      matchPattern.toList.map(x => {
        KEY.set(x.group(GROUP_THREE))
        context.write(KEY, key_value)
      })
    }
  }

// Reduce function combines the output of mapper class by adding the number messages of each type.
  class Job3Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val finalVal = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(finalVal))
    }
  }


  // This Partitioner class  takes place after Map phase and before reduce phase.
  // It divides the data according to the number of partitioner (# of partitioner  = # of reducer)

  class Job3Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      //input  key value paired data can be  divided into 2 parts based on message type
      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }
}