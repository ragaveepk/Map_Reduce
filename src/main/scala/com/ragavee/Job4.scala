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
import java.time.LocalTime

/**
 * This class denotes the mapper  and reducer classes which outputs
 * number of characters in each log message for each type
 * that contain the highest number of characters in the detected instances of the designated regex pattern..
 *
 **/
class Job4

object Job4
{
  class Job4Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val conf: Config = ConfigFactory.load("application.conf")
    val key_value = new IntWritable(1)
    val KEY = new Text()
    val GROUP_THREE = conf.getInt("configuration.GROUP_THREE")
    val GROUP_FOUR = conf.getInt("configuration.GROUP_FOUR")

 //This map function takes the log message which matches the injected pattern  and
 // writes key value pair with key as type of message and value as the length of each message
    override def map(key: Object, value: Text,context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val regexPattern: Regex = conf.getString("configuration.keyValPattern").r
      val Pattern =  regexPattern.findFirstMatchIn(value.toString)
      val injectedPattern  : Regex = conf.getString("configuration.injected_pattern").r

      Pattern.toList.map(x => {
        injectedPattern.findFirstMatchIn(x.group(5)) match {
          case Some(_) => {
              val temp = x.group(GROUP_FOUR).replace(" ","").length
              KEY.set(x.group(GROUP_THREE))
              context.write(KEY, new IntWritable(temp))
          }
          case None => { }
        }
      })
    }
  }

//This reducer function combines output of mapper function by finding the max of the values in each type.
  class Job4Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {
      val finalVal = values.asScala.foldLeft(0)(_ max _.get)
      context.write(key, new IntWritable(finalVal))
    }
  }

  // This Partitioner class  takes place after Map phase and before reduce phase.
  // It divides the data according to the number of partitioner ( # of partitioner  = # of reducers )
  class Job4Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      //input  key value paired data can be  divided into 2 parts based on message type
      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }
}