package com.ragavee

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Partitioner, Reducer}

import java.time.LocalTime
import java.time.format.DateTimeFormatter
import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.lang.Iterable

/**
 * This class denotes the mapper  and reducer classes to obtain the list the number of type of messages in the predefined time interval.
 **/
class Job1

object Job1 {

  val conf: Config = ConfigFactory.load("application.conf")

  class Job1Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val key_value = new IntWritable(1)
    val KEY = new Text()
    val GROUP_ONE = conf.getInt("configuration.GROUP_ONE")
    val GROUP_THREE = conf.getInt("configuration.GROUP_THREE")

    override def map(key: Object,value: Text,context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

      val regexPattern: Regex = conf.getString("configuration.keyValPattern").r
      val injectedPattern  : Regex = conf.getString("configuration.injected_pattern").r

      val dateFormat = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val startTime = LocalTime.parse(conf.getString("configuration.startTime"), dateFormat)
      val endTime = LocalTime.parse(conf.getString("configuration.endTime"), dateFormat)

      val Pattern = regexPattern.findFirstMatchIn(value.toString)

// check whether the log message matches the injected string pattern if yes then checks the time is between the start and end time
// and then writes as key, value pair
      Pattern.toList.map(x => {
        injectedPattern.findFirstMatchIn(x.group(5)) match
        {
          case Some(_) => {
            val time = LocalTime.parse(x.group(GROUP_ONE), dateFormat)
            if (startTime.isBefore(time) && endTime.isAfter(time)) {
              KEY.set(x.group(GROUP_THREE))
              context.write(KEY, key_value)
            }
          }
          case None => { }
        }
      })
    }
  }
//This is Reducer class where reduce function is overridden and the output of mapper is processed to get the final list of key-value pairs
  class Job1Reducer extends Reducer[Text, IntWritable, Text, IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable],
                        context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val finalVal = values.asScala.foldLeft(0)(_ + _.get)
      context.write(key, new IntWritable(finalVal))
    }
  }

// This Partitioner class  takes place after Map phase and before reduce phase.
// It divides the data according to the number of partitioner ( # of partitioner  = # of reducers )
  class Job1Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
  //input  key value paired data can be  divided into 2 parts based on message type
      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }

}

