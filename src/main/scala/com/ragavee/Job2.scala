package com.ragavee

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapreduce.{Mapper, Partitioner, Reducer}

import scala.util.matching.Regex
import scala.jdk.CollectionConverters._
import java.lang.Iterable

/**
 * This class denotes the mapper  and reducer classes to obtain the time intervals sorted in the descending order that
 *          contained most log messages of the type ERROR with injected regex pattern string instances
 **/

class Job2

object Job2 {

  class Job2Mapper extends Mapper[Object, Text, Text, IntWritable] {

    val conf: Config = ConfigFactory.load("application.conf")
    val key_value = new IntWritable(1)
    val KEY = new Text()
    val GROUP_ONE = conf.getInt("configuration.GROUP_ONE")
//Map function gets the input data shards as input and records the occurences of recorded ERROR type messages for every each hour in the log file
    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

     val regexPattern: Regex = conf.getString("configuration.keyValPattern").r
     val injectedPattern  : Regex = conf.getString("configuration.injected_pattern").r
      val matchPattern = regexPattern.findAllMatchIn(value.toString)

      matchPattern.toList.map(x => {
        injectedPattern.findFirstMatchIn(x.group(5)) match {
          case Some(_) => {

            val splitValue = x.group(GROUP_ONE).split(":") // splits the time using the colon (:) as separator
            KEY.set(splitValue(0)) //Takes the first value that is represents the Hour value
            context.write(KEY,key_value)
          }
          case None => { }
        }
      })
    }
  }
//This reducer reduces the key-value pair by adding the values of same keys to get aggregated result of key value pairs
  class Job2Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val finalVal = values.asScala.foldLeft(0)(_ + _.get())
      context.write(key, new IntWritable(finalVal))
    }
  }

//Mapper1 takes the output of the reducer which is unsorted order and sorts based on the values in descending order by multiplying the values by -1
// and switching the key and values pairs in the list
  class Job2Mapper1 extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {

      val line = value.toString.split(",")
      context.write(new IntWritable(line(1).toInt * -1), new Text(line(0)))

    }
  }
//Reducer1 takes the output of the mapper1 where values are  keys here which will be sorted and finally switching the key values pairs by multiplying the values again by -1
  class Job2Reducer1 extends Reducer[IntWritable,Text,Text,IntWritable] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {

      values.asScala.foreach(value =>
        context.write(value, new IntWritable(key.get() * -1))
      )
    }
  }

  // This Partitioner class  takes place after Map phase and before reduce phase.
  // It divides the data according to the number of partitioner ( # of partitioner  = # of reducers )
  class Job2Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {
      //input  key value paired data can be  divided into 2 parts based on message type
      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }
}

