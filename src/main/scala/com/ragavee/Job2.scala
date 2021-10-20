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
    val one = new IntWritable(1)
    val word = new Text()
    val GROUP_ONE = conf.getInt("configuration.GROUP_ONE")

    override def map(key: Object, value: Text, context: Mapper[Object, Text, Text, IntWritable]#Context): Unit = {

     val regexPattern: Regex = conf.getString("configuration.keyValPattern").r
     val injectedPattern  : Regex = conf.getString("configuration.injected_pattern").r
      val matchPattern = regexPattern.findAllMatchIn(value.toString)

      matchPattern.toList.map(x => {
        injectedPattern.findFirstMatchIn(x.group(5)) match {
          case Some(_) => {
            word.set(x.group(GROUP_ONE).split(":")(0))
            context.write(word,one)
          }
          case None => { }
        }
      })
    }
  }

  class Job2Reducer extends Reducer[Text,IntWritable,Text,IntWritable] {
    override def reduce(key: Text, values: Iterable[IntWritable], context: Reducer[Text, IntWritable, Text, IntWritable]#Context): Unit = {

      val finalVal = values.asScala.foldLeft(0)(_ + _.get())
      context.write(key, new IntWritable(finalVal))
    }
  }

  class Job2Mapper1 extends Mapper[Object, Text, IntWritable, Text] {

    override def map(key: Object, value: Text, context: Mapper[Object, Text, IntWritable, Text]#Context): Unit = {

      val line = value.toString.split(",")
      val result = line(1).toInt * -1
      context.write(new IntWritable(result), new Text(line(0)))

    }
  }

  class Job2Reducer1 extends Reducer[IntWritable,Text,Text,IntWritable] {
    override def reduce(key: IntWritable, values: Iterable[Text], context: Reducer[IntWritable, Text, Text, IntWritable]#Context): Unit = {

      values.asScala.foreach(value =>
        context.write(value, new IntWritable(key.get() * -1))
      )
    }
  }

  class Job2Partitioner extends Partitioner[Text, IntWritable] {
    override def getPartition(key: Text, value: IntWritable, numReduceTasks: Int): Int = {

      if (key.toString == "INFO") {
        return 1 % numReduceTasks
      }
      return 0
    }
  }
}

