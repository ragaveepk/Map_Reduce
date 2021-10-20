import com.ragavee.Job3
import com.ragavee.Job3.{Job3Mapper, Job3Reducer}
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.slf4j.{Logger, LoggerFactory}
import com.ragavee.Job3.Job3Reducer
import com.ragavee.Job4.Job4Reducer
import com.typesafe.config.{Config, ConfigFactory}
import org.scalatest.matchers.should.Matchers._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import java.time.LocalTime
import java.time.format.DateTimeFormatter
//import scala.util.matching.Regex
//import scala.jdk.CollectionConverters._
//import java.lang.Iterable



  class Test extends AnyFunSuite {
    val config = ConfigFactory.load("application.conf")
    test("Test the correctness of name of the job from application config") {
      val config = ConfigFactory.load("application.conf")
      config shouldBe a[Config]
      config.getString("configuration.job1") shouldBe ("job1")
    }

  }

  class Test2 extends AnyFlatSpec with Matchers {
    val config = ConfigFactory.load("application.conf")
    it should "Check start and end time" in {
      val formatter = DateTimeFormatter.ofPattern("HH:mm:ss.SSS")
      val startTime = LocalTime.parse(config.getString("configuration.startTime"), formatter)
      val endTime = LocalTime.parse(config.getString("configuration.endTime"), formatter)
      val startTime_test = LocalTime.parse("19:55:33.412", formatter)
      val endTime_Test = LocalTime.parse("20:11:06.281", formatter)
      assert(startTime ==  startTime_test && endTime == endTime_Test)
    }

    it should "Checks if the reducer combiner class has been set and compare " in {
      val configure: Configuration = new Configuration()
      val job3: Job = Job.getInstance(configure, "job3")
      job3.setCombinerClass(classOf[Job3Reducer])
      assert(job3.getCombinerClass() == classOf[Job3Reducer])
    }

    it should "Checks if the mapper combiner class has been set and compare " in {
      val configure: Configuration = new Configuration()
      val job4: Job = Job.getInstance(configure, "job4")
      job4.setMapperClass(classOf[Job3Mapper])
      assert(job4.getMapperClass() == classOf[Job3Mapper])
    }

    it should(" check  if the test_msg matches the regex pattern") in {
      val test_msg = "19:55:33.352 [scala-execution-context-global-125] WARN  HelperUtils.Parameters$ - A><YFqpg+~E1T"
      val pattern = config.getString("configuration.keyValPattern").r
      val patternMatch = pattern.findFirstMatchIn(test_msg) match {
        case Some(_) => 1
        case None => 0
      }
      assert(patternMatch == 1)
    }
}
