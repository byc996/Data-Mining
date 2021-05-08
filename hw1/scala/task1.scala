import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import scala.io.Source

object task1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)
    val input_file = args(0)//"E:\\Data\\inf553\\hw1\\review.json"
    val output_file = args(1)//"task1_ans"
    val stopwords = args(2)//"E:\\Data\\inf553\\hw1\\stopwords"
    val y = args(3)//"2018"
    val m = args(4)//"10"
    val n = args(5)//"10"
    val rdd = sc.textFile(input_file).map(item => parse(item))
    //    val result_a = rdd.count()
    val rdd_b = rdd.filter(item => compact(item \ "date").contains(y))
    //    val result_b = rdd_b.count()
    val rdd_c = rdd.map(item => compact(item \ "user_id")).distinct()
//    val result_c = rdd_c.count()
    val rdd_d = rdd.map(item => (compact(item \ "user_id"), 1)).reduceByKey((a, b) => a + b)
      .sortBy(item => item._1).sortBy(item => item._2, ascending = false)
//      .map(item => List(item))
//    val result_d = rdd_d.take(m.toInt)
    val rdd_sw = sc.textFile(stopwords)
    var sw = Source.fromFile(stopwords).getLines().toList
    val punctuations = List("(", "[", ",", ".", "!", "?", ":", ";", "]", ")")
    for (p <- punctuations) {
      sw = sw :+ p
    }
    sw = sw :+ ""
    val rdd_e = rdd.flatMap(item => compact(item \ "text").split(" "))
      .map(item => item.trim().toLowerCase()).filter(item => !sw.contains(item))
      .map(item => (item, 1)).reduceByKey((a, b) => a + b)
      .sortBy(item => item._1).sortBy(item => item._2, ascending = false)
      .map(item => item._1)

    val pw = new PrintWriter(new File(output_file))
    pw.write("{\"A\": "+rdd.count()+", ")
    pw.write("\"B\": "+rdd_b.count()+", ")
    pw.write("\"C\": "+rdd_c.count()+", ")
    val r_d = rdd_d.take(m.toInt).toList
    pw.write("\"D\": [")
    r_d.foreach(item=>{
      pw.write("["+item._1+", "+item._2+"]")
      if (item._1 != r_d.last._1){
        pw.write(", ")
      }
    })
    pw.write("] \"E\": "+rdd_e.take(n.toInt).toList.mkString("[", ", ", "]")+"}")
    pw.close()

  }
}
