import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import scala.util.parsing.json.JSON

object task3 {
  def main(args: Array[String]): Unit = {

    val input_file = args(0)//"E:\\Data\\inf553\\hw1\\review.json"
    val output_file = args(1)//"task3_default_ans"
    val partition_type = args(2)//"customized" //default
    val n_partitions = args(3)//"20"
    val n = args(4)//"50"

    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)

    var rdd = sc.textFile(input_file).map(item => parse(item)).map(item => (compact(item \ "business_id"), 1))
    //    var rdd = sc.textFile(input_file).map(JSON.parseFull)
    //      .map(item => item.get.asInstanceOf[Map[String, Any]]).map(item => (item("business_id"), 1))

    if (partition_type.trim() == "customized") {
      rdd = rdd.partitionBy(new HashPartitioner(n_partitions.toInt))
    }
    val n_par = rdd.getNumPartitions
    val n_items = rdd.glom().map(_.length).collect().toList
    val result = rdd.reduceByKey(_ + _).filter(item => item._2 > n.toInt).collect().toList
    val pw = new PrintWriter(new File(output_file))
    pw.write("{\"n_partitions\": " + n_par + ", ")
    pw.write("\"n_items\": [" + n_items.mkString(",") + "], ")
    pw.write("\"result\": [")
    result.foreach(item => {
      pw.write("[" + item._1 + ", " + item._2 + "]")
      if (item._1 != result.last._1) {
        pw.write(", ")
      }
    })
    pw.write("]}")
    pw.close()
  }
}
