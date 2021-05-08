import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import scala.io.Source
import scala.util.parsing.json.JSON

object task2 {
  def t2_spark(review_file: String, business_file: String, output_file: String, n: String) = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)

    val rdd_review = sc.textFile(review_file).map(item => parse(item))
      .map(item => (compact(item \ "business_id"), compact(item \ "stars")))
      .filter(item => item._2 != null).map(item => (item._1, item._2.toDouble))
      .groupByKey().mapValues(item => item.toList)
    val rdd_business = sc.textFile(business_file).map(item => parse(item))
      .map(item => (compact(item \ "business_id"), compact(item \ "categories")))
      .filter(item => item._2 != null).filter(item => item._2 != "")
    //      .mapValues(item => item.split(",").toList)
    val rdd = rdd_business.join(rdd_review).map(item => (item._2._1, item._2._2))
      .flatMap(item => item._1.substring(1, item._1.length - 1).split(",").map(x => (x.trim(), item._2)))
      .reduceByKey((a, b) => a ::: b).mapValues(item => item.sum / item.size)
      .sortBy(item => item._1).sortBy(item => item._2, ascending = false)
//    print(rdd.take(n.toInt).toList)
    write_result(rdd.take(n.toInt).toList, output_file)
  }

  def t2_no_spark(review_file: String, business_file: String, output_file: String, n: String) = {
    val review = Source.fromFile(review_file)
    val business = Source.fromFile(business_file)

    val business_l = business.getLines().map(item => parse(item))
      .map(item => (compact(item \ "business_id"), compact(item \ "categories")))
      .filter(item => item._2 != null).filter(item => item._2.trim() != "")
    val review_l = review.getLines().map(item => parse(item))
      .map(item => (compact(item \ "business_id"), compact(item \ "stars").toDouble))
      .toSeq.groupBy(_._1).map(item => {
      var temp: List[Double] = List()
      item._2.foreach(i => {
        temp = temp :+ i._2
      })
      (item._1, temp)
    })

    val a = business_l.collect { case (k, v) if review_l.contains(k) => (v, review_l(k)) }
      .flatMap(item => {
        var l: List[(String, List[Double])] = List()
        item._1.substring(1, item._1.length - 1).split(",").foreach(i => {
          l = l :+ (i.trim(), item._2)
        })
        l
      }).map(item => (item._1, item._2.sum, item._2.size))
      .toSeq.groupBy(_._1).map(item => {
      var sum = 0.0
      var count = 0.0
      item._2.foreach(i => {
        sum += i._2
        count += i._3
      })
      (item._1, sum / count)
    }).toSeq.sortBy(_._1).reverse.sortBy(_._2).reverse
//    println(a.take(n.toInt).toList)
    write_result(a.take(n.toInt).toList, output_file)
  }

  def write_result(data:List[(String, Double)], output_file:String): Unit ={
    val pw = new PrintWriter(new File(output_file))
    pw.write("{\"result\": [")
    data.foreach(item => {
      pw.write("[\""+item._1+"\", "+item._2+"]")
      if (item._1 != data.last._1){
        pw.write(", ")
      }
    })
    pw.write("]}")
    pw.close()
  }

  def main(args: Array[String]): Unit = {


    // review_file, business_file, output_file, if_spark, n
    val review_file = args(0)//"E:\\Data\\inf553\\hw1\\review.json"
    val business_file = args(1)//"E:\\Data\\inf553\\hw1\\business.json"
    val output_file = args(2)//"task2_spark_ans"
    val if_spark = args(3)//"spark"
    val n = args(4)//"10"

    if (if_spark.trim() == "spark") {
      t2_spark(review_file, business_file, output_file, n)
    } else if (if_spark.trim() == "no_spark") {
      t2_no_spark(review_file, business_file, output_file, n)
    }


  }
}
