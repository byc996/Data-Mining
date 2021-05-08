import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import java.util.Date
import scala.math.sqrt

object task2predict {
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1").set("spark.driver.memory","4g").set("spark.executor.memory","4g")
    val sc = new SparkContext(conf)

    val test_file = args(0) //"E:\\Data\\inf553\\hw3\\publicdata\\test_review.json"
    val model_file = args(1) //"task2.scala.model"
    val output_file = args(2) //"task2.scala.predict"

    val rdd_model = sc.textFile(model_file).map(item=>parse(item))

    val rdd_business = rdd_model.filter(item=>compact(item \ "type") == "\"b\"").map(item=>(compact(item \ "business_id"), compact(item \ "business_profile")))
    val rdd_user = rdd_model.filter(item=>compact(item \ "type") == "\"u\"").map(item=>(compact(item \ "user_id"), compact(item \ "user_profile")))
    val rdd_test = sc.textFile(test_file).map(item=>parse(item)).map(item => (compact(item \ "business_id"), compact(item \ "user_id")))
    val business_dict = rdd_business.collectAsMap()
    val user_dict = rdd_user.collectAsMap()
//    rdd_test.take(5).foreach(println)
    val predicted_pairs = rdd_test.collect()
//    println(user_dict)
    val pw = new PrintWriter(new File(output_file))
    predicted_pairs.foreach(item=>{
      val business = business_dict.get(item._1)
      val user = user_dict.get(item._2)
//      println(item._1, item._2)
//      println(business.isDefined, user.isDefined)
      if (business.isDefined && user.isDefined) {
        val business_list = business.head.substring(1, business.head.length-1).split(",")
        val user_list = user.head.substring(1, user.head.length-1).split(",")
        val sim = business_list.toSet.intersect(user_list.toSet).size / (sqrt(business_list.size)*sqrt(user_list.size))

        if (sim >= 0.01) {
          pw.write("{\"user_id\": "+item._2+", \"business_id\": "+item._1+", \"sim\": "+ sim+"}\n")
        }
      }

    })
    pw.close()
    val end_time =new Date().getTime
    println("time: "+(end_time-start_time)/1000.0+"s")
  }
}
