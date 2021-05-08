import org.apache.spark.storage.StorageLevel.{MEMORY_AND_DISK_SER,DISK_ONLY}
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import java.util.Date
import scala.math.log10
import scala.util.control.Exception.allCatch

object task2train {

  def is_number(s: String): Boolean = (allCatch opt s.toDouble).isDefined
  def remove_words(w_list: Iterable[String], stop_words: List[String]) = {
    var new_words:List[String] = List()
    for (elem <- w_list) {
      if (!stop_words.contains(elem) && !is_number(elem)) {
        new_words = new_words :+ elem
      }
    }
    new_words
  }

  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)
//    val train_file = "E:\\Data\\inf553\\hw3\\publicdata\\train_review.json"
//    val model_file = "task2.scala.model"
//    val stopwords = "E:\\Data\\inf553\\hw3\\publicdata\\stopwords"
    val train_file = args(0)
    val model_file = args(1)
    val stopwords = args(2)

    val rdd_pre = sc.textFile(train_file).map(item => parse(item)).map(item => (compact(item \ "business_id"), compact(item \ "user_id"), compact(item \ "text")))
//    val business_index = rdd_pre.map(_._1).zipWithIndex().collectAsMap()
//    val user_index = rdd_pre.map(_._2).zipWithIndex().collectAsMap()
    val rdd = rdd_pre.map(item => (item._1, item._3)).groupByKey()
      .mapValues(item=>item.flatMap(x=>x.replaceAll("[(|)|\\[|\\]|\"|,|.|!|:|;|?]","").split(" ")))


    val stop_words = sc.textFile(stopwords).collect().toList

    val rdd1 = rdd.mapValues(item=>item.map(_.toLowerCase().trim)).mapValues(item=>item.filter(_ != "")).mapValues(item=>item.filter(i=>(!stop_words.contains(i) && !is_number(i))))
//      .mapValues(item=>item.filter(!is_number(_)))

    val total_num_words = rdd1.map(item=>item._2.size).reduce((x, y)=> x + y)
    val threshold = total_num_words * 0.000001

    val no_rare_words_index = rdd1.flatMap(item=>item._2.map((_, 1))).reduceByKey((x,y)=>x+y).filter(item=>item._2 >= threshold).map(_._1).zipWithIndex().collectAsMap()
    val rdd2 = rdd1.mapValues(item=>item.filter(x=>no_rare_words_index.contains(x)).map(no_rare_words_index.get(_).head.toInt))//.persist(MEMORY_AND_DISK_SER)
    val n_business = rdd.count()
    val idf_dict = rdd2.flatMap(_._2.toSet).map((_, 1)).groupByKey().mapValues(_.sum).map(item=>(item._1, log10(n_business/item._2))).collect().toMap
    val rdd_tf_idf = rdd2.mapValues(item=>item.map((_,1.0))).mapValues(item=>item.groupBy(_._1).mapValues(_.size).toList)
      .mapValues(item=>{
        val max_f = item.map(_._2).max
        val r = item.map(i=>(i._1,i._2 * 1.0/max_f*idf_dict.get(i._1).head)).sortBy(_._2).reverse.map(_._1)
        if(r.size >= 200) {
          r.take(200)
        }else{
          r
        }
      })
    val business_profile_dict = rdd_tf_idf.collect().toMap

    val rdd_user_profile = rdd_pre.map(item => (item._2, item._1)).groupByKey().mapValues(item=> {
      var r: List[Int] = List()
      item.foreach(i=>r = r++business_profile_dict.get(i).head)
      r.distinct
    })

    val pw = new PrintWriter(new File(model_file))
    rdd_tf_idf.collect().foreach(item=>pw.write("{\"type\": \"b\", \"business_id\": "+item._1 + ", \"business_profile\": " +item._2.map(_.toString).mkString("[", ",", "]") + "}\n"))
    rdd_user_profile.collect().foreach(item=>pw.write("{\"type\": \"u\", \"user_id\": "+item._1 + ", \"user_profile\": " +item._2.map(_.toString).mkString("[", ",", "]") + "}\n"))
    pw.close()
    val end_time =new Date().getTime
    println("time: "+(end_time-start_time)/1000.0+"s")
  }
}
