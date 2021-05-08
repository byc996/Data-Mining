import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import java.util.Date
import scala.math.abs

object task3predict {

  def predict_item(item: (String, String), model_dict: collection.Map[Long, List[(Long, Double)]], business_index: collection.Map[String, Long], user_index: collection.Map[String, Long], user_business_dict: collection.Map[Long, List[(Long, Double)]], business_avg_rate: collection.Map[Long, Double], user_avg_rate: collection.Map[Long, Double], N: Int): ((String, String), Double) = {
    var p = 0.0
    val u = user_index.get(item._1)
    val b = business_index.get(item._2)
    if (b.isEmpty) {
      if (user_avg_rate.contains(u.head)) {
        p = user_avg_rate.get(u.head).head
      }
      return (item, p)
    }
    if (u.isEmpty || !model_dict.contains(b.head)) {
      if (business_avg_rate.contains(b.head)) {
        p = business_avg_rate.get(b.head).head
      }

      return (item, p)
    }
    val user = u.head
    val business = b.head
    val business_rate_dict = user_business_dict.get(user).head.toMap
    val business_sim_dict = model_dict.get(business).head.sortBy(_._2).reverse
    var candidate_items:List[(Long, Double, Double)] = List()

    var i = 0
    while (candidate_items.length < N && i < business_sim_dict.length) {
      val pair = business_sim_dict.apply(i)
      if (business_rate_dict.keys.toList.contains(pair._1)) {
        candidate_items = candidate_items :+ (pair._1, pair._2, business_rate_dict.get(pair._1).head)
      }
      i += 1
    }

    val denominator = candidate_items.map(item=>abs(item._2)).sum

    if (denominator == 0) {

      if ( business_avg_rate.contains(business)) {
        p = business_avg_rate.get(business).head
      }
    }else {
      p = candidate_items.map(item=>item._2*item._3).sum / denominator
    }
    (item, p)
  }

  def predict_user(item: (String, String), model_dict: collection.Map[Long, List[(Long, Double)]], business_index: collection.Map[String, Long], user_index: collection.Map[String, Long], business_user_dict: collection.Map[Long, List[(Long, Double)]], business_avg_rate: collection.Map[Long, Double], user_avg_rate: collection.Map[Long, Double], N: Int): ((String, String), Double) = {
    var p = 0.0
    val u = user_index.get(item._1)
    val b = business_index.get(item._2)

    if (u.isEmpty) {
      if (business_avg_rate.contains(b.head)) {
        p = business_avg_rate.get(b.head).head
      }
      return (item, p)
    }
    if (b.isEmpty || !model_dict.contains(u.head)) {
      if (user_avg_rate.contains(u.head)) {
        p = user_avg_rate.get(u.head).head
      }

      return (item, p)
    }

    val business = b.head
    val user = u.head
    val r_u = user_avg_rate.get(user).head
    val user_rate_dict = business_user_dict.get(business).head.toMap
    val user_sim_dict = model_dict.get(user).head.sortBy(_._2).reverse
    var candidate_users:List[(Long, Double, Double)] = List()

    var i = 0
    while (candidate_users.length < N && i < user_sim_dict.length) {
      val pair = user_sim_dict.apply(i)
      if (user_rate_dict.keys.toList.contains(pair._1)) {
        candidate_users = candidate_users :+ (pair._1, pair._2, user_rate_dict.get(pair._1).head)
      }
      i += 1
    }

    val numerator = candidate_users.map(item=>(item._3 - user_avg_rate.get(item._1).head)*item._2).sum
    val denominator = candidate_users.map(item=>abs(item._2)).sum

    if (denominator == 0) {
      if ( user_avg_rate.contains(user)) {
        p = user_avg_rate.get(user).head
      }
    }else {
      p = r_u + numerator / denominator
    }
    (item, p)
  }

  def main(args: Array[String]) = {
    val start_time = new Date().getTime
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)
    //train_file, test_file, model_file, output_file, cf_type = '/home/buyi/data/hw3/train_review.json', '/home/buyi/data/hw3/test_review.json', 'task3user.model', 'task3user.predict', "user_based"  # user_based
    val train_file = args(0) //"E:\\Data\\inf553\\hw3\\publicdata\\train_review.json"
    val test_file = args(1) //"E:\\Data\\inf553\\hw3\\publicdata\\test_review.json"

    val model_file = args(2) //"task3item.scala.model" // task3user.model
    val output_file = args(3) //"task3item.scala.predict" // task3user.model

    val cf_type = args(4) //"item_based" //item_based


    val rdd_train = sc.textFile(train_file).map(item=>parse(item))
    val business_index = rdd_train.map(item=>compact(item \ "business_id")).distinct().zipWithIndex().collectAsMap()
    val user_index = rdd_train.map(item=>compact(item \ "user_id")).distinct().zipWithIndex().collectAsMap()

    val rdd_user = rdd_train.map(item=>(user_index.get(compact(item \ "user_id")).head, compact(item \ "stars").toDouble)).groupByKey().mapValues(_.toList)
    val user_avg_rate = rdd_user.map(item=>(item._1, item._2.sum/item._2.length)).collectAsMap()
    val rdd_business = rdd_train.map(item=>(business_index.get(compact(item \ "business_id")).head, compact(item \ "stars").toDouble)).groupByKey().mapValues(_.toList)
    val business_avg_rate = rdd_business.map(item=>(item._1, item._2.sum/item._2.length)).collectAsMap()

    val rdd_test = sc.textFile(test_file).map(item=>parse(item)).map(item=>(compact(item \ "user_id"), compact(item \ "business_id")))
      .filter(item=>user_index.keys.toList.contains(item._1) || business_index.keys.toList.contains(item._2))
    val rdd_model = sc.textFile(model_file).map(item=>parse(item))
    val N = 3
    if (cf_type == "item_based") {

      val user_business_dict = rdd_train.map(item=>(user_index.get(compact(item \ "user_id")).head, (business_index.get(compact(item \ "business_id")).head, compact(item \ "stars").toDouble)))
        .groupByKey().mapValues(_.toList).collectAsMap()
      val rdd_model_1 = rdd_model.map(item=>(business_index.get(compact(item \ "b1")).head,business_index.get(compact(item \ "b2")).head,compact(item \ "sim").toDouble))
        .flatMap(item=>{
          var l:List[(Long, (Long, Double))] = List()
          l = l :+ (item._1, (item._2, item._3))
          l = l :+ (item._2, (item._1, item._3))
          l
        }).groupByKey().mapValues(_.toList)
      val model_dict = rdd_model_1.collectAsMap()
      //.filter(item=>business_index.keys.toList.contains(item._2))
      val result = rdd_test
        .map(item=>predict_item(item, model_dict, business_index, user_index, user_business_dict, business_avg_rate, user_avg_rate, N)).filter(_._2 > 0)

      val pw = new PrintWriter(new File(output_file))
      result.collect().foreach(item=>pw.write("{\"user_id\": "+item._1._1+", \"business_id\": "+item._1._2+ ", \"sim\": " +item._2.toString + "}\n"))
      pw.close()
    }else {

      val business_user_dict = rdd_train.map(item=>(business_index.get(compact(item \ "business_id")).head, (user_index.get(compact(item \ "user_id")).head, compact(item \ "stars").toDouble)))
        .groupByKey().mapValues(_.toList).collectAsMap()
      val rdd_model_1 = rdd_model.map(item=>(user_index.get(compact(item \ "u1")).head,user_index.get(compact(item \ "u2")).head,compact(item \ "sim").toDouble))
        .flatMap(item=>{
          var l:List[(Long, (Long, Double))] = List()
          l = l :+ (item._1, (item._2, item._3))
          l = l :+ (item._2, (item._1, item._3))
          l
        }).groupByKey().mapValues(_.toList)

      val model_dict = rdd_model_1.collectAsMap()
      //.filter(item=>user_index.keys.toList.contains(item._1))
      val result = rdd_test
        .map(item=>predict_user(item, model_dict, business_index, user_index, business_user_dict, business_avg_rate, user_avg_rate, N)).filter(_._2 > 0)

      val pw = new PrintWriter(new File(output_file))
      result.collect().foreach(item=>pw.write("{\"user_id\": "+item._1._1+", \"business_id\": "+item._1._2+ ", \"sim\": " +item._2.toString + "}\n"))
      pw.close()
    }

    val end_time =new Date().getTime
    println("time: "+(end_time-start_time)/1000.0+"s")
  }
}
