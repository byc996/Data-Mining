import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}


import java.io.{File, PrintWriter}
import java.util.Date
import scala.math.{pow, sin, sqrt}
import scala.util.Random
import scala.util.control.Breaks

object task3train {

  def calculate_pearson(pair:(String, String), profile_dict:Map[String, List[(Long, Double)]]): (String, String, Double) = {
    val i1 = pair._1
    val i2 = pair._2
    val i1_profile_rate_dict = profile_dict.get(i1).head.toMap
    val i2_profile_rate_dict = profile_dict.get(i2).head.toMap
    val i1_profile = i1_profile_rate_dict.keys
    val i2_profile = i2_profile_rate_dict.keys
    val intersect_items = i1_profile.toSet.intersect(i2_profile.toSet).toList
    var sim = 0.0
    if (intersect_items.size >= 3){
      val i1_stars = intersect_items.map(i1_profile_rate_dict.get(_).head)
      val i2_stars = intersect_items.map(i2_profile_rate_dict.get(_).head)
      val i1_mean = i1_stars.sum / i1_stars.size
      val i2_mean = i2_stars.sum / i2_stars.size

      val numerator = intersect_items.map(item=>(i1_profile_rate_dict.get(item).head - i1_mean)*(i2_profile_rate_dict.get(item).head - i2_mean)).sum
      val denominator = sqrt(intersect_items.map(item=>pow(i1_profile_rate_dict.get(item).head-i1_mean, 2)).sum) * sqrt(intersect_items.map(item=>pow(i2_profile_rate_dict.get(item).head-i2_mean, 2)).sum)

      if (denominator != 0) {
        sim = numerator / denominator

      }

    }
    (i1, i2, sim)
  }
  def find_prime(x:Int): Int ={
    var isPrime = false
    var x_pri = x
    while (!isPrime) {
      x_pri += 1
      val loop = new Breaks;
      var tmp = 2
      loop.breakable {
        for( i <- 2 until x){
          if (x_pri % i == 0){
            loop.break()
          }
          tmp += 1
        }
      }
      if (tmp == x) {
        isPrime = true
      }
    }
    x_pri
  }
  def generate_hash_parameters(n_features:Int, total_rows: Int)= {
    val m = find_prime(n_features)
    val r = Random
    var a_list:List[Int] = List()
    var b_list:List[Int] = List()
    while (a_list.length != total_rows) {
      val a = r.nextInt(500) + 1
      val b = r.nextInt(5000) + 100
      if (a % m != 0 || !a_list.contains(a)) {
        a_list = a_list :+ a
        b_list = b_list :+ b
      }
    }
    (a_list, b_list, m)
  }
  def hash_func(x:Int, a:Int, b:Int, m:Int) = {
    (a * x.toInt + b) % m
  }
  def signature(item:List[Int], a_list:List[Int], b_list:List[Int], m:Int) = {
    var signature: List[Int] = List()
    for (i <- a_list.indices) {
      var min_signature: List[Int] = List()
      for (j <- item.indices) {
        val hash_val = hash_func(item.apply(j), a_list.apply(i), b_list.apply(i), m)
        min_signature = min_signature :+ hash_val
      }
      signature = signature :+ min_signature.min
    }
    signature
  }
  def split_band(pair:(String, List[Int]), r:Int, b:Int) = {
    var tmp:List[(String, String)] = List()
    val business = pair._1
    val signatures = pair._2
    for (i <- 0 until b) {
      val band = signatures.slice(r*i, r*(i+1))
      var key = i.toString + "_"
      band.foreach(item=>key += item.toString)
      tmp = tmp :+ (key, business)
    }
    tmp
  }

  def jaccard_similarity(x:List[String], business_users:Map[String, List[Int]]) = {
    val s1 = business_users.get(x.head)
    val s2 = business_users.get(x.apply(1))
    val similarity = s1.head.toSet.intersect(s2.head.toSet).size * 1.0 /  s1.head.toSet.union(s2.head.toSet).size
    ((x.head, x.apply(1)), similarity)
  }
  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)

    val train_file = "E:\\Data\\inf553\\hw3\\publicdata\\train_review.json"
    val model_file = "task3user.scala.model" // task3user.model
    val cf_type = "user_based" //item_based
    val rdd = sc.textFile(train_file).map(item=>parse(item))
    val business_index_dict = rdd.map(item=>compact(item \ "business_id")).distinct().zipWithIndex().collectAsMap()
    val user_index_dict = rdd.map(item=>compact(item \ "user_id")).distinct().zipWithIndex().collectAsMap()
    if (cf_type == "item_based") {
      val rdd1 = rdd.map(item=>(compact(item \ "business_id"), (user_index_dict.get(compact(item \ "user_id")).head, compact(item \ "stars").toDouble))).groupByKey().mapValues(_.toList)
        .filter(_._2.length >=3).persist()
//      rdd1.take(5).foreach(println)
      val business_profile_dict = rdd1.collect().toMap
      val business = rdd1.map(_._1)
      val rdd2 = business.cartesian(business).filter(item=>item._1 < item._2).map(item=>calculate_pearson(item, business_profile_dict)).filter(_._3 > 0.0)

      val pw = new PrintWriter(new File(model_file))
      rdd2.collect().foreach(item=>pw.write("{\"b1\": "+item._1+", \"b2\": "+item._2 + ", \"sim\": " +item._3.toString + "}\n"))
      pw.close()
      val end_time =new Date().getTime
      println("time: "+(end_time-start_time)/1000.0+"s")
    } else {
      val total_rows = 30
      val band_rows = 1
      val rdd_user = rdd.map(item=>(compact(item \ "user_id"), (business_index_dict.get(compact(item \ "business_id")).head, compact(item \ "stars").toDouble))).groupByKey().mapValues(_.toList).persist()
      val user_profile_dict = rdd_user.collect().toMap
      //rdd1 = rdd_user.mapValues(lambda x:[i[0] for i in x])
      val rdd1 = rdd_user.mapValues(item=>item.map(_._1.toInt)).persist()
      val parameters = generate_hash_parameters(user_index_dict.size, total_rows)
      val a_list = parameters._1
      val b_list = parameters._2
      val m = parameters._3

      val rdd2 = rdd1.mapValues(item=>signature(item, a_list, b_list, m))

      val rdd3 = rdd2.flatMap(item=>split_band(item, band_rows, (total_rows/band_rows).toInt))

      val rdd4 = rdd3.groupByKey().map(item=>item._2.toList).filter(_.size > 1).flatMap(_.combinations(2).toList)

      val users_business = rdd1.collect().toMap
      val candidate_pairs = rdd4.map(item=>jaccard_similarity(item, users_business)).filter(_._2 >= 0.01).map(_._1)
      //result = candidate_pairs.map(lambda x: calculate_pearson(x, user_profile_dict)).filter(lambda x: x is not None).filter(lambda x:x[1]>0)
      val result = candidate_pairs.map(item=>calculate_pearson(item, user_profile_dict)).filter(_._3 > 0.0)
      val pw = new PrintWriter(new File(model_file))
      result.collect().foreach(item=>pw.write("{\"u1\": "+item._1+", \"u2\": "+item._2+ ", \"sim\": " +item._3.toString + "}\n"))
      pw.close()
      val end_time =new Date().getTime
      println("time: "+(end_time-start_time)/1000.0+"s")
    }
  }
}
