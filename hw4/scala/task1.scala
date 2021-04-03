import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import java.util.Date
import scala.util.Random
import scala.util.control.Breaks

object task1 {

  def hash_func(x:Int, a:Int, b:Int, m:Int) = {
    (a * x.toInt + b) % m
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
      if (a % m != 0 && !a_list.contains(a)) {
        a_list = a_list :+ a
        b_list = b_list :+ b
      }
    }
    (a_list, b_list, m)
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
    (x, similarity)
  }

  def main(args: Array[String]): Unit = {
    val start_time =new Date().getTime
    val conf = new SparkConf().setMaster("local[*]").setAppName("task1")
    val sc = new SparkContext(conf)

    val input_file = args(0)//"E:\\Data\\inf553\\hw3\\publicdata\\train_review.json"
    val output_file = args(1)//"task1.scala.res"
    val total_rows = 40
    val band_rows = 1

    val rdd_review = sc.textFile(input_file).map(item => parse(item))
      .map(item => (compact(item \ "business_id"), compact(item \ "user_id")))


    val rdd_users = rdd_review.map(item=>item._2).distinct().sortBy(item=>item)
    val user_index_dict = rdd_users.zipWithIndex().collect().toMap

    val hash_parameters = generate_hash_parameters(user_index_dict.size, total_rows)
    val a_list = hash_parameters._1
    val b_list = hash_parameters._2
    val m = hash_parameters._3

    val rdd = rdd_review.groupByKey().mapValues(item=>{
      var tmp:List[Int] = List()
      item.foreach(i=>tmp = tmp :+ user_index_dict.get(i).toList.head.toInt)
      tmp
    })
    val rdd1 = rdd.mapValues(item=>signature(item, a_list, b_list, m))
    val rdd2 = rdd1.flatMap(item=>split_band(item, band_rows, total_rows/band_rows.toInt))

    val rdd3 = rdd2.groupByKey().map(item=>item._2.toList).filter(item=>item.length > 1).flatMap(item=>item.combinations(2)).map(item=>item.sorted).distinct()

    val business_users = rdd.collect().toMap

    val result = rdd3.map(item=>jaccard_similarity(item, business_users)).filter(item=>item._2 >= 0.05)

    val pw = new PrintWriter(new File(output_file))
    result.collect().foreach(item=>pw.write("{\"b1\": "+item._1.head + ", \"b2\": " +item._1.apply(1) + ", \"sim\": " + item._2 + "}\n"))
    pw.close()
    val end_time =new Date().getTime
    println("time: "+(end_time-start_time)/1000.0+"s")
  }


}
