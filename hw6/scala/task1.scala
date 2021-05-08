import java.util.Date
import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, PrintWriter}
import scala.math.abs
import scala.util.Random

object task1 {

  def create_random_hash_funcs(n_bits:Int, n_hash_func: Int)= {
    val m = n_bits
    val r = Random
    var a_list:List[Long] = List()
    var b_list:List[Long] = List()
    while (a_list.length != n_hash_func) {
      val a = r.nextInt(500) + 1
      val b = r.nextInt(5000) + 100
      if (a % m != 0 && !a_list.contains(a)) {
        a_list = a_list :+ a.toLong
        b_list = b_list :+ b.toLong
      }
    }
    (a_list, b_list, m)
  }


  def hash_city(random_a: List[Long], random_b: List[Long], m: Int, city: String): List[Long] = {
    var hash_list:List[Long] = List()
    if (city.trim==""){
       return List(-1)
    }
    val x = abs(city.hashCode)
    for (i <- random_a.indices){
      val h_v = (random_a.apply(i) * x + random_b.apply(i)) % m

      hash_list = hash_list :+ h_v
    }
    hash_list
  }

  def is_exists(index_list: List[Long], bit_array: Array[Int]): String = {
    if (index_list.head == -1) {
      "0"
    } else {
      var exist = "1"
      for (index <- index_list) {
        if (bit_array(index.toInt) == 0) exist = "0"
      }
      exist
    }
  }

  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val conf = new SparkConf().setMaster("local[3]").setAppName("task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    val first_json_path = args(0) //"E:\\Data\\inf553\\hw6\\resource\\asnlib\\publicdata\\business_first.json"
    val second_json_path = args(1) //"E:\\Data\\inf553\\hw6\\resource\\asnlib\\publicdata\\business_second.json"
    val output_file_path = args(2) //"task1_ans.csv"

    val rdd_first = sc.textFile(first_json_path).map(item => parse(item))
      .map(item => compact(item \ "city"))
    val rdd_second = sc.textFile(second_json_path).map(item => parse(item))
      .map(item => compact(item \ "city"))
//    rdd_first.take(5).foreach(println)
    val n_bits = 10000
    val n_hash_func = 3
    val hash_params = create_random_hash_funcs(n_bits, n_hash_func)
//    println(hash_params)
    val rdd_indices = rdd_first.filter(item=> item.trim !="").flatMap(x=>hash_city(hash_params._1, hash_params._2, hash_params._3, x)).distinct()
    val indices = rdd_indices.collect()
    var bit_array:Array[Int] = new Array[Int](n_bits)
    for (index <- indices) {
      bit_array(index.toInt) = 1
    }
    val result = rdd_second.map(x=>hash_city(hash_params._1, hash_params._2, hash_params._3, x))
      .map(x=>is_exists(x, bit_array)).collect()

    val pw = new PrintWriter(new File(output_file_path))
    pw.write(result.mkString(" "))
    pw.close()
    val end_time =new Date().getTime
    println("time: "+(end_time-start_time)/1000.0+"s")
  }
}
