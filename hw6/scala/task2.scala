import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Durations, Seconds, StreamingContext}
import org.json4s.jackson.JsonMethods.{compact, parse}

import java.io.{File, FileOutputStream, PrintWriter}
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}
import scala.math.{abs, pow}
import scala.util.Random
import scala.util.control.Breaks

object task2 {

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

  def generate_hash_parameters(m_min:Int, n_hash_func: Int)= {
    val m = find_prime(m_min)
    val r = Random
    var a_list:List[Long] = List()
    var b_list:List[Long] = List()
    while (a_list.length != n_hash_func) {
      val a = r.nextInt(50000) + 10
      val b = r.nextInt(10000000) + 1000
      if (a % m != 0 && !a_list.contains(a)) {
        a_list = a_list :+ a.toLong
        b_list = b_list :+ b.toLong
      }
    }
    (a_list, b_list, m)
  }

  def my_print(ground_truth: Int, estimation: Int, output_file_path: String): Unit = {
    val now_time = Calendar.getInstance().getTime()
    val t_format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val t = t_format.format(now_time)
    val pw = new PrintWriter(new FileOutputStream(new File(output_file_path), true))
    pw.write(List(t, ground_truth.toString, estimation.toString).mkString(",")+"\n")
    pw.close()
  }

  def predict_num_distinct_value(item: RDD[String], a_list: List[Long], b_list: List[Long], m: Int, n_group: Int, output_file_path: String): Unit = {
    val city_list = item.collect()
    var long_tail_list:List[Int] = List()
    for (i <- a_list.indices) {
      val a = a_list.apply(i)
      val b = b_list.apply(i)
      var long_tail = 0
      for (city <- city_list) {
        val x = abs(city.hashCode).toLong
        val binary_str = ((a * x + b) % m.toLong).toBinaryString
        var tail = 0
        val loop = new Breaks;
        loop.breakable {
          for (e <- binary_str.reverse) {
            if (e == '0') {tail += 1}
            else {loop.break()}
          }
        }
        if (tail > long_tail) {long_tail = tail}
      }
      long_tail_list = long_tail_list :+ long_tail
    }
    val ordered_tail = long_tail_list.sorted
    var group_avg_list:List[Double] = List()
    for (i <- 0 until (ordered_tail.length/n_group).toInt){
      val est = ordered_tail.slice(n_group*i,n_group*(i+1)).map(r=>pow(2, r))
      group_avg_list = group_avg_list :+ est.sum / est.length
    }
    val ordered_avg = group_avg_list.sorted
    val estimation = group_avg_list.apply((ordered_avg.length/2).toInt).toInt
    val ground_truth = city_list.toSet.size
    println(estimation, ground_truth)
    my_print(ground_truth, estimation, output_file_path)
  }

  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val port = args(0) //"9999"
    val output_file_path = args(1) //"task2_ans_scala.csv"

    val window_length = 30
    val sliding_interval = 10
    val batch_interval  = 5
    val n_hash_func  = 25
    val n_group  = 5
    val hash_params = generate_hash_parameters(10000, n_hash_func)
    val a_list = hash_params._1
    val b_list = hash_params._2
    val m = hash_params._3

    val pw = new PrintWriter(new FileOutputStream(new File(output_file_path), true))
    pw.write(List("Time", "Ground Truth", "Estimation").mkString(",")+"\n")
    pw.close()
    val ssc = new StreamingContext("local[*]", "tasks2", Seconds(batch_interval))
    val sc = ssc.sparkContext
    sc.setLogLevel("OFF")
    val streaming = ssc.socketTextStream("localhost", port.toInt)
    streaming.window(Durations.seconds(window_length), Durations.seconds(sliding_interval))
      .map(item => parse(item)).map(item => compact(item \ "city")).filter(item=>item.trim!="")
      .foreachRDD(item=>predict_num_distinct_value(item, a_list, b_list, m, n_group, output_file_path))

    ssc.start()
    ssc.awaitTermination()
  }
}
