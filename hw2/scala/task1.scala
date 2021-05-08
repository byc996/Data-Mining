import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

object task1 {

  def hash_pcy(tuple: Iterable[String], n_buckets: Int) = ???

//  def pcy(baskets: List[Iterable[String]], support: Long, n_buckets: Int) = {
//    // Pass 1
//    var candidate_dict: Map[Array[String], Int] = Map()
//    var hash_table = Map()
//    var bitmap = new Array[Int](n_buckets)
//    var frequent_list = List()
//    var frequent_itemset = List()
//    var singleton = List()
//    for (b <- baskets) {
//      var l = b.toArray
//      for (i <- l.indices) {
//        candidate_dict += (l(i) -> (candidate_dict.getOrElse(l(i), 0) + 1))
//        for (j <- b.toList.slice(i + 1, b.toList.length).indices) {
//          var key = hash_pcy((b(i), b.take(j)), n_buckets)
//        }
//      }
//
//    }
//  }


  //def phase1_map(par: Iterator[Iterable[String]], threshold: Int, length: Long, n_buckets: Int) = {
  //  val baskets = par.toList
  //  val sub_threshold = baskets.length / length * threshold
  //  //  val frequent_itemsets = pcy(baskets, sub_threshold, n_buckets)
  //  //    for(i<- frequent_itemsets){
  //  //      return (i, 1)
  //  //    }
  //}

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("task1")
    val sc = new SparkContext(conf)

    // case_number, support, input_file_path, output_file_path = "1", "4", "/home/buyi/data/hw2/small2.csv", "task1_1_4_ans"
    val case_number = "1"
    val support = "4"
    val input_file_path = "E:\\Data\\inf553\\hw2\\publicdata\\small1.csv"
    val output_file_path = "task1_1_4_ans"
    val n_buckets = 300

    val rdd = sc.textFile(input_file_path)
    val first = rdd.first()

    //    val rdd1 = rdd.filter(x=>x!=first).map(x=>x.split(",")(0), )
    if (case_number.toInt == 1) {
      val rdd1 = rdd.filter(x => x != first).map(x => (x.split(",")(0).trim(), x.split(",")(1).trim()))
      val rdd2 = rdd1.groupByKey().partitionBy(new HashPartitioner(3)).map(_._2)
      //      print(rdd2.count())
      val length = rdd2.count()
      println("length:", length)
      println("num_partition: ", rdd2.getNumPartitions)
      //      val rdd3 = rdd2.mapPartitions(par=>phase1_map(par, support.toInt, length, n_buckets))
      rdd2.foreach(println)
    } else {
      val rdd1 = rdd.filter(x => x != first).map(x => (x.split(",")(1).trim(), x.split(",")(0).trim()))
      val rdd2 = rdd1.groupByKey()
      rdd2.foreach(println)
    }

  }
}