import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.fpm.FPGrowth

object task3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("task3")
    val sc = new SparkContext(conf)

    val filter_threshold = "120"
//    val input_file_path = "/home/buyi/data/hw2/user_business.csv"
    val input_file_path = "E:\\Data\\inf553\\hw2\\user_business.csv"
    val output_file_path = "scala_task3_ans"
    val support = "50"

    val rdd = sc.textFile(input_file_path)
    val first = rdd.first()
    val rdd_basket = rdd.filter(x=>x!=first).map(x=>(x.split(",")(0),x.split(",")(1))).groupBy(_._1).mapValues(x=>x.map(_._2).toSet.toArray).map(_._2).filter(x=>x.length > filter_threshold.toInt)
    val count = rdd_basket.count()
    val n_partitions = rdd_basket.getNumPartitions
    val fpg = new FPGrowth().setMinSupport(support.toFloat/count).setNumPartitions(n_partitions)
    val model = fpg.run(rdd_basket)

    model.freqItemsets.collect()
      .foreach { itemset =>
      println(s"${itemset.items.mkString("[", ",", "]")},${itemset.freq}")
    }
  }

}
