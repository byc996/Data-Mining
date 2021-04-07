import org.apache.spark.{SparkConf, SparkContext}

import java.io.{File, PrintWriter}
import java.util.Date
import scala.collection.mutable
import scala.util.control.Breaks._

class Node(na: Int){
  var name: Int = na
  var value: Double = 1.0
  var parents: Set[Node] = Set()
  var children: Set[Node] = Set()
}

object task2 {

  def get_communities(nodes: Set[Int], edges: Set[(Int, Int)]): Seq[Set[Int]] = {
    var visited_nodes:Set[Int] = Set()
    var communities:List[Set[Int]] = List()
    var tmp_edges = edges
    breakable {
      for (n <- nodes) {
        if (visited_nodes.size == nodes.size) break
        else if (!visited_nodes.contains(n)) {
          var community:Set[Int] = Set()
          var candidate_nodes:Set[Int] = Set(n)
          while (candidate_nodes.nonEmpty) {
            val tmp_candidate = candidate_nodes
            for (nn <- tmp_candidate) {
              for (e <- tmp_edges) {
                if (List(e._1, e._2).contains(nn)){
                  for (p <- List(e._1, e._2)){
                    if (!visited_nodes.contains(p)){
                      candidate_nodes += p
                      visited_nodes += p
                      community += p
                    }
                  }
                  tmp_edges -= e
                }
              }
              candidate_nodes -= nn
            }
          }
          if (community.isEmpty) communities = communities :+ Set(n)
          else communities = communities :+ community
        }
      }
    }
    communities
  }

  def tree_betweenness(c_edges:Set[(Int, Int)], level_dict: Map[Int, mutable.Set[Node]]): Map[(Int, Int), Double] = {
    var nodes_betweenness:Map[Int, Double] = Map()
    var edges_betweenness:Map[(Int, Int), Double] = Map()
    c_edges.foreach(item=>edges_betweenness += (item -> 0.0))
//    edges.foreach(item=>edges_betweenness += (item -> 0.0))
    val levels = level_dict.keys.toList.sorted.reverse
    for (level <- levels) {
      for (node <- level_dict.getOrElse(level, mutable.Set())){
        val children = node.children
        if (children.isEmpty) {nodes_betweenness += (node.name -> 1)}
        else {
          val children_links = children.map(child=>(node.name, child.name)).map(item=>List(item._1, item._2).sorted)
            .map(item=>(item.head, item.apply(1))).toList
//          println(children_links.head, children_links.apply(1))
//          val chil = children_links.map(link=> edges_betweenness.get(link).head)
//          val tmp_s = 1+ children_links.map(link=> edges_betweenness.get(link).head).sum
          nodes_betweenness += (node.name -> (1+ children_links.map(link=> edges_betweenness.get(link).head).sum))
//          println("kkkkkkk", nodes_betweenness.getOrElse(node.name, 0))
        }

        if (node.parents.nonEmpty) {
          var weights_dict:Map[Node, Int] = Map()
          if (node.parents.head.parents.nonEmpty) node.parents.foreach(n=>{
            weights_dict += (n -> n.parents.size)
          }) else node.parents.foreach(n=>weights_dict += (n -> 1))
          val score:Double = (1.0 / weights_dict.values.sum) * nodes_betweenness.getOrElse(node.name, 0.0)
//          println(score, weights_dict.values.sum, nodes_betweenness.getOrElse(node.name, 0.0))
//          println(1.0 / weights_dict.values.sum, nodes_betweenness.getOrElse(node.name, 0.0), score)
          for (p <- node.parents) {
            val link = List(node.name, p.name).sorted
//            if(link.head == 2 && link.apply(1) == 5) println(nodes_betweenness)
            edges_betweenness += ((link.head, link.apply(1)) -> score * weights_dict.getOrElse(p, 0))
          }
        }
      }
    }
//    println(edges_betweenness)
    edges_betweenness
  }

  def get_tree_betweenness(node: Int, c_edges:Set[(Int, Int)], edges_dict: Map[Int, mutable.Set[Int]]): Map[(Int, Int), Double] = {
    var root_node = new Node(node)
    var level = 0
    var level_dict:Map[Int, mutable.Set[Node]] = Map(level -> mutable.Set(root_node))

    var node_dict = Map(root_node.name->root_node)
    var tmp_edges_dict: Map[Int, mutable.Set[Int]] = Map()
//    edges_dict.foreach(item=>tmp_edges_dict += (item._1 -> item._2:_*))
    for (k <- edges_dict.keys) {tmp_edges_dict += (k -> mutable.Set(edges_dict.getOrElse(k, mutable.Set()).toSeq:_*))}
    while (level_dict.getOrElse(level, mutable.Set()).nonEmpty) {
      level += 1
      level_dict += (level -> mutable.Set())
      for (node <- level_dict.getOrElse(level-1, mutable.Set())) {
        var children:Set[Node] = Set()
        tmp_edges_dict.getOrElse(node.name, mutable.Set()).foreach(x=>{
          if (!node_dict.contains(x)) node_dict += (x -> new Node(x))
          children += node_dict.get(x).head
        })
//        println(level, children.map(_.name))
        for (c <- children) {
          breakable {
            node_dict += (c.name -> c)
            tmp_edges_dict += (node.name -> (tmp_edges_dict.getOrElse(node.name, mutable.Set()) -= c.name))
            tmp_edges_dict += (c.name -> (tmp_edges_dict.getOrElse(c.name, mutable.Set()) -= node.name))
            if (level_dict.getOrElse(level-1, mutable.Set()).contains(c)) break
            node.children += c
            c.parents += node
            level_dict += (level -> (level_dict.getOrElse(level, mutable.Set()) += c))
          }
        }
      }
    }
//    for (item <- level_dict){
//      println(item._1, item._2.map(item=>(item.name,item.parents.map(_.name))))
//    }
//    println(c_edges)
    tree_betweenness(c_edges, level_dict)
  }

  def calculate_graph_betweenness(communities: Seq[Set[Int]], edges: Set[(Int, Int)]): Map[(Int, Int), Double] = {
    var edge_betweenness: Map[(Int, Int), Double] = Map()
    for (c <- communities) {
      var edges_dict:Map[Int, mutable.Set[Int]]= Map()
      var c_edges:Set[(Int, Int)] = Set()
      var tmp_edges = edges
      for (edge <- tmp_edges){
        if (c.contains(edge._1) || c.contains(edge._2)){
          c_edges += edge
          tmp_edges -= edge
        }
      }
      for (e<- c_edges) {
        edges_dict += (e._1 -> (edges_dict.getOrElse(e._1, mutable.Set()) + e._2))
        edges_dict += (e._2 -> (edges_dict.getOrElse(e._2, mutable.Set()) + e._1))
      }
      for (node <- c) {
        val betweenness: Map[(Int, Int), Double] = get_tree_betweenness(node, c_edges, edges_dict)

        for (item <- betweenness) {
          edge_betweenness += (item._1 -> (edge_betweenness.getOrElse(item._1, 0.0)+ item._2) )
        }
      }

    }
    edge_betweenness.map(item=>(item._1, item._2 / 2))
  }

  def calculate_modularity(communities: Seq[Set[Int]], m: Int, nodes_degree: Map[Int, Int], adjacent_dict: Map[(Int, Int), Int]): Double = {
    var modularity = 0.0
    for (c <- communities) {
      var tmp_m = 0.0
      for (pair <- c.toList.combinations(2)){
        var p = (pair.min, pair.max)
        val aij = adjacent_dict.getOrElse(p, 0)
        tmp_m += aij - nodes_degree.getOrElse(p._1, 0) * nodes_degree.getOrElse(p._2,0) / 2.0 / m
      }
      modularity += tmp_m
    }
    modularity / 2.0 / m
  }

  def main(args: Array[String]): Unit = {
    val start_time = new Date().getTime
    val conf = new SparkConf().setMaster("local[3]").setAppName("task1")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val filter_threshold = args(0) //"7"
    val input_file_path = args(1) //"E:\\Data\\inf553\\hw4\\ub_sample_data.csv"
    val betweenness_output_file_path = args(2) //"task2_1_scala"
    val community_output_file_path = args(3) //"task2_2_scala"

    val rdd = sc.textFile(input_file_path)
    val header = rdd.first()
    val rdd1 = rdd.filter(item=>item!=header).map(_.split(",")).map(item=>Tuple2(item.head, item.apply(1)))
    val user_index = rdd1.map(_._1).distinct().zipWithIndex().collectAsMap()
    val index_user = user_index.map(item=>item._2->item._1)
    val business_index = rdd1.map(_._2).distinct().zipWithIndex().collectAsMap()
    val user_profile_dict = rdd1.map(item=>(user_index.get(item._1).head, business_index.get(item._2).head))
      .groupByKey().mapValues(_.toSeq).collectAsMap()
    val unique_user_index = sc.parallelize(index_user.keys.toSeq)
    var nodes:Set[Int] = Set()
    var edges:Set[(Int, Int)] = Set()
    unique_user_index.cartesian(unique_user_index).filter(item=>item._1 < item._2)
      .filter(item=>(user_profile_dict.get(item._1).head.toSet.intersect(user_profile_dict.get(item._2).head.toSet)).size >= filter_threshold.toInt)
      .collect().foreach(item=>{
      nodes += item._1.toInt
      nodes += item._2.toInt
      edges += ((item._1.toInt, item._2.toInt))
    })
    var communities = get_communities(nodes, edges)
    var betweenness_edges = calculate_graph_betweenness(communities, edges) //val betweenness_edges =
//    println(betweenness_edges.toList.sortBy(_._2).reverse)
    val pw = new PrintWriter(new File(betweenness_output_file_path))
    var sorted_betweenness_edges = betweenness_edges.map(item=>(List(index_user.getOrElse(item._1._1, ""), index_user.getOrElse(item._1._2, "")), item._2))
      .map(item=>((item._1.min, item._1.max), item._2)).toList.sortBy(_._2).reverse
    sorted_betweenness_edges.foreach(item=>pw.write("('"+item._1._1+"', '"+item._1._1+"'), "+item._2.toString+"\n"))
    pw.close()

    var nodes_degree:Map[Int, Int] = Map()
    for (e<-betweenness_edges.keys) {
      nodes_degree += (e._1 -> (nodes_degree.getOrElse(e._1, 0) + 1))
      nodes_degree += (e._2 -> (nodes_degree.getOrElse(e._2, 0) + 1))
    }
//    println(nodes_degree)
    var adjacent_dict:Map[(Int,Int),Int] = Map()
    for (pair <- nodes_degree.keys.toList.combinations(2)){
      val tmp = (pair.min, pair.max)
      var value = 0
      if (edges.contains(tmp)) adjacent_dict += (tmp -> 1)
    }
    var modularity = -1.0
    var max_modularity = -1.0
    val m = edges.size
//    println(m)
    var pre_communities = communities
    var sorted_dict = betweenness_edges.toList.sortBy(_._2).reverse
    while (modularity >= max_modularity) {
      pre_communities = communities
      max_modularity = modularity
//      println(communities.size, modularity, max_modularity)
      val highest_betweenness = sorted_dict.head._2
//      var copy_sorted = sorted_betweenness_edges
      sorted_dict = sorted_dict.filter(item=>item._2!=highest_betweenness)
      edges = sorted_dict.map(_._1).toSet
      communities = get_communities(nodes, edges)
      modularity =  calculate_modularity(communities, m, nodes_degree, adjacent_dict)
      betweenness_edges = calculate_graph_betweenness(communities, edges)
      sorted_dict = betweenness_edges.toList.sortBy(_._2).reverse
    }
    val pw1 = new PrintWriter(new File(community_output_file_path))
    val sorted_communities = pre_communities.map(_.map(item=>index_user.getOrElse(item, "")).toList.sorted).sortBy(_.head).sortBy(_.size)
    sorted_communities.map(item=>item.map(i=>"'"+i+"'")).foreach(item=>pw1.write(item.mkString(",")+"\n"))
    pw1.close()
    println("time: "+(new Date().getTime-start_time)/1000.0+"s")
  }
}
