package myPackage

import org.apache.spark.sql.SparkSession

object Job1 {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Job1")
      .master("spark://spark-master:7077")
      .config("spark.cores.max", "2")
      .config("spark.executor.memory", "2g")
      .getOrCreate()

    val order_path = "File:///raw-data/orders.json"
    val products_path = "File:///raw-data/products.json"
    val output_path = "/opt/bitnami/spark/job1_out"

    val order = spark.read.json(order_path)
    val products = spark.read.json(products_path)

    val transactions = new Transactions(order, products, output_path)
    transactions.run()

    spark.stop()
  }

}
