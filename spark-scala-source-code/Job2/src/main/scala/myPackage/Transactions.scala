package myPackage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, first, lit, sum, lag}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Transactions(order: DataFrame, products: DataFrame, output_path: String, output_join_path: String) {

  // class variable
  val __order: DataFrame = order
  val __products: DataFrame = products
  val __output_path: String = output_path
  val __output_join_path: String = output_join_path

  // run Transactions
  def run (): Unit = {

    // iki dataframei birbirine productid uzerinden joinleme
    val join_data = joinDataFrames(__order, __products)
    writeJoinData(join_data)

    val productPriceChangesDF = calculatePriceChanges(join_data)
    writeDataFrame(productPriceChangesDF)

  }

  // dosya olarak yazmak
  def writeJoinData(join_data: DataFrame): Unit = {

    join_data.repartition(1).write.option("header", "true") .csv(__output_join_path)
  }

  // dosya olarak yazmak
  def writeDataFrame(final_data: DataFrame): Unit = {

    final_data.repartition(1).write.option("header", "true") .csv(__output_path)
  }

  // iki dataframei birbirine productid uzerinden joinleme
  def joinDataFrames(order: DataFrame, products: DataFrame): DataFrame = {

    // her iki tablodaki productid ye göre "inner" birleştirme
    val resultDF = order.join(products, order("product_id") === products("productid"), "inner")

    // tekrarlayan productid columnu silmek
    val dropcolumn = resultDF.drop("productid")
    dropcolumn
  }

  def calculatePriceChanges(df: DataFrame): DataFrame = {

    // product_id ye gore gruplayip order_date e gore siralama
    val windowSpec = Window.partitionBy("product_id").orderBy("order_date")

    val createDF = df.filter(col("status")==="Created")

    val priceChangeDF = createDF.withColumn("change", lag("price", 1).over(windowSpec))
      // yeni column ekleme ve degisiklige gore deger atama
      .withColumn("change", when(col("price") > col("change"), "rise")
        .when(col("price") < col("change"), "fall")
        .otherwise("no_change"))
      .na.fill("no_change", Seq("change"))  // Boş değerleri "no_change" ile doldurma

    priceChangeDF.select("product_id", "price", "order_date", "change").filter(col("change") =!= "no_change")
  }



}
