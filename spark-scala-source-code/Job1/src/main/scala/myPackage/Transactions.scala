package myPackage

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{desc, first, lit, sum}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

class Transactions(order: DataFrame, products: DataFrame, output_path: String) {

  // class variable
  val __order: DataFrame = order
  val __products: DataFrame = products
  val __output_path: String = output_path

  // run Transactions
  def run (): Unit = {

    // iki dataframei birbirine productid uzerinden joinleme
    val join_data = joinDataFrames(__order, __products)

    // net satış adedi ve tutarı
    val netSalesDF = calculateNetSales(join_data)

    // brüt satış adedi ve tutarı
    val grossSalesDF = calculateGrossSales(join_data)

    // satıldığı son 5 gündeki satış sayısı ortalamaları
    val dailySalesAverageDF = calculateAverageSalesPerProduct(join_data)

    // en çok sattığı yer
    val mostSoldLocationForAllProductsDF = calculateMostSoldLocationForAllProducts(join_data)

    // fonksiyonlardan donen tum dataframeleri birbirine joinleyerek tek tablo yapmak
    val join_data_final = joinFinalData(netSalesDF, grossSalesDF, dailySalesAverageDF, mostSoldLocationForAllProductsDF)
    join_data_final.show()

    // dosya olarak yazmak
    writeDataFrame(join_data_final)

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

  // net satış adedi ve tutarı
  // satış - (iade+iptal)
  def calculateNetSales(df: DataFrame): DataFrame = {

    // "product_id" sütunu üzerine gruplama
    val netSalesDF = df.groupBy("product_id")
      // gruplama işlemi sonrasında, toplu veri analizi yapma
      .agg(
        // uc tipe gore fiyat toplami
        sum(when(col("status") === "Created", col("price")).otherwise(lit(0))).alias("total_sales_amount"),
        sum(when(col("status") === "Cancelled", col("price")).otherwise(lit(0))).alias("total_cancelled_amount"),
        sum(when(col("status") === "Returned", col("price")).otherwise(lit(0))).alias("total_returned_amount"),

        // uc tipe gore adet toplami
        sum(when(col("status") === "Created", lit(1)).otherwise(lit(0))).alias("total_sales_quantity"),
        sum(when(col("status") === "Cancelled", lit(1)).otherwise(lit(0))).alias("total_cancelled_quantity"),
        sum(when(col("status") === "Returned", lit(1)).otherwise(lit(0))).alias("total_returned_quantity")
      )
      // islemleri yapip yeni column ekleme
      .withColumn("net_sales_price", col("total_sales_amount") - (col("total_cancelled_amount") + col("total_returned_amount")))
      .withColumn("net_sales_amount", col("total_sales_quantity") - (col("total_cancelled_quantity") + col("total_returned_quantity")))
      // ihtiyac olanlari alma
      .select("product_id", "net_sales_amount", "net_sales_price")

    // net satis adeti 0 dan kucukse alma
    //val filteredDF = netSalesDF.filter(col("net_sales_amount") >= 0)

    netSalesDF
  }

  // en çok sattığı yer
  def calculateMostSoldLocationForAllProducts(df: DataFrame): DataFrame = {

    // new window
    // product_id bölünür ve azalan sıra ile sıralanır
    val windowSpec = Window.partitionBy("product_id").orderBy(desc("count"))

    // product ve locationa gore gruplama
    // ardından en cok satilan ili bulma
    val mostSoldLocationDF = df.filter(col("status") === "Created")
      .groupBy("product_id", "location")
      .count()
      .withColumn("rank", row_number().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .withColumn("top_selling_location", col("location"))

    mostSoldLocationDF.select("product_id", "top_selling_location")
  }

  // brüt satış adedi ve tutarı
  // net satış adedi ve tutarı ile benzer bir cozume sahiptir,
  // iptal ve iadeler dahil edilmez
  def calculateGrossSales(df: DataFrame): DataFrame = {
    val grossSalesDF = df.groupBy("product_id")
      .agg(
        sum(when(col("status") === "Created", col("price")).otherwise(lit(0))).alias("gross_sales_price"),
        sum(when(col("status") === "Created", lit(1)).otherwise(lit(0))).alias("gross_sales_amount"),
      )
      .withColumn("gross_sales_amount", col("gross_sales_amount"))
      .withColumn("gross_sales_price", col("gross_sales_price"))
      .select("product_id", "gross_sales_amount", "gross_sales_price")

    grossSalesDF
  }

  // satıldığı son 5 gündeki satış sayısı ortalamaları
  def calculateAverageSalesPerProduct(df: DataFrame): DataFrame = {
    // order_date sütununu tarihe çevir
    val formattedDF = df.withColumn("order_date", substring(col("order_date"), 1, 10)).filter(col("status") === "Created")

    // Ürünleri product_id'ye göre grupla, tarihleri unique hale getir
    val groupedData = formattedDF.groupBy("product_id").agg(collect_set("order_date").as("unique_dates"))

    // Tarihleri en yeniye göre sırala
    val sortedData = groupedData.withColumn("sorted_dates", sort_array(col("unique_dates"), asc = false))

    // Eğer tarih sayısı 5'ten küçükse, tüm tarihleri al. Aksi takdirde ilk 5 tarihi al
    val selectedDates = sortedData.withColumn("selected_dates", when(size(col("sorted_dates")) < 5, col("sorted_dates")).otherwise(slice(col("sorted_dates"), 1, 5)))

    // Seçilen tarihleri explode et
    val explodedData = selectedDates.select(col("product_id"), explode(col("selected_dates")).as("selected_date"))

    // toplam gun sayisi
    val countData = explodedData.groupBy("product_id", "selected_date").agg(count("selected_date").as("count_per_date"))
    val countDataRename = countData.withColumnRenamed("product_id", "productid")

    val sumCountDay = countDataRename.groupBy("productid")
      .agg(sum(col("count_per_date")).alias("count_per_date"))

    val finalResult = countDataRename
      .join(formattedDF, countDataRename("productid") === formattedDF("product_id") && countDataRename("selected_date") === formattedDF("order_date"))
      .groupBy("product_id")
      .agg(sum("count_per_date").as("total_count"))

    val finalDataJoin = finalResult.join(sumCountDay, finalResult("product_id")===sumCountDay("productid"))

    val finalDataSumJoin = finalDataJoin.withColumn("average_sales_amount_of_last_5_selling_days", col("total_count")/col("count_per_date"))

    finalDataSumJoin.select("product_id", "average_sales_amount_of_last_5_selling_days")
  }

  // tum datalar joinlenir
  def joinFinalData(df0: DataFrame, df1: DataFrame, df2: DataFrame, df3: DataFrame): DataFrame = {
    val joinedDF = df0
      .join(df1, Seq("product_id"))
      .join(df2, Seq("product_id"))
      .join(df3, Seq("product_id"))

    joinedDF
  }

}
