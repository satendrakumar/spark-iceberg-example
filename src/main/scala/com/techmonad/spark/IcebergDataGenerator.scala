package com.techmonad.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random
import java.sql.Date
import java.time.LocalDate

object IcebergDataGenerator {
  def main(args: Array[String]): Unit = {

    val spark =
      SparkSession
        .builder()
        .appName("Iceberg Data Generator")
        .master("local[*]")
        .config("spark.driver.memory", "8g")
        .config("spark.executor.memory", "8g")
        .config("spark.driver.maxResultSize", "2g")
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
        .config("spark.sql.catalog.rest", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.rest.type", "rest")
        .config("spark.sql.catalog.rest.uri", "http://127.0.0.1:9001/iceberg/")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.shuffle.partitions", "400")
        .getOrCreate()

    import spark.implicits._

    spark.sql("CREATE DATABASE IF NOT EXISTS rest.db;")

    spark.sql(
      """
        |CREATE TABLE IF NOT EXISTS rest.db.customers (
        |  customer_id INT,
        |  customer_name STRING,
        |  date DATE,
        |  transaction_details STRING
        |) USING iceberg
        |PARTITIONED BY (bucket(1000, customer_id), days(date))
  """.stripMargin)

//bucket(1000,customer_id),
    def generateCustomerData(numbers:Seq[Int]): Seq[(Int, String, Date, String)] = {
      val random = new Random()
      numbers.map { i =>
        val customerId = i
        val customerName = s"Customer_$i"
        val date = Date.valueOf(LocalDate.now().minusDays(random.nextInt(180)))  // Random date within the last 6 months
        val transactionDetails = s"Transaction details for customer $i"
        (customerId, customerName, date, transactionDetails)
      }
    }

    // Generate  1,00,000 users with 100 transaction
    (1 to 100).foreach { itr =>
      println("loading ...... " + itr)
      val customerData = generateCustomerData(1 to 100000)
      // Convert to DataFrame
      val customerDF = customerData.toDF("customer_id", "customer_name", "date", "transaction_details")
      // Write the data to an Apache Iceberg table
      customerDF
        .write
        .format("iceberg")
        .mode("append")
        .save("rest.db.customers")
    }

   val df =  spark.sql("SELECT * FROM rest.db.customers;")
    df.show()
    println(df.count())

    // Stop Spark session
    //spark.stop()
  }
}
