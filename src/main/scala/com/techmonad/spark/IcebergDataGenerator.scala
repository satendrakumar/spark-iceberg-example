package com.techmonad.spark
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import scala.util.Random
import java.sql.Date
import java.time.LocalDate

object IcebergDataGenerator {
  def main(args: Array[String]): Unit = {

    // Create Spark Session with Iceberg support
    val spark =
      SparkSession
        .builder()
      .appName("Iceberg Data Generator")
      .master("local[*]")
      .config("spark.driver.memory", "8g")
      .config("spark.driver.maxResultSize", "4g")
      .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
      .config("spark.sql.catalog.spark_catalog","org.apache.iceberg.spark.SparkSessionCatalog")
      .config("spark.sql.catalog.spark_catalog.type","hive")
      .config("spark.sql.catalog.local","org.apache.iceberg.spark.SparkCatalog")
      .config("spark.sql.catalog.local.type","hadoop")
      .config("spark.sql.catalog.local.warehouse","warehouse")
      .getOrCreate()

    import spark.implicits._

    spark.sql(
      """
        |CREATE TABLE local.db.customers (
        |  customer_id INT,
        |  customer_name STRING,
        |  date DATE,
        |  transaction_details STRING
        |) USING iceberg
        |PARTITIONED BY (days(date))
  """.stripMargin)
    //|CLUSTERED BY (bucket(1000, customer_id))
    // Function to generate random customer data
    def generateCustomerData(numbers:Seq[Int]): Seq[(Int, String, Date, String)] = {
      val random = new Random()
      numbers.map { i =>
        val customerId = i
        val customerName = s"Customer_$i"
        val date = Date.valueOf(LocalDate.now().minusDays(random.nextInt(365 * 5)))  // Random date within the last 5 years
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
      customerDF.write
        .format("iceberg")
        .mode("append")
        .save("local.db.customers")
    }

   val df =  spark.sql("SELECT * FROM local.db.customers;")
    df.show()
    println(df.count())

    // Stop Spark session
    //spark.stop()
  }
}
