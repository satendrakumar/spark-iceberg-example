package com.techmonad.ducksb


import com.techmonad.catalog.CatalogService
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.{CatalogProperties, Table, TableScan}

import java.sql.{Connection, DriverManager, ResultSet}
import scala.util.{Failure, Success, Try}
import org.slf4j.{Logger, LoggerFactory}
import org.apache.iceberg.expressions.{Expression, Expressions}
import org.apache.iceberg.hive.HiveCatalog
import org.apache.iceberg.rest.RESTCatalog
import org.apache.spark.sql.SparkSession

import scala.jdk.CollectionConverters._

object Reader {


  val logger: Logger = LoggerFactory.getLogger(this.getClass())

  implicit class JsonResult(resultSet:ResultSet) {
    def toStringList:List[String] = {
      new Iterator[String] {
        override def hasNext: Boolean = resultSet.next()
        override def next(): String = resultSet.getString(1)
      }.toList
    }
  }


  private def getIcebergTableByName(namespace: String, tableName: String, catalog: RESTCatalog): Try[Table] = Try{
    val tableID = TableIdentifier.of(namespace, tableName)
    catalog.loadTable(tableID)
  }

  private def scanTableWithPartitionPredicate(table:Table, partitionPredicate:Expression):Try[TableScan] =
    Try(table.newScan.filter(partitionPredicate))

  private def getDataFilesLocations(tableScan:TableScan): Try[String] = Try {
    // chain all files to scan in a single string => "'file1', 'file2'"
    val scanFiles = tableScan.planFiles().asScala
      .map(f => "'" + f.file.path.toString + "'")
    scanFiles.foreach(f => logger.info(s"Got planned data file:$f"))
    scanFiles.mkString(",")
  }

  private def initDuckDBConnection: Try[Connection] = Try {
    val con = DriverManager.getConnection("jdbc:duckdb:")
    con
  }

  private def executeQuery(connection: Connection, query:String):Try[ResultSet] = Try{
    logger.info("query ### " + query)
    connection.createStatement.executeQuery(query)
  }

  private def formatQuery(query:String, dataFilesStr:String):Try[String]  = Try {
    query.replaceAll("<FILES_LIST>", dataFilesStr)
  }

  private def executeIcebergQuery(query:String): List[String] = {
    val partitionPredicate = Expressions.equal("date", "2022-06-28")

    val jsonDataRows = for {
      catalog         <- Try{ CatalogService.getCatalog}
      table           <- getIcebergTableByName("db", "customers", catalog)
      tableScan       <- scanTableWithPartitionPredicate(table, partitionPredicate)
      dataFilesString <- getDataFilesLocations(tableScan)
      queryStatement  <- formatQuery(query, dataFilesString)
      dbConnection    <- initDuckDBConnection
      resultSet       <- executeQuery(dbConnection, queryStatement)
    } yield resultSet.toStringList

    jsonDataRows match {
      case Success(jsonRows) => jsonRows
      case Failure(exception) => {
        logger.error("Error fetching data", exception)
        List[String]()
      }
    }

  }

  def main(args: Array[String]): Unit = {

    val query = """
                  |SELECT row_to_json(lst)
                  |FROM (
                  | SELECT customer_id, customer_name, date,transaction_details
                  | FROM parquet_scan([ <FILES_LIST>])
                  |) lst
                  |""".stripMargin

    val results = executeIcebergQuery(query)
    results.foreach(result => logger.info(result))
  }

}
