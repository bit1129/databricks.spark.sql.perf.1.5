package test

object QueryTest {
  def main(args: Array[String]): Unit = {
    val sqlContext = null
    import com.databricks.spark.sql.perf.tpcds.Tables
    val dsdgenDir ="/export/App/TPCDSVersion1.3.1/dbgen2"
    val scaleFactor = 30
    val tables = new Tables(sqlContext, dsdgenDir, scaleFactor)
    val location="hdfs://ns1/tmp/spark_perf/spark.1.5"
    val format= "parquet"
    val overwrite = true
    val partitionTables=true
    val useDoubleForDecimal =true
    val clusterByPartitionColumns = true
    val filterOutNullPartitionValues = true


    tables.genData(location, format, overwrite, partitionTables, useDoubleForDecimal, clusterByPartitionColumns, filterOutNullPartitionValues)

//    tables.createExternalTables(location, format, databaseName, overwrite)

    tables.createTemporaryTables(location, format)
    import com.databricks.spark.sql.perf.tpcds.TPCDS

    val tpcds = new TPCDS (sqlContext = sqlContext)

    val experiment = tpcds.runExperiment(queriesToRun =tpcds.q7Derived)

  }
}
