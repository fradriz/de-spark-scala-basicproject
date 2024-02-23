package de.fr.sparkscala
package utils.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object readers {

  def getCSVFile(path_to_file: String, spark: SparkSession): DataFrame = {
    // https://spark.apache.org/docs/latest/sql-data-sources-csv.html
    val df: DataFrame = spark.read
      .option("sep", ",")
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(path_to_file)
    df
  }
}
