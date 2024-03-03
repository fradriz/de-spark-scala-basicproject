package de.fr.sparkscala
package utils.spark

import org.apache.spark.sql.{DataFrame, SparkSession}

object settings {

  def setSpark(sessionName: String): SparkSession = {
    val spark = SparkSession.builder()
      .appName(name=sessionName)
      .getOrCreate()

    //.master("local[*]")

    spark.sparkContext.setLogLevel("ERROR")
    spark
  }
}
