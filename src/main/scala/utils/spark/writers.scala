package de.fr.sparkscala
package utils.spark

import de.fr.sparkscala.utils.Logging
import org.apache.spark.sql.{DataFrame, SparkSession}

object writers extends Logging{

  def writeParquet(df: DataFrame, dest: String): Unit = {
    log(s"Writing DF to $dest in parquet format")
    df
      .write
      .mode("overwrite")
      .parquet(dest)
  }
}
