package de.fr.sparkscala
package utils.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, row_number, year}

object transforms {

  def highestClosingPricesPerYear(df: DataFrame): DataFrame = {
    // Se obtiene la sparkSession desde el DF !!
    import df.sparkSession.implicits._
    val window = Window.partitionBy(year(col("Date")).as("year")).orderBy(col("Close").desc)

    df
      .withColumn("rank", row_number().over(window))
      .filter($"rank" === 1)
      .drop($"rank")
      .sort($"close".desc)

  }

  // Definimos una función para probar en test
  def add(x: Int, y: Int): Int = x + y

}
