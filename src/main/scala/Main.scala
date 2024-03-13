package de.fr.sparkscala

import utils.spark.settings.setSpark
import utils.spark.readers.getCSVFile
import utils.spark.transforms.highestClosingPricesPerYear
import utils.configs.getConfigs.getInputArguments
import utils.Logging

import utils.spark.writers.writeParquet
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

import java.sql.Date

// Define a case class to represent your data structure (schema for the dataset)
case class stock(
                  Date: Date,
                  Open: Double,
                  High: Double,
                  Low: Double,
                  Close: Double,
                  `Adj Close`: Double,
                  Volume: Int)

case class State(state: String, stateName: String)


object Main extends Logging {

  private def flatProduct(t: Product): Iterator[Any] = t.productIterator.flatMap {
    case p: Product => flatProduct(p)
    case x => Iterator(x)
  }

  def main(args: Array[String]): Unit = {
    // Reading input arguments
    log("Reading input arguments in Main")
    val inputParameters = getInputArguments(args: Array[String])

    // Getting the sparkSession
    val spark: SparkSession = setSpark(sessionName="Spark Scala - Basic Project")
    log("Spark session parameters:")
    val arr = spark.sparkContext.getConf.getAll
    for (tup <- arr) println(flatProduct(tup).mkString(" = "))

    import spark.implicits._

    // Read the data into a dataframe
    val path_to_file = inputParameters.path_to_file
    log(s"Reading data from $path_to_file")
    val df: DataFrame = getCSVFile(path_to_file, spark)
    df.show(2)
    df.printSchema()

    // Dataframe to Dataset
    val stockDS: Dataset[stock] = df.as[stock]
    stockDS.show(5)
    stockDS.printSchema()

    // Display the type
    println("Type:")
    println(stockDS.getClass)

    // Another way to create a DS
    println("-- Another DS2 --")
    val ds2 = Seq(
      State("CA", "California"),
      State("NY", "New York")
    ).toDS()
    ds2.show()

    // Transforming the DF: Window function
    val df_windowed: DataFrame = highestClosingPricesPerYear(df)

    df_windowed.show(5)
    //df_windowed.explain(extended = true)

    //Loading to parquet
    writeParquet(df_windowed.limit(5), dest=inputParameters.dest_path + "/window_dataframe")

    println("-- Spark process finished --")
    spark.stop()
  }

}



