package de.fr.sparkscala
package utils.configs

object getConfigs {

  val usage: String =
    """Usage:
    spark-submit --master "local[*]" spark-scala212_2.12-0.1.0-SNAPSHOT.jar \
      --path_to_file='path_to_csv_file' \
      --dest_path=path to save the results"""

  case class inputParameters(
                              path_to_file: String,
                              dest_path: String)

  def getInputArguments(args: Array[String]): inputParameters = {
    var path_to_file = "/data"          // Default value
    var dest_path = "/tmp/scala_spark" // Default value

    // Parse command line arguments
    if (args.length > 0) {
      // Args to (key, value) map
      val argMap = args.map(_.split("=")).collect { case Array(key, value) => key.drop(2) -> value }.toMap
      // Retrieve named arguments
      path_to_file = argMap.getOrElse("path_to_file", "/data")
      dest_path = argMap.getOrElse("dest_path", "/tmp/scala_spark")

      //println(s"Reading data from: '$path_to_file'")
      //println(s"ETL Type to process: '$etl_type'")

    }
    else {
      println(usage)
      throw new IllegalArgumentException("Argument 'path_to_file' is mandatory, please enter the path to the CSV file")
    }

    // Return
    inputParameters(path_to_file, dest_path)

  }
}
