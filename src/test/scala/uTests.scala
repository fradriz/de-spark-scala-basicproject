package de.fr.sparkscala

import utils.spark.transforms
import org.apache.spark.sql.types.{DateType, DoubleType, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders, Row, SparkSession}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.must.Matchers.contain
import org.scalatest.matchers.should.Matchers.convertToAnyShouldWrapper

import java.sql.Date

class uTests extends AnyFunSuite {
  private val spark = SparkSession
    .builder()
    .appName("SparkSession Test")
    .master("local[*]")
    .getOrCreate()

  private val testSchema = StructType(Seq(
    StructField("Date", DateType, nullable = true),
    StructField("Open", DoubleType, nullable = true),
    StructField("Close", DoubleType, nullable = true)
    )
  )

  // TEST 1
  test("Mock Test 'add' function: 2 + 4 => 6") {
    assert(transforms.add(2, 4) == 6)
  }

  // TEST 2
  test("Test 'highestClosingPricesPerYear' Method") {

    spark.sparkContext.setLogLevel("ERROR")

    val testRows = Seq (
      Row(Date.valueOf("2021-01-12"), 1.0, 2.0 ),     //Java date = YYYY-MM-DD
      Row(Date.valueOf("2023-03-01"), 1.0, 2.0 ),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0 ),
      Row(Date.valueOf("2022-05-12"), 1.0, 6.0 )
    )

    val expectedRows = Seq(
      Row(Date.valueOf("2021-01-12"), 1.0, 2.0 ),
      Row(Date.valueOf("2023-01-12"), 1.0, 3.0 ),
      Row(Date.valueOf("2022-05-12"), 1.0, 6.0 )
    )

    implicit val encoder: Encoder[Row] = Encoders.row(testSchema)

    val testDS = spark.createDataset(testRows)

    val resultList = transforms.highestClosingPricesPerYear(testDS).collect()

    //resultList should contain allElementsOf(expected)
    resultList should contain theSameElementsAs expectedRows     //Order is not relevant.
  }
}
