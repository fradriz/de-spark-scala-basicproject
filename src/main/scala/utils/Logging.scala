package de.fr.sparkscala
package utils

// Define a trait for logging the activity
trait Logging {
  def log(message: String): Unit = {
    println(s"LOGGER - $message")
  }
}
