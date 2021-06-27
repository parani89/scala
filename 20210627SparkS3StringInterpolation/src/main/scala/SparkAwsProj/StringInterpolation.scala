package SparkAwsProj

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.SparkSession;

object StringInterpolation {
  
  def main(args:Array[String]): Unit ={
    
    
    println("==== String Interpolation =======");
    
    val name = "parani";
    println("I am name");
    println(s"I am $name");
    
    val currentDate = java.time.LocalDate.now().toString();
    
    println(s"Current Time is $currentDate");
  }
}