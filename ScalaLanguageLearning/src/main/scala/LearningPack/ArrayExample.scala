package LearningPack

import Array._;

object ArrayExample {
  
  // If not defined fefault value of the datatype.
  // 0 for integer and false for Boolean.
  
  val myArray : Array[Int] = new Array[Int](4);
  val myArray2 = new Array[Int](5);
  val myArray3 = Array(1,2,3,4,5,6,7);
  
  def main(args:Array[String]): Unit = {
    
    println("Array Example");
    
    myArray(0) = 20;
    myArray(1) = 50;
    myArray(2) = 2060;
    
    println(myArray);
    
    for(i <- myArray) {
      println(i);
    }
    
    println;
    println;
    
    for(i <- 0 to (myArray.length - 1) ) {
      println(myArray(i));
    }
    
    println;
    println("CONCAT");
    
    val concatArray = concat(myArray,myArray3);
    
    for(i <- 0 to (concatArray.length - 1) ) {
      println(concatArray(i));
    }
    
    println("MAX ELEMENT "+concatArray.max);
    
  }
}