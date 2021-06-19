package SparkPartitionPack

object MkStringTest {
  
  def main(args:Array[String]):Unit={
    
    val list1 = List(1,2,3,4,5);
    
    val opList = list1.mkString("-");
    val opList1 = list1.mkString;
    
    // Converts List to String
    //opList.foreach(print);
    
    println("===== Result 1 =====");
    println(opList);
    
    println("===== Result 2 =====");
    println(opList1);
    
  }
}