package LearningPack;

object forLoop {

	def main(args: Array[String]): Unit = {
		// Local variable declaration:

	  println("====== For Loops ======");
	  
	  for (i <- 1.to(5)) {
	    println(s"Simple for loop $i");
	  }
	  
	  println
	  
	  for (i <- 1 until 6) {
	    println(s"Untill loop $i");
	  }
	  
	  println
	  
	  for (i <- 1.to(4) ; j <- 1.to(2); k <- 1.to(2)) {
	    println(s"Multi condition i is $i; j is $j; k is $k")
	  }
	  
	  println
	  
	  val lst = List(1,2,3,4,5,6);
	  
	  for (i <- lst) {
	    println(s"List is $i");
	  }
	  
	  println;
	  
	  for (i <- lst; if i <5) {
	    println(s"Filtered list $i");
	  }
	  
	  println;
	  
	  var result = for { i <- lst ; if i < 5 } yield {
	    i * i;
	  }
	  
	  println("==== Squared result =======");
	  println(result);
	}
}