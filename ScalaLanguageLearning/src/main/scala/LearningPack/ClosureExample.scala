package LearningPack

object ClosureExample {

	var number = 10;
	val add = (x : Int ) => {
		number = x + number;
		number;
	}
	
	/*  Closure is a function which uses one or more variables declared outside the function. 
	 *  If we use var, it's impure closure. If we use val, it's pure closure.
	 */
	
	def main(args: Array[String]) : Unit = {

	  number = 200;
	  println(add(100));
	  println(number);
	}
}