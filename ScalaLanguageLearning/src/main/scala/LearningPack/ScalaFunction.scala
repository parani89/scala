package LearningPack

object ScalaFunction {

	object Math {

		def add(x :Int, y :Int) : Int = {
				return x + y;
		} 

		def square(x :Int) : Int = x * x;
	}

	def add(x :Int, y :Int) : Int = {
			return x + y;
	}

	def subtract(x : Int, y : Int) : Int = {
			x - y;
	}

	def multiply(x : Int, y : Int) : Int = x * y;

	def divide(x: Int, y: Int) = x / y;

	def add1(x : Int = 45, y : Int = 45) : Int = {
	  x + y;
	}
	
	def main(args: Array[String]) {

		println("addition "+add(1,3));
		println("subtract "+subtract(1,3));
		println("multiplication "+multiply(3,4));
		println("divide "+divide(6,2));
		println;
		println("Object Add "+Math.add(5,6));
    println("Object Square "+ Math.square(5));
    println("addition default "+add1());
    
    var add2 = (x : Int, y : Int) => x+ y;
    println("Anonymous function "+add2(10,40));
	}

}