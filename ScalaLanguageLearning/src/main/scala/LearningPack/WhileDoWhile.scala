package LearningPack

object WhileDoWhile {

	def main(args: Array[String]) {
		// Local variable declaration:
		var a = 10;
		var y = 20;

		// while loop execution
		while( a < 20 ){
			println( "Value of a: " + a );
			a = a + 1;
		}

		do {
			println(s"do while x is $y")
		} while (y < 1);
	}
}