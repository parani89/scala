package LearningPack

object MatchedExpressions {
  
  	def main(args: Array[String]) {

  	  val age = 2;
  	  
  	  age match {
  	    case 20 => println(age);
  	    case 21 => println(age);
  	    case 22 => println(age);
  	    case 23 => println(age);
  	    case _ => println("default");
  	  }
  	  
  	  val result = age match {
  	    case 20 => age
  	    case 21 => age
  	    case 22 => age
  	    case 23 => age
  	    case _ => "default";
  	  }
  	  
  	  println(s"result is [$result]");
  	  
  	  val res = age match {
  	    case 0 | 2 | 4 => "even"
  	    case _ => "odd";
  	  }
  	  println(s"result is [$res]");
  	}
  	
}