package LearningPack

object IfElse {
  
  def main(args: Array[String]): Unit ={
    
    var x = 30;
    var res ="";
    
    if(x==20 && x!=20) {
      res="x==20";
    }
    else if (x==30) {
      res="x==30";
    }
    else {
      res="X!=20";
    }
    
    println(s"Result is [$res]");
    println(if (x==20) "x==20" else "x!=20");
  }
}