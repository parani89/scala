package LearningPack

object HigherOrderFunction {
  
  def math(x : Double, y : Double, f : (Double, Double) => Double) : Double = f (x,y); 
  def math1(x : Double, y : Double, z : Double, f : (Double, Double) => Double) : Double = f (f(x,y), z); 
  
  def main(args :Array[String]) : Unit = {
    
    println("========= Two arguments ==========");
    
    var result = math(10, 20, (x,y) => x max y);
    println(result);
    
    result = math(10, 20, (x,y) => x min y);
    println(result);
    
    result = math(10, 20, (x,y) => x + y);
    println(result);
    
    println("========= Three arguments ==========");
    
    var result1 = math1(10, 20, 40, (x,y) => x max y);
    println(result1)
    
    var result2 = math1(10, 20, 40,  _ max _ );
    println(result1)
  }
}