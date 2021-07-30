package LearningPack

object StringInterpolation {
  
  def main(args: Array[String]): Unit ={
   
    println("Hello String Interpolation ");
    val name ="Parani";
    val age = 31;
    
    println(name+ "is "+ age +" years old");
    println(s"$name is $age years old");
    println(f"$name%s is $age%d years old");
    println(s"Hello \nWorld");
    println(raw"Hello \n World");
  }
}