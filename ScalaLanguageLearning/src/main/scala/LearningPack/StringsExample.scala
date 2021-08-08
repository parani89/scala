package LearningPack

object StringsExample {
  
  // This string is same as java.lang.string
  
  val str1 : String = "Hello World ";
  val str2 : String = "Parani ";
  val num1 = 75;
  val num2 = 100.25;
  def main(srga:Array[String]): Unit = {
    
    println("Inside String ");
    println("LEN = "+str1.length());
    println("CONCAT = "+str1.concat(str2));
    println("CONCAT PLUS = "+str1+str2);
    
    val result = printf("%d -- %f -- %s", num1,num2,str1); // This returns the unit so adding () braket
    println(result);
    
    printf("%d -- %f -- %s", num1,num2,str1);
    println;
    printf("%d -- %f -- %s".format(num1,num2,str1));
    
    
  }
}