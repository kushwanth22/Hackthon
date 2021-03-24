package org.realtime.hack

class allmethods extends java.io.Serializable{
  def remspecialchar(str:String):String = {
    return str.replaceAll("[0-9 -?,/_()\\]\\[]", "")  
  }
}

//object allmethods {
//  def main(args:Array[String]){
//    val ins:allmethods = new allmethods()
//    var data = ins.remspecialchar("test123[]- 1,(1,2)")
//    print(data)
//  }
//}