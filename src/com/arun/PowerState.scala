package com.arun
import scala.xml.XML

 import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object PowerState {
  def main(args:Array[String]){    
  System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
			 System.setProperty("spark.sql.warehouse.dir", "file:/G:/spark-2.2.1-bin-hadoop2/spark-2.2.1-bin-hadoop2.7/spark-warehouse")
       val spark = SparkSession.builder.master("local").appName("DevMaxDur").getOrCreate()
       
       val pattern = "^\\d+\\^\\d+\\^(\\d+)\\^";
       val r = Pattern.compile(pattern);
       val pattern2 ="(<nv n=\"PowerState\" v=\".*?\" />)";
       val r2 =Pattern.compile(pattern2)
       val data  =spark.read.textFile("G:\\Workspace\\SetUpBOX\\Set_Top_Box_Data.txt").rdd
       val result =data.filter(l => {val grp=r.matcher(l) 
            if(grp.find)
                       grp.group(1).equals("101")
                       else false
                       }).map(l => {
           
              val grp2=r2.matcher(l) 
                        if(grp2.find){
                              val xml = XML.loadString(grp2.group(1))
                              val name =xml.attribute("n").get.toString
                              if(name.equals("PowerState")){
                                       val value =xml.attribute("v").get.toString
                                       if(value.equals("ON"))
                                       ("Power State On",1)
                                       else ("Power State off",1)}
                              else ("null",1)              
                   }
                  else
                   ("null",1)
                  }).reduceByKey(_+_)
      
       result.foreach(println)
       spark.stop()
       
   }
}