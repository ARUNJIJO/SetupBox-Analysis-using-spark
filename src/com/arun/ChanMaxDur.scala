package com.arun
import scala.xml.XML

 import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object ChanMaxDur {
  def main(args:Array[String]){    
  System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
			 System.setProperty("spark.sql.warehouse.dir", "file:/G:/spark-2.2.1-bin-hadoop2/spark-2.2.1-bin-hadoop2.7/spark-warehouse")
       val spark = SparkSession.builder.master("local").appName("DevMaxDur").getOrCreate()
       
       val pattern = "^\\d+\\^\\d+\\^(\\d+)\\^";
       val r = Pattern.compile(pattern);
       val pattern2 ="(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)";
       val r2 =Pattern.compile(pattern2)
       val data  =spark.read.textFile("G:\\Workspace\\SetUpBOX\\Set_Top_Box_Data.txt").rdd
       val result =data.filter(l => {val grp=r.matcher(l) 
            if(grp.find)
                       grp.group(1).equals("100")
                       else false
                       }).map(l => {
             var name="null"
              val grp2=r2.matcher(l) 
                        if(grp2.find){
                              val xml = XML.loadString(grp2.group(4))
                              var attName =xml.attribute("n").get.toString
                              if(attName.equals("Duration")){
                                   val value =xml.attribute("v").get.toString
                                   val xml2 =XML.loadString(grp2.group(3))
                                  attName =xml2.attribute("n").get.toString
                              if(attName.equals("ChannelNumber")){
                                        name =xml2.attribute("v").get.toString
                              }
                                       (name,java.lang.Long.parseLong(value))}
                              else (name,java.lang.Long.parseLong("0"))              
                   }
                  else
                   (name,java.lang.Long.parseLong("0"))
                  }).reduceByKey(_+_).sortBy(_._2,false).take(5)
      
       result.foreach(println)
       spark.stop()
       
   }
}


