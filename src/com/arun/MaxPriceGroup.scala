package com.arun
import scala.xml.XML

 import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object MaxPriceGroup {
  def main(args:Array[String]){    
         System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
			 System.setProperty("spark.sql.warehouse.dir", "file:/G:/spark-2.2.1-bin-hadoop2/spark-2.2.1-bin-hadoop2.7/spark-warehouse")
       val spark = SparkSession.builder.master("local").appName("DevMaxDur").getOrCreate()
       
       val pattern = "^\\d+\\^\\d+\\^(\\d+)\\^";
       val r = Pattern.compile(pattern);
       val pattern2 ="(<nv n=\"OfferId\" v=\".+?\" />)";
       val pattern3 ="(<nv n=\"Price\" v=\".+?\" />)"
       val r3 = Pattern.compile(pattern3)
       val r2 =Pattern.compile(pattern2)
       val data  =spark.read.textFile("G:\\Workspace\\SetUpBOX\\Set_Top_Box_Data.txt").rdd
       val result =data.filter(l => {val grp=r.matcher(l) 
            if(grp.find)
                       grp.group(1).equals("102")||grp.group(1).equals("113")
                       else false
                       }).map(l => {
           
              val grp2=r2.matcher(l) 
              val grp3=r3.matcher(l)
                        if(grp2.find && grp3.find){
                              val xml = XML.loadString(grp2.group(1))
                              val value1 =xml.attribute("v").get.toString
                               val xml2 = XML.loadString(grp3.group(1))
                              val value2 =xml2.attribute("v").get.toString 
                              (value1,java.lang.Float.parseFloat(value2))
                   }
                  else
                   ("null",java.lang.Float.parseFloat("0"))
                  }).reduceByKey(_+_).sortBy(_._2,false).take(1)
      
       result.foreach(println)
       spark.stop()
  }
   }