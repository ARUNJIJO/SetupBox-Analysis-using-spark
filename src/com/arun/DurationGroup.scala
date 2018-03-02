package com.arun
import scala.xml.XML

 import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object DurationGroup {
  def main(args:Array[String]){    
         System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
			 System.setProperty("spark.sql.warehouse.dir", "file:/G:/spark-2.2.1-bin-hadoop2/spark-2.2.1-bin-hadoop2.7/spark-warehouse")
       val spark = SparkSession.builder.master("local").appName("DevMaxDur").getOrCreate()
       
       val pattern = "^\\d+\\^\\d+\\^(\\d+)\\^";
       val r = Pattern.compile(pattern);
       val pattern2 ="(<nv n=\"DurationSecs\" v=\".+?\" />)";
       val pattern3 ="(<nv n=\"ProgramId\" v=\".+?\" />)"
       val r3 = Pattern.compile(pattern3)
       val r2 =Pattern.compile(pattern2)
       val data  =spark.read.textFile("G:\\Workspace\\SetUpBOX\\Set_Top_Box_Data.txt").rdd
       val result =data.filter(l => {val grp=r.matcher(l) 
            if(grp.find)
                       grp.group(1).equals("115")||grp.group(1).equals("118")
                       else false
                       }).map(l => {
           
              val grp2=r2.matcher(l) 
              val grp3=r3.matcher(l)
                        if(grp2.find && grp3.find){
                              val xml = XML.loadString(grp2.group(1))
                              val val1 =xml.attribute("v").get.toString
                               val xml2 = XML.loadString(grp3.group(1))
                              val val2 =xml2.attribute("v").get.toString 
                              (val2,val1)
                   }
                  else
                   ("null",java.lang.Long.parseLong("0"))
                  }).groupByKey
      
       result.foreach(l =>{val a=l._1; print("("+a);val b= l._2.foreach(l => {print(","+l)});println(")")  })
       spark.stop()
  }
   }