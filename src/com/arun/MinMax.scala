package com.arun
import scala.xml.XML

 import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object MinMax {
  def main(args:Array[String]){    
  System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
			 System.setProperty("spark.sql.warehouse.dir", "file:/G:/spark-2.2.1-bin-hadoop2/spark-2.2.1-bin-hadoop2.7/spark-warehouse")
       val spark = SparkSession.builder.master("local").appName("DevMaxDur").getOrCreate()
       
       val pattern = "^\\d+\\^\\d+\\^(\\d+)\\^";
       val r = Pattern.compile(pattern);
       val pattern2 ="(<nv n=\"DurationSecs\" v=\"\\d+\" />)";
       val r2 =Pattern.compile(pattern2)
       val data  =spark.read.textFile("G:\\Workspace\\SetUpBOX\\Set_Top_Box_Data.txt").rdd
       val result =data.filter(l => {val grp=r.matcher(l) 
            if(grp.find)
                       grp.group(1).equals("118")
                       else false
                       }).map(l => {
             val s = l.split("</d>")
             val dev = s(1).split("\\^")
             val devId = dev(1)
              val grp2=r2.matcher(l) 
                        if(grp2.find){
                              val xml = XML.loadString(grp2.group(1))
                              val name =xml.attribute("n").get.toString
                              if(name.equals("DurationSecs")){
                                       val value =xml.attribute("v").get.toString
                                       ("DurationSecs",java.lang.Long.parseLong(value))}
                              else ("Duration",java.lang.Long.parseLong("0"))              
                   }
                  else
                   ("Duration",java.lang.Long.parseLong("0"))
                  }).groupByKey().filter(l =>{l._1.equals("DurationSecs")}).map({l =>{("Duration Max  "+l._2.max,"Duration Min  "+l._2.min)}})
      
       result.foreach(println)
       spark.stop()
       
   }
}