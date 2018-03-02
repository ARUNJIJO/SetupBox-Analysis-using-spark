package com.arun
import scala.xml.XML

 import org.apache.spark.sql.SparkSession
import java.util.regex.Matcher;
import java.util.regex.Pattern;
object DevMaxDur {
  def main(args:Array[String]){    
  System.setProperty("hadoop.home.dir", "G:\\hadoop-2.5.0-cdh5.3.2")
			 System.setProperty("spark.sql.warehouse.dir", "file:/G:/spark-2.2.1-bin-hadoop2/spark-2.2.1-bin-hadoop2.7/spark-warehouse")
       val spark = SparkSession.builder.master("local").appName("DevMaxDur").getOrCreate()
       
       val pattern = "^\\d+\\^\\d+\\^(\\d+)\\^";
       val r = Pattern.compile(pattern);
       val pattern2 ="(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)(<nv.*?>)";
       val r2 =Pattern.compile(pattern2)
       val data  =spark.read.textFile("G:\\Workspace\\SetUpBOX\\Set_Top_Box_Data.txt").rdd
       val result =data.filter(l => {//val grp=r.matcher(l) 
           // if(grp.find)
                     //  grp.group(1).equals("100")
                       //else false
         l.contains("^100^")
                       }).map(l => {
             val s = l.split("</d>")
             val dev = s(1).split("\\^")
             val devId = dev(1)
              val grp2=r2.matcher(l) 
                        if(grp2.find){
                              val xml = XML.loadString(grp2.group(4))
                              val name =xml.attribute("n").get.toString
                              if(name.equals("Duration")){
                                       val dur =xml.attribute("v").get.toString
                                       (devId,java.lang.Long.parseLong(dur))}
                              else (devId,java.lang.Long.parseLong("0"))              
                   }
                  else
                   (devId,java.lang.Long.parseLong("0"))
                  }).sortBy(_._2,false)
       result.saveAsTextFile("out3")
                       
       spark.stop()
                      /* .map { x => x.replace("^", "#") }
                            
    val xml_str = result.map { x => x.substring(x.indexOf("<"), x.lastIndexOf("#")) }                        
    
    val tuple2_xml_str = xml_str.map { x => (x.substring(x.indexOf("<"), x.indexOf("#")), x.substring(x.indexOf("#")+1, x.length())) }
                            
    val tuple2_toxml_str = tuple2_xml_str.map(x => (xml.XML.loadString(x._1),x._2))
                                        
    val tuple2_processing_1 = tuple2_toxml_str.map(x => (x._1.\\("nv"), x._2))
    
    val tuple2_duration_device = tuple2_processing_1.map(x => ((x._1.theSeq(x._1.length-5)), x._2))
    
    val tuple2_duration_device2 = tuple2_duration_device.map(x => (x._1 \\"nv" \ "@v", x._2))
    
    val tuple2_durationtoInt_device = tuple2_duration_device2.map(x => (x._1.text.toInt, x._2))
    
    val tuple2_maxduration_withdevice = tuple2_durationtoInt_device.sortByKey(false)
                            
    tuple2_maxduration_withdevice.saveAsTextFile("output3") 
       */
   }
}