package com.tk

import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction
import org.json.JSONObject
import phoenixHbase.PhoenixClient_v2

class SinkData2phoenix extends RichSinkFunction[String]{
  var zkip=""
  var zkport=""
  var phoenixclient:PhoenixClient_v2=null
  override def open(parameters: Configuration): Unit ={
    zkip="192.168.12.248"
    zkport="2181"
    phoenixclient=new PhoenixClient_v2(zkip,zkport)
  }
  override def invoke(s:String): Unit ={
    val s2=s.split("\\|")(1)
    println("sssss"+s2)
    val a=100
    val b=a+101
    //phoenix_yuju(s+b.toString)
  }
  def phoenix_yuju(str:String): Unit ={
    val phoenixSQL="upsert into \"test\" (ROW1,\"city\") values ('4','"+str+"') "
    phoenixclient.Update_Insert_Delete(phoenixSQL)
    println(phoenixSQL)
  }
  def phoenix_operation(): Unit ={
    val phoenixSQL="select * from STAT_REALTIMEBOSS where KEYTYPE=''"
  }
  def isJSONValid(teststr:String):Boolean = {
    var B:Boolean=true
    try {
      new JSONObject(teststr)
      //println(json.get("account"))
    }catch {
      case ex: Exception=>B=false
    }
    B
  }
}
