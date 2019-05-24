package com.tk
import java.io.IOException
import java.util.concurrent.TimeUnit

import scala.io.Source
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala.{StreamExecutionEnvironment, _}
import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}
import org.json.JSONObject
import phoenixHbase.PhoenixClient_v2


import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
object Main {
  //  val env=StreamExecutionEnvironment.getExecutionEnvironment
  var kvmap:mutable.HashMap[String,String]=null
  var phoenixclient:PhoenixClient_v2=null
  var zkip=""
  var zkport=""
  def main(args: Array[String]): Unit = {
    //        env.enableCheckpointing(1000)
    //        env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    //        env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)
    //        env.getCheckpointConfig.setCheckpointTimeout(60000)
    //        env.getCheckpointConfig.setFailOnCheckpointingErrors(false)
    //        env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    //        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
    //          3, // number of restart attempts
    //          Time.of(10, TimeUnit.SECONDS) // delay
    //        ))
    //       val dataStream:DataStream[String]=env.addSource(new sourceConsumer)
    //        dataStream.print()
    //        dataStream.addSink(new SinkData2phoenix)
    //        env.execute()
    zkip="192.168.12.248"
    zkport="2181"
    phoenixclient=new PhoenixClient_v2(zkip,zkport)
    //处理数据
    //process_json()
    //查询phoenix中结果
    val resultmap=phoenix_process
    println(resultmap)
  }

  def process_json(): Unit ={
    kvmap=new mutable.HashMap[String,String]()
    var kvresult=new mutable.HashMap[String,String]()
    //val sourcelogin=Source.fromFile("D:\\KK\\boss_RESUME_2019-05-14_20190514_200.txt")
    val source=Source.fromFile("E:\\数据svn公司级\\公用组\\MT4\\日版\\JP_MoneyFlow.log")

    val lines = source.getLines()
    for(line<-lines){
      //println(fieldname.zip(a).toMap)
      if(line.split("\\|")(14).toInt>=31){
        //println(line)
        jsonoperation(line)
      }

    }

    kvmap.map(x=>{
      //按游戏,服务器,设备类型,账号和日期统计总钱数
      if(kvresult.contains(x._1.split("_")(0)+"_"+x._1.split("_")(1)+"_"+x._1.split("_")(2)+"_"+x._1.split("_")(4)+"_"+x._1.split("_")(5)+
        "_"+x._1.split("_")(6)+"_"+x._1.split("_")(7)+"|1")){
        var getval1:Option[String]=kvresult.get(x._1.split("_")(0)+"_"+x._1.split("_")(1)+"_"+x._1.split("_")(2)+"_"+x._1.split("_")(4)+"_"+x._1.split("_")(5)+
          "_"+x._1.split("_")(6)+"_"+x._1.split("_")(7)+"|1")
        var getcharge=getval1.getOrElse("0_0").toString.split("_")(0).toInt+x._2.split("_")(0).toInt
        var getbalance=getval1.getOrElse("0_0").toString.split("_")(1).toInt+x._2.split("_")(1).toInt
        var sumcharge_sumbalanct=getcharge.toString+"_"+getbalance.toString
        kvresult+=(x._1.split("_")(0)+"_"+x._1.split("_")(1)+"_"+x._1.split("_")(2)+"_"+x._1.split("_")(4)+"_"+x._1.split("_")(5)
          +"_"+x._1.split("_")(6)+"_"+x._1.split("_")(7)+"|1"->sumcharge_sumbalanct)
      }else{
        kvresult+=(x._1.split("_")(0)+"_"+x._1.split("_")(1)+"_"+x._1.split("_")(2)+"_"+x._1.split("_")(4)+"_"+x._1.split("_")(5)
          +"_"+x._1.split("_")(6)+"_"+x._1.split("_")(7)+"|1"->x._2)
      }

    })
    println(kvmap)
    println(kvresult)
    write_phoenixData(kvresult)

  }
  def write_phoenixData(resultmap:mutable.Map[String,String]): Unit ={
    //println(resultmap)
    phoenixclient.Update_Insert_Flinkdata(resultmap,"FLINK_REALTIME_MONEY_JP")
  }
  def get_phoenixData(keyname:String): Unit ={
    val phoenixSQL="select * from from FLINK_REALTIME where KEYNAME='"+keyname+"'"
    phoenixclient.Select_Sql(phoenixSQL)
  }
  def jsonoperation(str:String): Unit ={
    val fieldname=Array("Tblename","GameSvrId","dtEventTime","vGameAppid","PlatID","iZoneAreaID","vopenid","Sequence","Level",
      "AfterMoney","iMoney","Reason","SubReason","AddOrReduce","iMoneyType","RoleId","RoleProfession","RoleGuild")
    val a=str.split("\\|")
    val json=fieldname.zip(a).toMap
    println(json)
    //按账号vopenid
    if(kvmap.contains("acc|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("vopenid",0)+"_"+json.getOrElse("iMoneyType",0)+
      "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$"))){
      var getval:Option[String]=kvmap.get("acc|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("vopenid",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$"))
      var sumcharge=getval.getOrElse("0_0").split("_")(0).toInt+json.getOrElse("iMoney",0).toString.toInt
      var lastbalance=json.getOrElse("AfterMoney","0")
      var sumcharge_balance=sumcharge.toString+"_"+lastbalance
      //println(summoney)
      kvmap+=("acc|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("vopenid",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$")-> sumcharge_balance)
    }else {
      var sumcharge_balance=json.getOrElse("iMoney","0")+"_"+json.getOrElse("AfterMoney","0")
      kvmap+=("acc|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("vopenid",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$")->sumcharge_balance)
      //println(json.get("money").toString.toDouble)
    }
    //roleid
    if(kvmap.contains("roleid|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("RoleId",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$"))){
      var getval:Option[String]=kvmap.get("roleid|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("RoleId",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$"))
      var sumcharge=getval.getOrElse("0_0").split("_")(0).toInt+json.getOrElse("iMoney",0).toString.toInt
      var lastbalance=json.getOrElse("AfterMoney","0")
      var sumcharge_balance=sumcharge.toString+"_"+lastbalance
      //println(summoney)
      kvmap+=("roleid|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("RoleId",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$")-> sumcharge_balance)
    }else {
      var sumcharge_balance=json.getOrElse("iMoney","0")+"_"+json.getOrElse("AfterMoney","0")
      kvmap+=("roleid|"+json.getOrElse("vGameAppid",0)+"_"+json.getOrElse("GameSvrId",0)+"_"+json.getOrElse("PlatID",0)+"_"+json.getOrElse("RoleId",0)+"_"+json.getOrElse("iMoneyType",0)+
        "_"+json.getOrElse("AddOrReduce",0)+"_"+json.getOrElse("Reason",0)+"_"+json.getOrElse("dtEventTime",0).toString.split(":")(0).replace(" ","$")->sumcharge_balance)
      //println(json.get("money").toString.toDouble)
    }
  }
  def phoenix_process(): mutable.HashMap[String,String] ={
    val r=phoenixclient.Select_Sql("select * from FLINK_REALTIME_MONEY_JP where KEYNAME in('acc|267_11001_1_2019-05-14$11|1','roleid|267_11001_1_2019-05-14$11|1')")
    //println(r)
    var mapphoenix:mutable.HashMap[String,String]=new mutable.HashMap[String,String]
    for(r1<-r){
      var (keyname1,time)=("","1970-01-01")
      var (hour,serverid,publish31,publish31reason,consume31,consume31reason,killoff31,killoff31reason,balance31)=(0,0,0,0,0,0,0,0,0)
      var (publish32,publish32reason,consume32,consume32reason,killoff32,killoff32reason,balance32)=(0,0,0,0,0,0,0)
      var (publish33,publish33reason,consume33,consume33reason,killoff33,killoff33reason,balance33)=(0,0,0,0,0,0,0)
      var iMoneyType=0
      var AddOrReduce=0
      //println(r1)
      if(r1.getOrElse("PUBLISH31","0")!=null){
        publish31=r1.getOrElse("PUBLISH31","0").toInt
        balance31=r1.getOrElse("BALANCE31","0").toInt
        publish31reason=r1.getOrElse("PUBLISH31REASON","0").toInt
        AddOrReduce=0
        iMoneyType=31
        var zuheresult=publish31+"_"+balance31
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+publish31reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)-> zuheresult)

      }
      if(r1.getOrElse("CONSUME31","0")!=null){
        consume31=r1.getOrElse("CONSUME31","0").toInt
        balance31=r1.getOrElse("BALANCE31","0").toInt
        consume31reason=r1.getOrElse("CONSUME31REASON","0").toInt
        AddOrReduce=1
        iMoneyType=31
        var zuheresult=consume31+"_"+balance31
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+consume31reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }
      if(r1.getOrElse("KILLOFF31","0")!=null){
        killoff31=r1.getOrElse("KILLOFF31","0").toInt
        balance31=r1.getOrElse("BALANCE31","0").toInt
        killoff31reason=r1.getOrElse("KILLOFF31REASON","0").toInt
        AddOrReduce=1
        iMoneyType=31
        var zuheresult=killoff31+"_"+balance31
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+killoff31reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }

      if(r1.getOrElse("PUBLISH32","0")!=null){
        publish32=r1.getOrElse("PUBLISH32","0").toInt
        balance32=r1.getOrElse("BALANCE32","0").toInt
        publish32reason=r1.getOrElse("PUBLISH32REASON","0").toInt
        AddOrReduce=0
        iMoneyType=32
        var zuheresult=publish32+"_"+balance32
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+publish32reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }
      if(r1.getOrElse("CONSUME32","0")!=null){
        consume32=r1.getOrElse("CONSUME32","0").toInt
        balance32=r1.getOrElse("BALANCE32","0").toInt
        consume32reason=r1.getOrElse("CONSUME32REASON","0").toInt
        AddOrReduce=1
        iMoneyType=32
        var zuheresult=consume32+"_"+balance32
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+consume32reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }
      if(r1.getOrElse("KILLOFF32","0")!=null){
        killoff32=r1.getOrElse("KILLOFF32","0").toInt
        balance32=r1.getOrElse("BALANCE32","0").toInt
        killoff32reason=r1.getOrElse("KILLOFF32REASON","0").toInt
        AddOrReduce=1
        iMoneyType=32
        var zuheresult=killoff32+"_"+balance32
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+killoff32reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }

      if(r1.getOrElse("PUBLISH33","0")!=null){
        publish33=r1.getOrElse("PUBLISH33","0").toInt
        balance33=r1.getOrElse("BALANCE33","0").toInt
        publish33reason=r1.getOrElse("PUBLISH33REASON","0").toInt
        AddOrReduce=0
        iMoneyType=33
        var zuheresult=publish33+"_"+balance33
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+publish33reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }
      if(r1.getOrElse("CONSUME33","0")!=null){
        consume33=r1.getOrElse("CONSUME33","0").toInt
        balance33=r1.getOrElse("BALANCE33","0").toInt
        consume33reason=r1.getOrElse("CONSUME33REASON","0").toInt
        AddOrReduce=1
        iMoneyType=33
        var zuheresult=consume33+"_"+balance33
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+consume33reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }
      if(r1.getOrElse("KILLOFF33","0")!=null){
        killoff33=r1.getOrElse("KILLOFF33","0").toInt
        balance33=r1.getOrElse("BALANCE33","0").toInt
        killoff33reason=r1.getOrElse("KILLOFF33REASON","0").toInt
        AddOrReduce=1
        iMoneyType=33
        var zuheresult=killoff33+"_"+balance33
        mapphoenix+=(r1.getOrElse("KEYNAME","0").split("_")(0)+"_"+r1.getOrElse("KEYNAME","0").split("_")(1)+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(2)+"_"+iMoneyType+"_"+AddOrReduce+"_"+killoff33reason+"_"+
          r1.getOrElse("KEYNAME","0").split("_")(3)->zuheresult)

      }

    }
    mapphoenix
  }
}
