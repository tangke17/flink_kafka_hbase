package phoenixHbase

import java.sql._

import org.apache.hadoop.hbase.client.{Connection => _}
import org.json.JSONException

import scala.Array
import scala.collection.mutable._
import scala.collection.{immutable, mutable}

class PhoenixClient_v2(host:String, port:String) {
  private val url = "jdbc:phoenix:" + host + ":" + port
  private var conn: Connection = DriverManager.getConnection(url)
  private var ps: PreparedStatement = null
  private var stmt:Statement=null
  try{
    Class.forName("org.apache.phoenix.jdbc.PhoenixDriver")
  }
  catch{
    case e :ClassNotFoundException => println(e)
  }
  def Select_Sql(phoenixSQL: String):ArrayBuffer[immutable.Map[String,String]] = {
    if (phoenixSQL == null || (phoenixSQL.trim == "" )) println("PhoenixSQL is needed")
    //存放列名
    val cols = new ArrayBuffer[String]
    //定义一个数组缓冲，存放每一行的列名及值组成的映射
    val All_Res = new ArrayBuffer[immutable.Map[String,String]]()

    stmt = conn.createStatement()
    try {
      //      println("Select_Sql:"+phoenixSQL)
      val set = stmt.executeQuery(phoenixSQL)
      //      println("set = "+set.toString)
      val meta: ResultSetMetaData = set.getMetaData //获得ResultSetMeataData对象
      //      println("meta = "+meta.toString)
      //遍历set
      var KvMap =  new HashMap[String,String]
      //      var flag=false
      while (set.next()) {
        //如果存放列名的数组为空，则取列名
        if (cols.length != meta.getColumnCount) {
          for( i <- 1.to(meta.getColumnCount)){ cols += meta.getColumnLabel(i)}
        }
        cols.foreach(x=> KvMap(x) = set.getString(x) )
        //将每个映射添加到数组缓冲中
        All_Res += KvMap.toMap
      }
    }
    catch {
      case e: Exception => e.printStackTrace();println("SQL exec error:" + e.getMessage)
      case e: JSONException => e.printStackTrace();println("JSON error:" + e.getMessage)
    }
    finally {
      conn.close()
      stmt.close()
    }
    All_Res
  }
  def Update_Insert_Delete(phoenixSQL: String) = {
    var res = 0
    if (phoenixSQL == null || (phoenixSQL.trim ==  "")) {
      System.out.println("PhoenixSQL is needed")
      res = -1
    }else {
      //val url = "jdbc:phoenix:" + host + ":" + port
      //val conn = DriverManager.getConnection(url)
      stmt = conn.createStatement()
      try {
        res = stmt.executeUpdate(phoenixSQL)
        conn.commit()
      }
      catch {
        case e: SQLException => println(phoenixSQL.trim.split(" ")(0)+" Except:" + e.toString)
      }
      finally {
        conn.close()
        stmt.close()
      }
    }
    res
  }
  def Update_Insert_Flinkdata(resultmap:mutable.Map[String,String],tablename:String): Unit ={
    println(resultmap )
    resultmap.map(x=>{
          println(x._1,x._2)
          var (keyname1,time)=("","1970-01-01")
          var (hour,serverid,publish31,publish31reason,consume31,consume31reason,killoff31,killoff31reason,balance31)=(0,0,0,0,0,0,0,0,0)
          var (publish32,publish32reason,consume32,consume32reason,killoff32,killoff32reason,balance32)=(0,0,0,0,0,0,0)
          var (publish33,publish33reason,consume33,consume33reason,killoff33,killoff33reason,balance33)=(0,0,0,0,0,0,0)
           keyname1=x._1.split("_")(0)+"_"+x._1.split("_")(1)+"_"+x._1.split("_")(2)+"_"+x._1.split("_")(6)
      time=x._1.split("_")(6).split("\\|")(0)
      serverid=x._1.split("_")(1).toInt
      hour=x._1.split("\\$")(1).split("\\|")(0).toInt
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="31" && x._1.split("_")(4)=="0"){
               publish31=x._2.split("_")(0).toInt
        balance31=x._2.split("_")(1).toInt
        publish31reason=x._1.split("_")(5).toInt
               Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,publish31,balance31,publish31reason) " +
                 "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+publish31+","+balance31+","+publish31reason+")")
          }
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="31" && x._1.split("_")(4)=="1" && x._1.split("_")(5)!="729"){
        consume31=x._2.split("_")(0).toInt
        balance31=x._2.split("_")(1).toInt
        consume31reason=x._1.split("_")(5).toInt
              Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,consume31,balance31,consume31reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+consume31+","+balance31+","+consume31reason+")")
        }
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="31" && x._1.split("_")(4)=="1" && x._1.split("_")(5)=="729"){
        killoff31=x._2.split("_")(0).toInt
        balance31=x._2.split("_")(1).toInt
        killoff31reason=x._1.split("_")(5).toInt
              Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,killoff31,balance31,killoff31reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+killoff31+","+balance31+","+killoff31reason+")")
      }
      //role
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="31" && x._1.split("_")(4)=="0"){
        publish31=x._2.split("_")(0).toInt
        balance31=x._2.split("_")(1).toInt
        publish31reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,publish31,balance31,publish31reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+publish31+","+balance31+","+publish31reason+")")
      }
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="31" && x._1.split("_")(4)=="1" && x._1.split("_")(5)!="729"){
        consume31=x._2.split("_")(0).toInt
        balance31=x._2.split("_")(1).toInt
        consume31reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,consume31,balance31,consume31reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+consume31+","+balance31+","+consume31reason+")")
      }
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="31" && x._1.split("_")(4)=="1" && x._1.split("_")(5)=="729"){
        killoff31=x._2.split("_")(0).toInt
        balance31=x._2.split("_")(1).toInt
        killoff31reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,killoff31,balance31,killoff31reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+killoff31+","+balance31+","+killoff31reason+")")
      }
      //有料符石32
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="32" && x._1.split("_")(4)=="0"){
        publish32=x._2.split("_")(0).toInt
        balance32=x._2.split("_")(1).toInt
        publish32reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,publish32,balance32,publish32reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+publish32+","+balance32+","+publish32reason+")")
      }
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="32" && x._1.split("_")(4)=="1" && x._1.split("_")(5)!="729"){
        consume32=x._2.split("_")(0).toInt
        balance32=x._2.split("_")(1).toInt
        consume32reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,consume32,balance32,consume32reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+consume32+","+balance32+","+consume32reason+")")
      }
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="32" && x._1.split("_")(4)=="1" && x._1.split("_")(5)=="729"){
        killoff32=x._2.split("_")(0).toInt
        balance32=x._2.split("_")(1).toInt
        killoff32reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,killoff32,balance32,killoff32reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+killoff32+","+balance32+","+killoff32reason+")")
      }
      //role
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="32" && x._1.split("_")(4)=="0"){
        publish32=x._2.split("_")(0).toInt
        balance32=x._2.split("_")(1).toInt
        publish32reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,publish32,balance32,publish32reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+publish32+","+balance32+","+publish32reason+")")
      }
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="32" && x._1.split("_")(4)=="1" && x._1.split("_")(5)!="729"){
        consume32=x._2.split("_")(0).toInt
        balance32=x._2.split("_")(1).toInt
        consume32reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,consume32,balance32,consume32reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+consume32+","+balance32+","+consume32reason+")")
      }
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="32" && x._1.split("_")(4)=="1" && x._1.split("_")(5)=="729"){
        killoff32=x._2.split("_")(0).toInt
        balance32=x._2.split("_")(1).toInt
        killoff32reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,killoff32,balance32,killoff32reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+killoff32+","+balance32+","+killoff32reason+")")
      }
      //有料符石33
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="33" && x._1.split("_")(4)=="0"){
        publish33=x._2.split("_")(0).toInt
        balance33=x._2.split("_")(1).toInt
        publish33reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,publish33,balance33,publish33reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+publish33+","+balance33+","+publish33reason+")")
      }
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="33" && x._1.split("_")(4)=="1" && x._1.split("_")(5)!="729"){
        consume33=x._2.split("_")(0).toInt
        balance33=x._2.split("_")(1).toInt
        consume33reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,consume33,balance33,consume33reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+consume33+","+balance33+","+consume33reason+")")
      }
      if(x._1.split("\\|")(0)=="acc" && x._1.split("_")(3)=="33" && x._1.split("_")(4)=="1" && x._1.split("_")(5)=="729"){
        killoff33=x._2.split("_")(0).toInt
        balance33=x._2.split("_")(1).toInt
        killoff33reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,killoff33,balance33,killoff33reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+killoff33+","+balance33+","+killoff33reason+")")
      }
      //role
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="33" && x._1.split("_")(4)=="0"){
        publish33=x._2.split("_")(0).toInt
        balance33=x._2.split("_")(1).toInt
        publish33reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,publish33,balance33,publish33reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+publish33+","+balance33+","+publish33reason+")")
      }
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="33" && x._1.split("_")(4)=="1" && x._1.split("_")(5)!="729"){
        consume33=x._2.split("_")(0).toInt
        balance33=x._2.split("_")(1).toInt
        consume33reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,consume33,balance33,consume33reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+consume33+","+balance33+","+consume33reason+")")
      }
      if(x._1.split("\\|")(0)=="roleid" && x._1.split("_")(3)=="33" && x._1.split("_")(4)=="1" && x._1.split("_")(5)=="729"){
        killoff33=x._2.split("_")(0).toInt
        balance33=x._2.split("_")(1).toInt
        killoff33reason=x._1.split("_")(5).toInt
        Update_Insert_Delete("upsert into FLINK_REALTIME_MONEY_JP(keyname,time,hour,serverid,killoff33,balance33,killoff33reason) " +
          "values('"+keyname1+"','"+time+"',"+hour+","+serverid+","+killoff33+","+balance33+","+killoff33reason+")")
     }
    })

  }
  def CreateTable(phoenixSQL: String) = {
    var res = 0
    //System.out.println("phoenixSQL:" + phoenixSQL)
    val tablename: String = phoenixSQL.split(" ")(2).trim
    if (phoenixSQL == null || (phoenixSQL.trim eq "")) {
      System.out.println("PhoenixSQL is needed")
      res = -1
    }else {
      //      val url = "jdbc:phoenix:" + host + ":" + port
      //      val conn = DriverManager.getConnection(url)
      stmt = conn.createStatement()
      try {
        val tab_cnt = stmt.executeQuery("select count(DISTINCT TABLE_NAME) as \"cnt\"from SYSTEM.CATALOG where table_name='" + tablename + "'")

        while(tab_cnt.next()) {
          if (tab_cnt.getInt("cnt") == 0) {
            println("executing create table...")
            res = stmt.executeUpdate(phoenixSQL)
          }
          else
            println(tablename + " is exists!")
        }
      }
      catch {
        case e: SQLException => println("create table except:" + e.getMessage)
      }
      finally {
        conn.close()
        stmt.close()
      }
    }
    res
  }
  def GetUpsertSql(ColumnArr:Array[String],tablename:String) = {
    //拼出插入语句
    var UpsertSql = "upsert into \""+tablename+"\"(\"pk\""
    ColumnArr.foreach(x=>UpsertSql += ",\""+x+"\"")
    UpsertSql += ") values(?"
    ColumnArr.foreach(x=> UpsertSql += ",?")
    UpsertSql += ")"
    println("UpsertSql"+UpsertSql)
    UpsertSql
  }

  //获取create语句
  def GetCreateSql(ColumnArr:Array[String],tablename:String) = {
    //拼出建表语句
    var CreateSql = "create table if not exists \""+tablename+"\"(\"pk\" varchar(100) not null primary key"
    for(col <- ColumnArr){
      CreateSql += ",\"cf1\".\""+col+"\" varchar(50)"
    }
    //这样的话，0|开头的startkey是0| endkey 是100|， 所以分区为SPLIT ON ('10|','20|','30|','40|','50|','60|','70|','80|','90|')"
    //CreateSql += ") SPLIT ON ('0|','10|','20|','30|','40|','50|','60|','70|','80|','90|','100|')"
    CreateSql += ") SPLIT ON ('10|','20|','30|','40|','50|','60|','70|','80|','90|')"
    println("CreateSql = "+CreateSql)
    CreateSql
  }

}
