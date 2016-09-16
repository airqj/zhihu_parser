package sql

/**
  * Created by qjb on 16-9-16.
  */

import scalikejdbc._

case class zhihuSimplifyData(zhihuId: String, topicName: String, lineNumber: Long)

class sqlCmd {
  Class.forName("org.postgresql.Driver")
  ConnectionPool.singleton("jdbc:postgresql://localhost:5432/zhihudb","dbuser","admin")
  implicit val session = AutoSession
  def userIsExist(userId: String): Boolean = {
    val res = sql"""
                      SELECT * FROM userMaxDataLength WHERE zhihuid = $userId
                      """.map(rs => rs.string("zhihuid")).list().apply()
    if(res.length == 1) true else false
  }

  def isLonger(userId: String,lineLength: Long): Boolean = {
    val res = sql"""
                      SELECT * FROM userMaxDataLength WHERE zhihuid = $userId and linelength < $lineLength
                   """.map(rs => rs.string("zhihuid")).list().apply()
    if(res.length == 1) true else false
  }

  def insertUser(userId: String,name: String,numberIndex: Long,lineLength: Long) = {
    sql"""
            INSERT INTO userMaxDataLength VALUES($userId,$name,$numberIndex,$lineLength)
            """.execute().apply()
  }

  def updateLineLength(userId: String,name: String,numberIndex: Long,lineLength: Long): Unit = {
    sql"""
            UPDATE userMaxDataLength SET topicName = $name , lineNumber = $numberIndex, lineLength = $lineLength
            WHERE zhihuid = $userId
            """.execute().apply()
  }

  def whereIsData(zhihuId: String):(String,Long) = {
    val res =
      sql"""
              SELECT * FROM userMaxDataLength WHERE zhihuId = $zhihuId
          """.map(rs => (rs.string("topicname"),rs.long("linenumber"))).list().apply()

    if(res.length == 1) res(0) else ("0",0)
  }

  def getAllDataFromUserMaxDataLength() = {
      sql"""
         SELECT * FROM userMaxDataLength
      """.map(rs => zhihuSimplifyData(rs.string("zhihuid"),rs.string("topicname"),rs.long("linenumber"))).list().apply()
  }
}
