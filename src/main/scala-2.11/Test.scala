/**
  * Created by qjb on 16-8-29.
  */

import scala.io.{Codec, Source}
import org.json4s._
import org.json4s.native.JsonMethods._
import scalikejdbc._
import java.io.File

object Test {
  def main(args: Array[String]): Unit = {
    case class userData(zhihuId: String, topic: String, lineNumber: Long)
    Class.forName("org.postgresql.Driver")
    ConnectionPool.singleton("jdbc:postgresql://localhost:5432/zhihudb", "dbuser", "admin")

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

    for (fileName <- new File("/home/qjb/DATA/topics1").listFiles) {
      val name = fileName.getName()
      var numberIndex = 1
      for (line <- Source.fromFile(fileName).getLines()) {
        val userDataLine = parse(line)
        val lineLength = line.length()
        val userId = (userDataLine \ "id").values.toString()
        if(userIsExist(userId)) {
          if(isLonger(userId,lineLength)) updateLineLength(userId,name,numberIndex,lineLength)
        }
        else insertUser(userId,name,numberIndex,lineLength)

        numberIndex += 1
      }

    }
  }
}
