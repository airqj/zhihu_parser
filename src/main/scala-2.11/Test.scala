/**
  * Created by qjb on 16-8-29.
  */

import scala.io.{Codec, Source}
import scala.collection.mutable.HashMap
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.native.JsonMethods._
import scalikejdbc._
import java.io.File
import java.io.PrintWriter
import java.io.FileInputStream
import java.io.FileOutputStream
import java.io.ObjectInputStream
import java.io.ObjectOutputStream
import java.io.Serializable

import com.sun.org.apache.xalan.internal.utils.XMLSecurityPropertyManager.Property
import org.postgresql.util.PGobject

import scala.collection.mutable

case class Answer(answer_id: Long,question_id: Long,question_title: String,question_topics: Array[Long],votup_count: Long)
case class Article(id: Long, title: String, votup_count: Long)
case class Question(question_followers: Long,question_id: Long,question_title: String,question_topics: Array[Long])

case class user(CREATED_ANSWERS: Array[Answer],
                CREATED_ARTICLES: Array[Article],
                CREATED_QUESTIONS: Array[Question],
                FOLLOWED_QUESTIONS: Array[Question],
                VOTEUPED_ANSWERS: Array[Answer],
                VOTEUPED_ARTICLES: Array[Article],
                followers_count: Long,
                id: String)

case class simplifyUserData(followedQuestion: Array[Long],
                            votupAnswers: Array[Long],
                            votupArticles: Array[Long])

object Test {
  def main(args: Array[String]): Unit = {

    val newDataPath = "/home/qjb/DATA/totalDataPath/"

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

    def whereIsData(zhihuId: String):(String,Long) = {
      val res =
        sql"""
              SELECT * FROM userMaxDataLength WHERE zhihuId = $zhihuId
          """.map(rs => (rs.string("topicname"),rs.long("linenumber"))).list().apply()

      if(res.length == 1) res(0) else ("0",0)
    }

    def processData() = {
      val currentDataDir = "/home/qjb/DATA/topics/"
      for (fileName <- new File(currentDataDir).listFiles) {
        val name = fileName.getName()
        //      var numberIndex = 1
        //      val newTopicName = new File(newDataPath+fileName.getName())
        val pw = new PrintWriter(newDataPath + fileName.getName())
        for (line <- Source.fromFile(fileName).getLines()) {
          /*
        val userDataLine = parse(line)
        val lineLength = line.length()
        val userId = (userDataLine \ "id").values.toString()
        if(userIsExist(userId)) {
          if(isLonger(userId,lineLength)) updateLineLength(userId,name,numberIndex,lineLength)
        }
        else insertUser(userId,name,numberIndex,lineLength)

        numberIndex += 1
        */
          val userId = (parse(line) \ "id").values.toString()
          val dataLocation = whereIsData(userId)
          if (dataLocation == ("0", 0)) throw new RuntimeException("user not found")
          val data = new File(currentDataDir + dataLocation._1)
          val dataLine = Source.fromFile(currentDataDir + dataLocation._1)
                          .getLines().drop((dataLocation._2 - 1).toInt).next()
          pw.write(dataLine + "\n")
          pw.flush()
          println("finished a line")
        }
        pw.close()
      }
    }
    implicit val farmat = DefaultFormats
    import scala.collection.mutable.ListBuffer

    case class zhihuSimplifyData(zhihuId: String, topicName: String, lineNumber: Long)
    val dataSerializable = new mutable.HashMap[String,String]()

    val allData: List[zhihuSimplifyData] = sql"""
                                                 SELECT * FROM userMaxDataLength
                                                 """.map(rs => zhihuSimplifyData(rs.string("zhihuid"),rs.string("topicname"),rs.long("linenumber"))).list().apply()

    val topicLines = allData.map(x => (x.topicName,x.lineNumber)).groupBy(_._1).map(x => (x._1,x._2.map(_._2)))

    topicLines.foreach( x => {
      val topicData = Source.fromFile("/home/qjb/DATA/totalDataPath/" + x._1).getLines().toArray
      x._2.map { lineNum => {
        val userData = parse(topicData(lineNum.toInt - 1)).extract[user]
        val userId = userData.id
        val followedQuestionsId = ListBuffer[Long]()
        val votupAnswers = ListBuffer[Long]()
        val votupArticles = ListBuffer[Long]()
        for (data <- userData.FOLLOWED_QUESTIONS) {
          followedQuestionsId.append(data.question_id)
        }
        for (data <- userData.VOTEUPED_ANSWERS) {
          if (!followedQuestionsId.contains(data.question_id)) {
            votupAnswers.append(data.answer_id)
          }
        }
        for (data <- userData.VOTEUPED_ARTICLES) {
          votupArticles.append(data.id)
        }
        val value = compact(render((followedQuestionsId.toList, votupAnswers.toList, votupArticles.toList).toString()))
        dataSerializable += (userId -> value)
      }
      }
/*
        val jsonObject = new PGobject()
        jsonObject.setType("jsonb")
        jsonObject.setValue(value)

        sql"""
            INSERT INTO zhihuSimplifyData VALUES($userId,$jsonObject)
        """.execute().apply()
        println("finish a user")
*/
    })
    val oos = new ObjectOutputStream(new FileOutputStream("/home/qjb/zhihuData"))
    oos.writeObject(dataSerializable)
    oos.close()
  }
}
