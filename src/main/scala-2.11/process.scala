package process

import java.io.{File, FileOutputStream, ObjectOutputStream, PrintWriter}

import org.json4s.DefaultFormats
import org.json4s.native.JsonMethods._
import sql.zhihuSimplifyData

import scala.collection.mutable
import scala.io.Source
import sql.sqlCmd


case class userData(zhihuId: String, topic: String, lineNumber: Long)

class process {

  implicit val format = DefaultFormats

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


  def getQuestionIdTiTleTopic():mutable.HashMap[Long,(String,Array[Long])] = {
    val res = mutable.HashMap[Long,(String,Array[Long])]()
    val path = "/home/qjb/DATA/totalDataPath"
    for(file <- new File(path).listFiles()) {
      for(line <- Source.fromFile(file).getLines()) {
        val userData = parse(line).extract[user]
        for(answer <- userData.CREATED_ANSWERS) {
          if(!res.contains(answer.question_id)) {
            res(answer.question_id) = (answer.question_title,answer.question_topics)
          }
        }
        for(createdQuestion <- userData.CREATED_QUESTIONS) {
          if(!res.contains(createdQuestion.question_id)) {
            res(createdQuestion.question_id) = (createdQuestion.question_title,createdQuestion.question_topics)
          }
        }
        for(followedQuestion <- userData.FOLLOWED_QUESTIONS) {
          if(!res.contains(followedQuestion.question_id)) {
            res(followedQuestion.question_id) = (followedQuestion.question_title,followedQuestion.question_topics)
          }
        }
        for(votupAnswer <- userData.VOTEUPED_ANSWERS) {
          if(!res.contains(votupAnswer.question_id)) {
            res(votupAnswer.question_id) = (votupAnswer.question_title,votupAnswer.question_topics)
          }
        }
      }
    }
  res
  }

  def writeQuestionIdTitleTopics2File() = {
    val oos = new ObjectOutputStream(new FileOutputStream("/home/qjb/questionIdTitleTopic"))
    val data = getQuestionIdTiTleTopic()
    oos.writeObject(data)
    oos.close()
  }

  def processData() = {
    val sqlCmdInstant = new sqlCmd()
    val newDataPath = "/home/qjb/DATA/totalDataPath/"
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
        val dataLocation = sqlCmdInstant.whereIsData(userId)
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
  def seliarizedData2File() = {
    val sqlCmdInstant = new sqlCmd()
    import scala.collection.mutable.ListBuffer


    val dataSerializable = new mutable.HashMap[String, (List[Long], List[Long], List[Long])]()

    val allData: List[zhihuSimplifyData] = sqlCmdInstant.getAllDataFromUserMaxDataLength()
    val topicLines = allData.map(x => (x.topicName, x.lineNumber)).groupBy(_._1).map(x => (x._1, x._2.map(_._2)))

    topicLines.foreach(x => {
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
        val value = (followedQuestionsId.toList, votupAnswers.toList, votupArticles.toList)
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
