/**
  * Created by qjb on 16-8-29.
  */

import scala.io.{Codec, Source}
import java.io.{FileInputStream, FileOutputStream, ObjectInputStream, ObjectOutputStream}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.HashMap
import computeSimilar.computeSimilar
import akka.actor.ActorSystem
import akka.http.scaladsl.model._
import akka.http.scaladsl.Http
import akka.http.scaladsl.server.Directives._
import akka.http.scaladsl.server.Route
import akka.stream.ActorMaterializer
import java.util.NoSuchElementException

import scala.io.StdIn
import process.process

object Test {
  def main(args: Array[String]): Unit = {
    if(args.length < 1) {
      System.err.println("need a userId as parameter")
    }
    /*
    val reversedTable = new mutable.HashMap[Long,Set[String]]()
    val zhihuData = new ObjectInputStream(new FileInputStream("/home/qjb/zhihuData")).readObject().asInstanceOf[HashMap[String,(List[Long],List[Long],List[Long])]]
    zhihuData.map(userData => {
      userData._2._1.map(topicId => {
        if(reversedTable.contains(topicId)) reversedTable(topicId) += userData._1
        else  reversedTable += (topicId -> Set(userData._1))
      })
    })
    val oos = new ObjectOutputStream(new FileOutputStream("/home/qjb/reversedTable"))
    oos.writeObject(reversedTable)
    oos.close()
    */
    val jaccard = new computeSimilar()
    val zhihuSimplifyData = new ObjectInputStream(new FileInputStream("/home/qjb/zhihuData"))
      .readObject.asInstanceOf[HashMap[String,(List[Long],List[Long],List[Long])]]
    val reversedTable = new ObjectInputStream(new FileInputStream("/home/qjb/reversedTable"))
      .readObject.asInstanceOf[HashMap[Long,Set[String]]]

    println("read Object finished")
    def computeSimilarityUser(user: String): String = {
      println("computeSimilarityUser "+ user)
      var res = 0D
      var resUser: String = "0"
      var userNeedCompute = Set[String]()
      try {
        zhihuSimplifyData(user)._1.map(topicId => {
          userNeedCompute ++= reversedTable(topicId).toSet
        })
      } catch {
        case e: NoSuchElementException => println("no such user")
      }

      if(userNeedCompute.size == 0) "0"
      else  {
        userNeedCompute -= user
        userNeedCompute.foreach(userId => {
          if(!zhihuSimplifyData.contains(user)) resUser = "0"
          else {
            val result = jaccard.jaccardSimilarity(zhihuSimplifyData(user), zhihuSimplifyData(userId))
            if (result > res) {
              res = result
              resUser = userId
            }
      }
        })
        resUser
      }
    }

/*
    val processFile = new process()
    println("process Questions started")
    processFile.writeQuestionIdTitleTopics2File()
    println("process Questions finished")
*/
    implicit val system = ActorSystem()
    implicit val actorMater = ActorMaterializer()
    implicit val execuction = system.dispatcher

    val route = {
      get{
        pathPrefix("user" / Segment ) { id =>{
          val similarUser = computeSimilarityUser(id)
       //   val similarUser = System.currentTimeMillis() / 1000
          var response = "<h>"+ similarUser.toString +"</h>"
          if(similarUser == "0") {
            response = "<h>No such user</h>"
          }
          complete(HttpEntity(ContentTypes.`text/html(UTF-8)`,response))
        }}
      }
    }

    val bindindFutrue = Http().bindAndHandle(route,"localhost",8081)
    println(s"server online,enter any key to stop")
    StdIn.readLine()

    bindindFutrue.flatMap(_.unbind()).onComplete(_ => system.terminate())

  }
}
