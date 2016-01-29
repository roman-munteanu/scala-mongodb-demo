package com.munteanu.reactivemongo

import reactivemongo.api._
import reactivemongo.bson.BSONDocument
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}

import scala.concurrent.Future
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.Await

import scala.util.{ Failure, Success }

/**
  * Created by romunteanu on 1/29/2016.
  */
object ReactiveMongoDemo {
  def main(args: Array[String]) = {

    val driver = new MongoDriver
//    val connection = driver.connection(List("localhost"))
    val connection = driver.connection(List("127.0.0.1:27017"))

    val db = connection("reactivemongo")

    val collection: BSONCollection = db("employees")

    val timeout = 5.seconds

    // drop collection
    collection.drop()

    // insert operation
    val alice = BSONDocument(
      "firstName" -> "Alice",
      "lastName" -> "Parker",
      "username" -> "alice.parker",
      "password" -> "123abc",
      "department" -> "Development",
      "isActive" -> true
    )

    val insertResult: Future[WriteResult] = collection.insert(alice)
    Await.result(insertResult, timeout)


    // bulk insert
    val bulkInsertResult: Future[MultiBulkWriteResult] = collection.bulkInsert(ordered = false)(
      BSONDocument(
        "firstName" -> "Daniel",
        "lastName" -> "Fowler",
        "username" -> "daniel.fowler",
        "password" -> "1a2b",
        "department" -> "Development",
        "isActive" -> false
      ),
      BSONDocument(
        "firstName" -> "Lisa",
        "lastName" -> "Hunter",
        "username" -> "lisa.hunter",
        "password" -> "2b3c",
        "department" -> "Project Management",
        "isActive" -> true
      )
    )
    Await.result(bulkInsertResult, 10.seconds)

    // find
    val futureEmployees: Future[List[BSONDocument]] =
      collection.find(BSONDocument()).cursor[BSONDocument]().collect[List]()

    futureEmployees.map(employess =>
      employess.foreach(doc =>
        println(s"Employee: ${BSONDocument pretty doc}")
      )
    )

    ()
  }
}
