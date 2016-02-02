package com.munteanu.reactivemongo

import reactivemongo.api._
import reactivemongo.api.indexes.{IndexType, Index}
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
//    collection.drop()

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
        "lastName" -> "Gordon",
        "username" -> "lisa.gordon",
        "password" -> "2b3c",
        "department" -> "Project Management",
        "isActive" -> true
      )
    )
    Await.result(bulkInsertResult, 10.seconds)

    // create index
    val futureCreateIndex = collection.indexesManager.create(Index(key = Seq(("username", IndexType.Ascending))))
    Await.result(futureCreateIndex, timeout)


    // find
    val futureEmployees: Future[List[BSONDocument]] =
      collection.find(BSONDocument()).cursor[BSONDocument]().collect[List]()

    futureEmployees.map(employees =>
      employees.foreach(doc =>
        println(s"Employee: ${BSONDocument pretty doc}")
      )
    )

    // find one
    val where = BSONDocument("username" -> "daniel.fowler")
    val futureEmployee = collection.find(where).one[BSONDocument]
    Await.result(futureEmployee, timeout)
    futureEmployee.map(maybeEmployee =>
      maybeEmployee match {
        case Some(employee) => println(s"Fetch one: ${BSONDocument pretty employee}")
        case _ => println("Employee NotFound")
      }
    )

    // TODO skip
    val limit = 2
    val futureDataSortedLimited = collection.find(BSONDocument())
                                    .sort(BSONDocument("firstName" -> -1))
                                    .cursor[BSONDocument]()
                                    .collect[List](limit)
    Await.result(futureDataSortedLimited, timeout)
    futureDataSortedLimited.map(employees =>
      employees.foreach(doc =>
        println(s"Employee Sorted: ${BSONDocument pretty doc}")
      )
    )


    // remove
    val futureRemove = collection.remove(BSONDocument("username" -> "alice.parker"))
    Await.result(futureRemove, timeout)
    futureRemove.map(lastError => println("Remove operation succeeded: " + lastError.ok))


    // multiple update
    val futureMultipleUpdate =
      collection.update(selector = BSONDocument("isActive" -> true), update = BSONDocument("$set" -> BSONDocument("department" -> "Testing")), multi = true)
    Await.result(futureMultipleUpdate, timeout)
    futureMultipleUpdate.map(lastError => println("Multiple update operation succeeded: " + lastError.ok))


    // update or insert
    val update = BSONDocument("$set" ->
      BSONDocument(
        "username" -> "helen.smith",
        "department" -> "HR",
        "isActive" -> true
      )
    )
    val futureUpsert = collection.update(BSONDocument("username" -> "helen.smith"), update, upsert = true)
    Await.result(futureUpsert, timeout)
    futureUpsert.map(lastError => println("Upsert operation succeeded: " + lastError.ok))

    ()
  }
}
