package com.munteanu.reactivemongo

import reactivemongo.api._
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{WriteResult, MultiBulkWriteResult}
import reactivemongo.bson.BSONDocument

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by romunteanu on 2/3/2016.
  */
object ReactiveMongoCRUDDemo {
  def main(args: Array[String]) = {

    val driver = new MongoDriver
    val connection = driver.connection(List("127.0.0.1:27017"))
    val db = connection("reactivemongo")
    val collection: BSONCollection = db("employees_crud")

    val timeout = 5.seconds

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
    // db.employees_crud.insert([{...}, {...}])
    val bulkInsertResult: Future[MultiBulkWriteResult] = collection.bulkInsert(ordered = false)(
//      BSONDocument(
//        "firstName" -> "Alice",
//        "lastName" -> "Parker",
//        "username" -> "alice.parker",
//        "password" -> "123abc",
//        "department" -> "Development",
//        "isActive" -> true
//      ),
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
      ),
      BSONDocument(
        "firstName" -> "Mark",
        "lastName" -> "Lawrence",
        "username" -> "mark.lawrence",
        "password" -> "444ddd",
        "department" -> "Facilities",
        "isActive" -> true
      )
    )
    Await.result(bulkInsertResult, 10.seconds)


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


    // remove
    val futureRemove = collection.remove(BSONDocument("username" -> "alice.parker"))
    Await.result(futureRemove, timeout)
    futureRemove.map(lastError => println("Remove operation succeeded: " + lastError.ok))

    // TODO CRUD operations using domain objects


    ()
  }
}
