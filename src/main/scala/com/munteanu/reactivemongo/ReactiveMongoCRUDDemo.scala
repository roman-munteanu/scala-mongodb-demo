package com.munteanu.reactivemongo

import com.munteanu.model.Employee
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

    val futureDrop = collection.drop()
    Await.result(futureDrop, timeout)

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

    //
    // bulk insert
    // db.employees_crud.insert([{...}, {...}])
    //
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

    //
    // multiple update
    // db.employees_crud.update({"isActive": true}, {"$set": {"department": "Testing"}}, {"multi": true})
    //
    val futureMultipleUpdate =
      collection.update(selector = BSONDocument("isActive" -> true), update = BSONDocument("$set" -> BSONDocument("department" -> "Testing")), multi = true)
    Await.result(futureMultipleUpdate, timeout)
    futureMultipleUpdate.map(updateWriteResult => println("Multiple update operation succeeded: " + updateWriteResult.ok))

    //
    // update or insert
    // db.employees_crud.update({"username": "helen.smith"}, {"$set": {"username": "helen.smith", "department": "HR", "isActive": true}}, {"upsert": true})
    //
    val update = BSONDocument("$set" ->
      BSONDocument(
        "username" -> "helen.smith",
        "department" -> "HR",
        "isActive" -> true
      )
    )
    val futureUpsert = collection.update(BSONDocument("username" -> "helen.smith"), update, upsert = true)
    Await.result(futureUpsert, timeout)
    futureUpsert.map(updateWriteResult => println("Upsert operation succeeded: " + updateWriteResult.ok))

    //
    // remove
    // db.employees_crud.remove({"username": "alice.parker"})
    //
    val futureRemove = collection.remove(BSONDocument("username" -> "alice.parker"))
    Await.result(futureRemove, timeout)
    futureRemove.map(writeResult => println("Remove operation succeeded: " + writeResult.ok))

    //
    // Insert and update operations using domain objects
    //
    val martin = Employee("Martin", "Ford", "martin.ford", "1234abcd", "Project Management", false, Nil, List("Scala"))

    // insert
    val futureInsertDomain = collection.insert(martin)
    Await.result(futureInsertDomain, timeout)

    // update
    val futureMartin = collection.find(BSONDocument("username" -> "martin.ford")).one[Employee]
    Await.result(futureMartin, timeout)

    futureMartin.map {
      case Some(ford) =>
        println(ford.toString)
        collection.update(ford, BSONDocument("$set" -> BSONDocument("department" -> "Development", "isActive" -> true)))
          .map(updateWriteResult => println("Update domain operation succeeded: " + updateWriteResult.ok))
      case _ =>
        println("Martin was not found in the database :(")
    }

    ()
  }
}
