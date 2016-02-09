package com.munteanu.reactivemongo

import play.api.libs.iteratee.{Iteratee, Enumerator}
import reactivemongo.api._
import reactivemongo.api.indexes.{IndexType, Index}
import reactivemongo.bson.BSONDocument
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

import scala.util.{ Failure, Success }

/**
  * Created by romunteanu on 1/29/2016.
  */
object ReactiveMongoFetchDemo {
  def main(args: Array[String]) = {

    val driver = new MongoDriver
//    val connection = driver.connection(List("localhost"))
    val connection: MongoConnection = driver.connection(List("127.0.0.1:27017"))

    val db: DefaultDB = connection("reactivemongo")

    val collection: BSONCollection = db("employees")

    val timeout = 5.seconds

    //
    // drop collection
    // db.employees.drop()
    //
    val futureDrop = collection.drop()
    Await.result(futureDrop, timeout)

    //
    // insert operation
    // db.employees.insert({...})
    //
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
    // db.employees.insert([{...}, {...}])
    //
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

    //
    // create index
    // db.employees.createIndex({"username": 1});
    //
    val futureCreateIndex = collection.indexesManager.create(Index(key = Seq(("username", IndexType.Ascending))))
    Await.result(futureCreateIndex, timeout)

    //
    // find
    // db.employees.find()
    //
    val futureFindResult: Future[List[BSONDocument]] =
      collection.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
    printFutureCollection(futureFindResult, "Employee find:")

    //
    // find one
    // db.employees.findOne({"username": "daniel.fowler"})
    //
    val where = BSONDocument("username" -> "daniel.fowler")
    val futureEmployee = collection.find(where).one[BSONDocument]
    Await.result(futureEmployee, timeout)
    futureEmployee.map {
        case Some(employee) => println(s"Find one: ${BSONDocument pretty employee}")
        case _ => println("Employee NotFound")
      }

    //
    // sort and limit
    // db.employees.find().sort({"firstName": -1}).limit(2)
    //
    val limit = 2
    val futureSortAndLimitResult = collection.find(BSONDocument())
                                    .sort(BSONDocument("firstName" -> -1))
                                    .cursor[BSONDocument]()
                                    .collect[List](limit)
    Await.result(futureSortAndLimitResult, timeout)
    printFutureCollection(futureSortAndLimitResult, "Employees sort and limit:")

    //
    // skip and limit
    // db.employees.find().skip(2).limit(1)
    //
    val queryBuilder = collection.find(BSONDocument())
    val queryOptions = QueryOpts().skip(2).batchSize(1)
    val futureSkipAndLimitResult = queryBuilder.options(queryOptions)
                                                .cursor[BSONDocument]()
                                                .collect[List]()
    Await.result(futureSkipAndLimitResult, timeout)
    printFutureCollection(futureSkipAndLimitResult, "Employees skip and limit:")

    //
    // find using domain objects
    //
    import com.munteanu.model._
    import com.munteanu.model.Employee._

    val futureFetchResult: Future[List[Employee]] =
      collection.find(BSONDocument()).cursor[Employee]().collect[List]()
    Await.result(futureFetchResult, timeout)
    printFutureCollection(futureFetchResult, "Find result with domain objects")

    //
    // count method with a query
    // db.employees.count({"department": "Development"})
    //
    val countFutureResult: Future[Int] = collection.count(Some(BSONDocument("department" -> "Development")))
    Await.result(countFutureResult, timeout)
    countFutureResult.map(res => println(s"Count result: $res"))

    //
    // count command with a query
    // db.employees.count({"isActive": true})
    //
    // BSON implementation of the count command
    import reactivemongo.api.commands.bson.BSONCountCommand.{ Count, CountResult }
    // BSON serialization-deserialization for the count arguments and result
    import reactivemongo.api.commands.bson.BSONCountCommandImplicits._

    val query = BSONDocument("isActive" -> true)
    val command = Count(query)
    val futureCommandCountResult: Future[CountResult] = collection.runCommand(command)
    Await.result(futureCommandCountResult, timeout)

    futureCommandCountResult.map { res =>
      println(s"Count command: ${res.value}")
    }

    //
    // Consume streams of documents using Play Iteratee
    //
    val enumeratorOfEmployees: Enumerator[BSONDocument] =
      collection.find(BSONDocument()).cursor[BSONDocument]().enumerate()

    val processDocuments: Iteratee[BSONDocument, Unit] =
      Iteratee.foreach { employee =>
        // may contain some logic of processing
        println(BSONDocument.pretty(employee))
      }

    println("Process the stream of employees:")
    enumeratorOfEmployees.run(processDocuments)


    ()
  }
}
