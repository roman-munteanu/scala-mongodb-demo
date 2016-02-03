package com.munteanu.reactivemongo

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
    val futureFindResult: Future[List[BSONDocument]] =
      collection.find(BSONDocument()).cursor[BSONDocument]().collect[List]()
//    printFutureCollection(futureFindResult, "Employee find")

    // find one
    val where = BSONDocument("username" -> "daniel.fowler")
    val futureEmployee = collection.find(where).one[BSONDocument]
    Await.result(futureEmployee, timeout)
//    futureEmployee.map(maybeEmployee =>
//      maybeEmployee match {
//        case Some(employee) => println(s"Find one: ${BSONDocument pretty employee}")
//        case _ => println("Employee NotFound")
//      }
//    )

    // sort and limit
    val limit = 2
    val futureSortAndLimitResult = collection.find(BSONDocument())
                                    .sort(BSONDocument("firstName" -> -1))
                                    .cursor[BSONDocument]()
                                    .collect[List](limit)
    Await.result(futureSortAndLimitResult, timeout)
//    printFutureCollection(futureEmployees, "Employee sort and limit")


    // skip and limit
    val queryBuilder = collection.find(BSONDocument())
    val queryOptions = QueryOpts().skip(2).batchSize(1)
    val futureSkipAndLimitResult = queryBuilder.options(queryOptions)
                                                .cursor[BSONDocument]()
                                                .collect[List]()
    Await.result(futureSkipAndLimitResult, timeout)
//    printFutureCollection(futureSkipAndLimitResult, "Employee skip and limit")


    // TODO Iterate a collection with a lot of items with a cursor



    // find using domain objects

    import com.munteanu.model._
    import com.munteanu.model.Employee._

    val futureFetchResult: Future[List[Employee]] =
      collection.find(BSONDocument()).cursor[Employee]().collect[List]()
    Await.result(futureFetchResult, timeout)
    printFutureDomainCollection(futureFetchResult, "Find with domain objects")

    ()
  }

  def printFutureCollection(futureCollection: Future[List[BSONDocument]], label: String): Unit = {
    futureCollection.map(docs =>
      docs.foreach(doc =>
        println(s"$label: ${BSONDocument pretty doc}")
      )
    )
  }

  def printFutureDomainCollection(futureCollection: Future[List[Any]], label: String): Unit = {
    futureCollection.map(items =>
      items.foreach(item =>
        println(s"$label: $item")
      )
    )
  }
}
