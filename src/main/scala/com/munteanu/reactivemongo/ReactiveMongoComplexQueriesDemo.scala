package com.munteanu.reactivemongo

import com.munteanu.model.{Assignment, Employee}
import org.joda.time.DateTime
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.{MultiBulkWriteResult, WriteResult}
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}
import reactivemongo.bson.{BSONDateTime, BSONString, BSONDocument}

import scala.concurrent.{Future, Await}
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._

/**
  * Created by romunteanu on 2/4/2016.
  */
object ReactiveMongoComplexQueriesDemo {
  def main(args: Array[String]) = {

    val driver = new MongoDriver
    val connection: MongoConnection = driver.connection(List("127.0.0.1:27017"))

    val db: DefaultDB = connection("reactivemongo")
    val collection: BSONCollection = db("assignments")

    val timeout = 5.seconds

    val futureDrop = collection.drop()
    Await.result(futureDrop, timeout)

    // assignments
    val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

    val date2013JAN01 = formatter.parseDateTime("2013-01-01T00:00:00.000Z")
    val date2013DEC31 = formatter.parseDateTime("2013-12-31T00:00:00.000Z")
    val date2014JAN01 = formatter.parseDateTime("2014-01-01T00:00:00.000Z")
    val date2014DEC31 = formatter.parseDateTime("2014-12-31T00:00:00.000Z")
    val date2015JAN01 = formatter.parseDateTime("2015-01-01T00:00:00.000Z")
    val date2015DEC31 = formatter.parseDateTime("2015-12-31T00:00:00.000Z")
    val date2016JAN01 = formatter.parseDateTime("2016-01-01T00:00:00.000Z")

    val edward = Employee("Edward", "Adams", "edward.adams", "11aa22bb", "Development", true,
      List(
        Assignment("pc111", date2013JAN01, date2013DEC31, 75),
        Assignment("pc222", date2014JAN01, date2016JAN01, 100)
      ),
      List("Scala", "Java")
    )

    val insertResult: Future[WriteResult] = collection.insert(edward)
    Await.result(insertResult, timeout)

    val sophie = Employee("Sophie", "Newman", "sophie.newman", "321321", "Testing", true,
      List(
        Assignment("pc111", date2014JAN01, date2016JAN01, 100)
      ),
      List("Ruby", "Python")
    )

    val martin = Employee("Martin", "Ford", "martin.ford", "65465456", "Development", true,
      List(
        Assignment("pc222", date2015JAN01, date2015DEC31, 100)
      ),
      List("Groovy", "Java", "Scala")
    )

    val harry = Employee("Harry", "Thomson", "harry.thomson", "98798798", "Testing", true,
      List(
        Assignment("pc333", date2014DEC31, date2016JAN01, 100)
      ),
      List("Ruby", "Go")
    )

    val sam = Employee("Sam", "Roberts", "sam.roberts", "asdasd654", "Project Management", false,
      List(
        Assignment("pc444", date2015JAN01, date2016JAN01, 50)
      ),
      List("Agile")
    )

    val bulkInsertResult: Future[MultiBulkWriteResult] =
      collection.bulkInsert(ordered = false)(sophie, martin, harry, sam)
    Await.result(bulkInsertResult, 10.seconds)

    // an example of grouping all active employees by technologies
    /*
    db.assignments.aggregate([
      {"$match": {
          "isActive": true
        }
      },
      {"$unwind": "$tags"},
      {"$group": {
          "_id": "$tags",
          "employees": {"$push": "$username"}
        }
      },
      {"$project": {
          "_id": 0,
          "language": "$_id",
          "employees": 1
        }
      },
      {"$sort": {
          "language": 1
        }
      }
    ]);
    */

    val groupByTechnologiesResult = groupByTechnologies(collection)
    Await.result(groupByTechnologiesResult, timeout)
    printFutureCollection(groupByTechnologiesResult)


    // filter assignments by a date range
    // unwind phase should go first
    /*
    db.assignments.aggregate([
      {"$unwind": "$assignments"},
      {"$match": {
          "assignments.dateFrom": {"$lte": ISODate("2013-12-31T00:00:00.000Z")},
          "assignments.dateTo": {"$gte": ISODate("2013-01-01T00:00:00.000Z")},
          "tags": "Scala",
          "isActive": true
        }
      },
      {"$group": {
          "_id": {
            "username": "$username",
            "firstName": "$firstName",
            "lastName": "$lastName",
            "department": "$department",
            "tags": "$tags",
          },
          "assignments": {"$addToSet": "$assignments"}
        }
      },
      {"$project": {
          "_id": 0,
          "username": "$_id.username",
          "firstName": "$_id.firstName",
          "lastName": "$_id.lastName",
          "department": "$_id.department",
          "tags": "$_id.tags",
          "assignments": 1
        }
      }
    ]).pretty();
    */

    val filterAssignmentsResult = filterAssignments(collection, date2013JAN01, date2013DEC31)
    Await.result(filterAssignmentsResult, timeout)
    printFutureCollection(filterAssignmentsResult)

    ()
  }

  /**
    * Groups by technologies
    */
  def groupByTechnologies(collection: BSONCollection): Future[List[BSONDocument]] = {
    import collection.BatchCommands.AggregationFramework.{Ascending, Group, Match, Project, Sort, Push, Unwind}

    val phaseMatch = Match(BSONDocument("isActive" -> true))

    val phaseUnwind = Unwind("tags")

    val phaseGroup = Group(BSONString("$tags"))( "employees" -> Push("username"))

    val phaseProject = Project(BSONDocument("_id" -> 0, "language" -> "$_id", "employees" -> 1))

    val phaseSort = Sort(Ascending("language"))

    collection.aggregate(phaseMatch, List(phaseUnwind, phaseGroup, phaseProject, phaseSort)).map(_.documents)
  }

  /**
    * Filters assignments by a date range
    */
  def filterAssignments(collection: BSONCollection, startDate: DateTime, endDate: DateTime): Future[List[Employee]] = {
    import collection.BatchCommands.AggregationFramework.{AddToSet, Ascending, Group, Match, Project, Sort, Push, Unwind}

    val phaseUnwind = Unwind("assignments")

    val phaseMatch = Match(
      BSONDocument(
        "assignments.dateFrom" -> BSONDocument("$lte" -> BSONDateTime(endDate.getMillis)),
        "assignments.dateTo"   -> BSONDocument("$gte" -> BSONDateTime(startDate.getMillis)),
        "tags" -> "Scala",
        "isActive" -> true
      )
    )

    val phaseGroup = Group(
      BSONDocument(
        "username"   -> "$username",
        "firstName"  -> "$firstName",
        "lastName"   -> "$lastName",
        "department" -> "$department",
        "tags" -> "$tags"
      )
    )("assignments" -> AddToSet("assignments"))


    val phaseProject = Project(
      BSONDocument(
        "_id" -> 0,
        "username" -> "$_id.username",
        "firstName" -> "$_id.firstName",
        "lastName" -> "$_id.lastName",
        "department" -> "$_id.department",
        "tags" -> "$_id.tags",
        "assignments" -> 1
      )
    )

    collection.aggregate(phaseUnwind, List(phaseMatch, phaseGroup, phaseProject)).map(_.result[Employee])
  }
}
