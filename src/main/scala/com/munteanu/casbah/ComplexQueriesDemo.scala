package com.munteanu.casbah

import com.mongodb.casbah.Imports._
import com.mongodb.casbah.commons.conversions.scala._
import org.joda.time.DateTime
import org.joda.time.format.{DateTimeFormatter, ISODateTimeFormat}

/**
  * Created by romunteanu on 1/29/2016.
  */
object ComplexQueriesDemo {
  def main(args: Array[String]) = {

    val db = MongoClient("localhost", 27017)("casbah")

    val coll = db("assignments")

    coll.dropCollection

    RegisterJodaTimeConversionHelpers()

    // insert with inner object
    val edward = MongoDBObject(
      "firstName" -> "Edward",
      "lastName" -> "Adams",
      "username" -> "edward.adams",
      "password" -> "11aa22bb",
      "department" -> "Development",
      "isActive" -> true,
      "assignments" -> List(
        MongoDBObject(
          "projectCode" -> "pc111",
          "dateFrom" -> (new DateTime).withYear(2014).withMonthOfYear(1).withDayOfMonth(1),
          "dateTo" -> DateTime.now,
          "percentage" -> 50
        ),
        MongoDBObject(
          "projectCode" -> "pc222",
          "dateFrom" -> (new DateTime).withYear(2013).withMonthOfYear(1).withDayOfMonth(1),
          "dateTo" -> (new DateTime).withYear(2013).withMonthOfYear(12).withDayOfMonth(31),
          "percentage" -> 50
        )
      ),
      "tags" -> List("Scala", "Ruby", "Python")
    )

    val roy = MongoDBObject(
      "firstName" -> "Roy",
      "lastName" -> "Howard",
      "username" -> "roy.howard",
      "password" -> "33cc44dd",
      "department" -> "Development",
      "isActive" -> true,
      "assignments" -> List(
        MongoDBObject(
          "projectCode" -> "pc111",
          "dateFrom" -> (new DateTime).withYear(2014).withMonthOfYear(1).withDayOfMonth(1),
          "dateTo" -> DateTime.now,
          "percentage" -> 100
        )
      ),
      "tags" -> List("Java", "Ruby")
    )

    val emma = MongoDBObject(
      "firstName" -> "Emma",
      "lastName" -> "Watson",
      "username" -> "emma.watson",
      "password" -> "55ee66ff",
      "department" -> "Testing",
      "isActive" -> false,
      "assignments" -> List(
        MongoDBObject(
          "projectCode" -> "pc333",
          "dateFrom" -> (new DateTime).withYear(2015).withMonthOfYear(1).withDayOfMonth(1),
          "dateTo" -> DateTime.now,
          "percentage" -> 75
        )
      ),
      "tags" -> List("Haskell", "Go")
    )

    val bulkBuilder = coll.initializeOrderedBulkOperation

    bulkBuilder.insert(edward)
    bulkBuilder.insert(roy)
    bulkBuilder.insert(emma)

    val bulkResult = bulkBuilder.execute()

    // multiple indexes
    // db.assignments.createIndex({"department": 1, "isActive": 1})
    coll.createIndex(MongoDBObject("department" -> 1, "isActive" -> 1))

    // db.assignments.find({"tags": {"$in": ["Scala", "Ruby"]}}, {"username": 1, "tags": 1}).pretty();
    println("search by tags: ")
    coll.createIndex(MongoDBObject("tags" -> 1))

    coll.find("tags" $in ("Scala", "Ruby"), MongoDBObject("username" -> 1, "tags" -> 1)) foreach (println _)


    // more complex queries with $gte $lte on inner properties
    // db.assignments.find({"assignments.dateFrom": {"$lte": ISODate("2014-01-01T00:00:00.000Z")}, "assignments.dateTo": {"$gte": ISODate("2013-01-01T00:00:00.000Z")}}).sort({"firstName": 1}).pretty();

    coll.createIndex(MongoDBObject("firstName" -> 1, "assignments.dateFrom" -> 1, "assignments.dateTo" -> 1))

    println("gte, lte: ")
    val formatter: DateTimeFormatter = ISODateTimeFormat.dateTime()

    val date2013JAN1  = formatter.parseDateTime("2013-01-01T00:00:00.000Z")
    val date2013DEC31 = formatter.parseDateTime("2013-12-31T00:00:00.000Z")
    val date2014JAN1  = formatter.parseDateTime("2014-01-01T00:00:00.000Z")
    val date2015JAN1  = formatter.parseDateTime("2015-01-01T00:00:00.000Z")
    val date2015DEC31 = formatter.parseDateTime("2015-12-31T00:00:00.000Z")

    val startDate = date2013JAN1
    val endDate   = date2014JAN1

    val condition = /*MongoDBObject("department" -> "Development") ++ ("isActive" -> true) ++*/
                    ("assignments.dateFrom" $lte endDate) ++
                    ("assignments.dateTo" $gte startDate)

    coll.find(condition).sort(MongoDBObject("firstName" -> 1)) foreach (println _)

    ()
  }
}
