package com.munteanu.casbah

import com.mongodb.casbah.Imports._

/**
  * Created by romunteanu on 1/28/2016.
  */
object CasbahDemo {
  def main(args: Array[String]) = {

    val mongoClient = MongoClient("localhost", 27017)

    val db = mongoClient("casbah")
    println(db.collectionNames)

    val coll = db("employees")

    coll.dropCollection

    val alice = MongoDBObject(
                  "firstName" -> "Alice",
                  "lastName" -> "Parker",
                  "username" -> "alice.parker",
                  "password" -> "123abc",
                  "department" -> "Development",
                  "isActive" -> true)

    coll.insert(alice)

    val builder = MongoDBObject.newBuilder
    builder += "firstName" -> "Mark"
    builder += "lastName" -> "Lawrence"
    builder += "username" -> "mark.lawrence"
    builder += "password" -> "abc123"
    builder += "department" -> "Testing"
    builder += "isActive" -> true
    val mark = builder.result

    coll += mark

    // bulk insert
    val daniel = MongoDBObject(
      "firstName" -> "Daniel",
      "lastName" -> "Fowler",
      "username" -> "daniel.fowler",
      "password" -> "1a2b",
      "department" -> "Development",
      "isActive" -> false)

    val lisa = MongoDBObject(
      "firstName" -> "Lisa",
      "lastName" -> "Hunter",
      "username" -> "lisa.hunter",
      "password" -> "2b3c",
      "department" -> "Project Management",
      "isActive" -> true)

    val bulkBuilder = coll.initializeOrderedBulkOperation

    bulkBuilder.insert(daniel)
    bulkBuilder.insert(lisa)

    val bulkResult = bulkBuilder.execute()


    // create index
    // db.employees.createIndex({"username": 1});
    coll.createIndex("username")

    // TODO create multiple index: department, isActive

    // first one
    // db.employees.findOne()
    val first = coll.findOne()
    println("first:")
    println(first)


    // find by username
    // db.employees.findOne({"username": "mark.lawrence"}, {"_id": 0, "firstName": 1, "lastName ": 1});
    val username = "mark.lawrence"
    val maybeEmployee = coll.findOne(MongoDBObject("username" -> username), MongoDBObject("_id" -> 0, "firstName" -> 1, "lastName" -> 1))
    maybeEmployee match {
      case Some(employee) =>
        println("find by username:")
        println(employee)
      case _ => println("Employee " + username + " not found.")
    }


    // count
    // db.employees.count()
    val count = coll.count()
    println("count: " + count)


    // count with filter (count all active users)
    // db.employees.count({"isActive": true})
    val countWithFilter = coll.count(MongoDBObject("isActive" -> true))
    println("countWithFilter: " + countWithFilter)


    // all
    // db.employees.find().pretty();
    println("all:")
    coll.find() foreach (println _)


    // sort, skip, limit
    // db.employees.find().sort({"firstName": -1}).skip(2).limit(1)
    println("sort, skip, limit:")
    val orderBy = MongoDBObject("firstName" -> -1)
    coll.find().sort(orderBy).skip(2).limit(1) foreach (println _)


    // with filter
    // db.employees.find({"department": "Development", "isActive": true}).pretty();
    val filter = MongoDBObject("department" -> "Development") ++ ("isActive" -> true)
    println("filter:")
    coll.find(filter) foreach (employee =>
      println(employee("firstName") + " | " + employee("lastName") + " | " + employee("username") + " | " + employee("department")))

    // TODO distinct

    // TODO update

    // TODO remove

    // TODO insert with inner object

    // TODO more complex queries with $gte $lte and inner properties

  }
}
