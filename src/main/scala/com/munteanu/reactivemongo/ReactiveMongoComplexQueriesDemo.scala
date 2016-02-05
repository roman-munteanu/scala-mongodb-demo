package com.munteanu.reactivemongo

import com.munteanu.model.{Assignment, Employee}
import org.joda.time.format.{ISODateTimeFormat, DateTimeFormatter}
import reactivemongo.api.collections.bson.BSONCollection
import reactivemongo.api.commands.WriteResult
import reactivemongo.api.{DefaultDB, MongoConnection, MongoDriver}

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
      )
    )

    val insertResult: Future[WriteResult] = collection.insert(edward)
    Await.result(insertResult, timeout)

    ()
  }
}
