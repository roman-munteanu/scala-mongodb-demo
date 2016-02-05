package com.munteanu.model

import org.joda.time.DateTime
import reactivemongo.bson.{BSONDateTime, BSONDocument, BSONDocumentWriter, BSONDocumentReader}

/**
  * Created by romunteanu on 1/28/2016.
  */
case class Assignment(
  projectCode: String,
  dateFrom: DateTime,
  dateTo: DateTime,
  percentage: Int) {
  override def toString = s"Assignment{projectCode=$projectCode, dateFrom=$dateFrom, dateTo=$dateTo, percentage=$percentage}"
}

object Assignment {

  implicit object AssignmentBSONReader extends BSONDocumentReader[Assignment] {
    override def read(bson: BSONDocument): Assignment =
      Assignment(
        bson.getAs[String]("projectCode").get,
        bson.getAs[BSONDateTime]("dateFrom").map(dt => new DateTime(dt.value)).get,
        bson.getAs[BSONDateTime]("dateTo").map(dt => new DateTime(dt.value)).get,
        bson.getAs[Int]("percentage").get
      )
  }

  implicit object AssignmentBSONWriter extends BSONDocumentWriter[Assignment] {
    override def write(t: Assignment): BSONDocument =
      BSONDocument(
        "projectCode" -> t.projectCode,
        "dateFrom" -> BSONDateTime(t.dateFrom.getMillis),
        "dateTo" -> BSONDateTime(t.dateTo.getMillis),
        "percentage" -> t.percentage
      )
  }
}