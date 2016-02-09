package com.munteanu.model

import reactivemongo.bson.{BSONDocument, BSONDocumentWriter, BSONDocumentReader}

/**
  * Created by romunteanu on 1/28/2016.
  */
case class Employee(
    firstName: String,
    lastName: String,
    username: String,
    password: String,
    department: String,
    isActive: Boolean,
    assignments: Seq[Assignment],
    tags: Seq[String]) {
  override def toString = s"Employee{firstName=$firstName, lastName=$lastName, username=$username, password=$password, department=$department, isActive=$isActive, assignments=$assignments, tags=$tags}"
}

object Employee {

  implicit object EmployeeBSONReader extends BSONDocumentReader[Employee] {
    override def read(bson: BSONDocument): Employee =
      Employee(
        bson.getAs[String]("firstName").get,
        bson.getAs[String]("lastName").get,
        bson.getAs[String]("username").get,
        bson.getAs[String]("password").getOrElse(""),
        bson.getAs[String]("department").get,
        bson.getAs[Boolean]("isActive").getOrElse(true),
        bson.getAs[Seq[Assignment]]("assignments").toSeq.flatten,
        bson.getAs[Seq[String]]("tags").toSeq.flatten
      )
  }

  implicit object EmployeeBSONWriter extends BSONDocumentWriter[Employee] {
    override def write(employee: Employee): BSONDocument =
      BSONDocument(
        "firstName" -> employee.firstName,
        "lastName" -> employee.lastName,
        "username" -> employee.username,
        "password" -> employee.password,
        "department" -> employee.department,
        "isActive" -> employee.isActive,
        "assignments" -> employee.assignments,
        "tags" -> employee.tags
      )
  }

}