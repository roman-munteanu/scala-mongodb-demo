package com.munteanu.model

import com.mongodb.casbah.query.dsl.BSONType.BSONBoolean
import reactivemongo.bson.{BSONString, BSONDocument, BSONDocumentWriter, BSONDocumentReader}

/**
  * Created by romunteanu on 1/28/2016.
  */
case class Employee(
    firstName: String,
    lastName: String,
    username: String,
    password: String,
    department: String,
    isActive: Boolean) {
  override def toString = s"Employee{firstName=$firstName, lastName=$lastName, username=$username, password=$password, department=$department, isActive=$isActive}"
}

object Employee {

  implicit object EmployeeImplicit extends BSONDocumentReader[Employee] with BSONDocumentWriter[Employee] {

    override def write(employee: Employee): BSONDocument =
      BSONDocument(
        "firstName" -> employee.firstName,
        "lastName" -> employee.lastName,
        "username" -> employee.username,
        "password" -> employee.password,
        "department" -> employee.department,
        "isActive" -> employee.isActive
      )

    override def read(bson: BSONDocument): Employee =
      Employee(
        bson.getAs[String]("firstName").get,
        bson.getAs[String]("lastName").get,
        bson.getAs[String]("username").get,
        bson.getAs[String]("password").get,
        bson.getAs[String]("department").get,
        bson.getAs[Boolean]("isActive").get
      )
  }


}