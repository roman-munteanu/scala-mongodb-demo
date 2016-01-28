package com.munteanu.model

/**
  * Created by romunteanu on 1/28/2016.
  */
case class Employee(
    firstName: String,
    lastName: String,
    username: String,
    password: String,
    department: String) {
  override def toString = s"Employee{firstName=$firstName, lastName=$lastName, username=$username, password=$password, department=$department}"
}