package com.munteanu

import _root_.reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by romunteanu on 2/8/2016.
  */
package object reactivemongo {

  def printFutureCollection(futureCollection: Future[List[Any]], label: String = "DATA:"): Unit = {
    println(label)
    futureCollection.map(items =>
      items.foreach {
          case doc: BSONDocument =>
            println(BSONDocument pretty doc)
          case item =>
            println(item.toString)
        }
    )
  }

}
