package com.munteanu

import _root_.reactivemongo.bson.BSONDocument

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future

/**
  * Created by romunteanu on 2/8/2016.
  */
package object reactivemongo {

  def printFutureCollection(futureCollection: Future[List[BSONDocument]], label: String = ""): Unit = {
    futureCollection.map(docs =>
      docs.foreach(doc =>
        println(s"$label: ${BSONDocument pretty doc}")
      )
    )
  }

  def printFutureDomainCollection(futureCollection: Future[List[Any]], label: String = ""): Unit = {
    futureCollection.map(items =>
      items.foreach(item =>
        println(s"$label: $item")
      )
    )
  }
}
