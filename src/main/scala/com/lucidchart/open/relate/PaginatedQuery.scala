package com.lucidchart.open.relate

import java.sql.Connection
import scala.collection.mutable.ArrayBuffer

object PaginatedQuery extends PaginatedQueryBuilder {

  @deprecated("Use a DBMS-specific method, such com.lucidchart.open.relate.mysql.PaginatedQuery.apply", "1.7.0")
  def apply[A](parser: SqlResult => A, limit: Int, startingOffset: Long)(query: ParameterizedSql)(implicit connection: Connection): Stream[A] = {
    mysql.PaginatedQuery.apply(parser, limit, startingOffset)(query)
  }

}

class PaginatedQueryBuilder {

  /**
   * Create a new PaginatedQuery with user supplied queries, execute it, and return a Stream over
   * the results. It should be noted that the PaginatedQuery makes absolutely no changes to the
   * supplied query, so users should make sure to include LIMIT and conditional statements in the
   * query.
   * @param parser the RowParser that will parse records from the database
   * @param getNextStmt a function that will, optionally given the last record in a page of results,
   * produce a query object that can be executed to get the next page of results. The last record 
   * Option will be None when getting the first page of results.
   * @param connection the connection to use to make the query
   * @return a Stream over all the records returned by the query, getting a new page of results
   * when the current one is exhausted
   */
  def apply[A](parser: SqlResult => A)(getNextStmt: Option[A] => Sql)(implicit connection: Connection): Stream[A] = {
    new PaginatedQuery(parser, connection).withQuery(getNextStmt)
  }

}

/**
 * A query object that will execute a query in a paginated format and return the results in a Stream
 */
private[relate] class PaginatedQuery[A](parser: SqlResult => A, connection: Connection) {

  /**
   * Create a lazily evaluated stream of results
   * @param lastRecord the last record of the previous page
   * @param getNextStmt a function that will take the last record of the previous page
   * and return a new statement to get the next page of results
   * @return a stream of results
   */
  private[relate] def withQuery(getNextStmt: Option[A] => Sql): Stream[A] = {
    /**
     * Get the next page of results
     * @param lastRecord the last record of the previous page
     * @return a stream of the records in the page
     */
    def page(lastRecord: Option[A]): ArrayBuffer[A] = {
      val sql = getNextStmt(lastRecord)
      implicit val c = connection
      sql.asCollection[A, ArrayBuffer](parser)
    }

    /**
     * Recursively create a lazily calculated list of records
     * @param lastRecord the last record of the previous page
     * @return a stream of records
     */
    def records(lastRecord: Option[A]): Stream[A] = {
      val currentPage = page(lastRecord)
      if (!currentPage.isEmpty) {
        currentPage.toStream #::: records(Some(currentPage.last))
      }
      else {
        Stream.Empty
      }
    }

    records(None)
  }

}
