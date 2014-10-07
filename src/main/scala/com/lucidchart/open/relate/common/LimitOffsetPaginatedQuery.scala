package com.lucidchart.open.relate.common

import com.lucidchart.open.relate._
import java.sql.Connection

private[relate] class LimitOffsetPaginatedQueryBuilder extends PaginatedQueryBuilder {

  /**
   * Create a new PaginatedQuery that uses LIMIT and OFFSET, execute it, and return a Stream over
   * the results.
   * @note using [[PaginatedQueryBuilder#apply(SqlResult => A, Int, Long)(ParameterizedSql)(Connection)]] may be faster
   * @param parser the RowParser that will parse records from the database
   * @param limit the number of records each page will contain
   * @param startingOffset the offset to start with
   * @param query the Sql object to use for the query. This object should already have all
   * parameters substituted into it
   * @param connection the connection to use to make the query
   * @return a Stream over all the records returned by the query, getting a new page of results
   * when the current one is exhausted
   */
  def apply[A](parser: SqlResult => A, limit: Int, startingOffset: Long)(query: ParameterizedSql)(implicit connection: Connection): Stream[A] = {
    new LimitOffsetPaginatedQuery(parser, connection).withLimitAndOffset(limit, startingOffset, query)
  }

}

/**
 * Paginates queries based on LIMIT and OFFSET.
 */
private[relate] class LimitOffsetPaginatedQuery[A](parser: SqlResult => A, connection: Connection) {
  self =>

  /**
   * Paginate results of a query by using LIMIT and OFFSET.
   * @param limit the number of records for a page
   * @param startingOffset the offset to start querying at
   * @param query the Sql object to use as the query (should have all parameters substituted in already)
   * @return whatever the callback returns
   */
  private[common] def withLimitAndOffset(limit: Int, startingOffset: Long, query: ParameterizedSql): Stream[A] = {
    val queryParams = query.queryParams
    val queryString = query.queryParams.query

    /**
     * Get the next page of results
     * @param offset how much to offset into the results
     * @return a stream of the records in the page
     */
    def page(offset: Long): Stream[A] = {
      val newParams = queryParams.copy(query = queryString + " LIMIT " + limit + " OFFSET " + offset)
      new ParameterizedSql with NormalStatementPreparer {
        def connection = self.connection
        val query = queryString
        def queryParams = newParams
      }.execute(_.asIterable(parser)).toStream
    }

    /**
     * Create a lazily evaluated stream of results
     * @param offset the offset into the database results
     * @return a stream of results
     */
    def records(offset: Long): Stream[A] = {
      val currentPage = page(offset)
      if (!currentPage.isEmpty) {
        currentPage #::: records(offset + limit)
      }
      else {
        Stream.Empty
      }
    }

    records(startingOffset)
  }

}
