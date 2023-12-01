/*
 * Copyright 2022 Google LLC
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *       https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.google.cloud.spark.bigquery.pushdowns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.execution.FormattedMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter

class SparkQuerySuite extends AnyFunSuite with BeforeAndAfter {
  private var spark: SparkSession = _

  before {
    spark = SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
  }

  after {
    spark.stop()
  }

  test("pushdown query") {
    spark.sql(
      """
         CREATE TABLE student(name string)
                USING com.google.cloud.spark.bigquery.v2.Spark33BigQueryTableProvider
         """).show()

    val df = spark.sql(
      """
        |SELECT *
        |  FROM student
        |""".stripMargin)
    val plan = df.queryExecution.executedPlan

    assert(plan.isInstanceOf[Spark33BigQueryPushdownScanExec])
    val sfPlan = plan.asInstanceOf[Spark33BigQueryPushdownScanExec]
    assert(sfPlan.pushdownQuery.getStatement().toString ==
      """SELECT * FROM ( default.student ) AS "CONNECTOR_QUERY_ALIAS""""
        .stripMargin)

    // explain plan
    val planString = df.queryExecution.explainString(FormattedMode)
    val expectedString =
      """== Physical Plan ==
        |PhysicalScan (1)
        |
        |
        |(1) Scan
        |Output [1]: [name#1]
        |Arguments: [name#1], SELECT * FROM ( default.student ) AS "CONNECTOR_QUERY_ALIAS", Relation
        """.stripMargin
    assert(planString.trim == expectedString.trim)
  }

}
