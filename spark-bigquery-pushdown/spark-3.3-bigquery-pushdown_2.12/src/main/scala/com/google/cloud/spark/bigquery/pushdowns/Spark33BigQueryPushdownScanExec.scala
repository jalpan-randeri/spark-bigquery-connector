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

import com.google.cloud.spark.bigquery.direct.BigQueryRDDFactory
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Attribute, UnsafeProjection}
import org.apache.spark.sql.execution.LeafExecNode

import java.util.concurrent.{Callable, ExecutorService, Executors, Future}

case class Spark33BigQueryPushdownScanExec(projection: Seq[Attribute],
                                           pushdownQuery: BigQuerySQLQuery,
                                           bigQueryRDDFactory: BigQueryRDDFactory)
  extends LeafExecNode {

  // result holder
  @transient implicit private var data: Future[PushDownResult] = _
  @transient implicit private val service: ExecutorService = Executors.newCachedThreadPool()

  override def doPrepare(): Unit = {
    logInfo(s"Preparing query to push down - ${pushdownQuery.getStatement()}")

    val work = new Callable[PushDownResult]() {
      override def call(): PushDownResult = {
        val result = {
          try {
            val data = bigQueryRDDFactory.buildScanFromSQL(pushdownQuery.getStatement().toString)
            PushDownResult(data = Some(data))
          } catch {
            case e: Exception =>
              logError("Failure in query execution", e)
              PushDownResult(failure = Some(e))
          }
        }
        result
      }
    }
    data = service.submit(work)
    logInfo("submitted query asynchronously")
  }


  override protected def doExecute(): RDD[InternalRow] = {
    if (data.get().failure.nonEmpty) {
      // raise original exception
      throw data.get().failure.get
    }

    data.get().data.get.mapPartitions { iter =>
      val project = UnsafeProjection.create(schema)
      iter.map(project)
    }
  }

  override def cleanupResources(): Unit = {
    logDebug(s"shutting down service to clean up resources")
    service.shutdown()
  }

  override def output: Seq[Attribute] = projection

}

/**
 * Result holder
 *
 * @param data    RDD that holds the data
 * @param failure failure information if we unable to push down
 */
private case class PushDownResult(data: Option[RDD[InternalRow]] = None,
                                  failure: Option[Exception] = None)
  extends Serializable
