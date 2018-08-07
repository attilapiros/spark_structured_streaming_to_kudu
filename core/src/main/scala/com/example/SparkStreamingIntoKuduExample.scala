/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package com.example

import org.apache.kudu.client._

import org.apache.spark.sql._

class KuduForeachWriter(val kuduMaster: String, val kuduTableName: String)
  extends ForeachWriter[Row] {

  var client: KuduClient = _
  var kuduSession: KuduSession = _
  var kuduTable: KuduTable = _

  def open(partitionId: Long, version: Long): Boolean = {
    this.client = new KuduClient.KuduClientBuilder(kuduMaster).build()
    this.kuduTable = client.openTable(kuduTableName);
    this.kuduSession = client.newSession()
    true
  }

  def process(record: Row): Unit = {
    val upsert = this.kuduTable.newUpsert();
    val row: PartialRow = upsert.getRow();
    row.addString(0, record.getString(0));
    if (record.isNullAt(1)) {
      row.setNull(1)
    } else {
      row.addInt(1, record.getInt(1));
    }
    this.kuduSession.apply(upsert);
  }

  def close(errorOrNull: Throwable): Unit = {
   this.kuduSession.close()
   this.client.shutdown()
  }
}

/**
 * Assuming table is created ie via impala-shell like:
 * CREATE TABLE test_table (value STRING PRIMARY KEY, count INT) STORED AS KUDU;
 *
 * Usage: SparkStreamingIntoKuduExample <masterUrl> <hostname> <port> <kuduMaster> <kuduTable>
 * <hostname> and <port> describe the TCP server that Structured Streaming
 * would connect to receive data.
 *
 * To run this on your local machine, you need to first run a Netcat server
 *    `$ nc -lk 9999`
 * and then run this example.
 */
object SparkStreamingIntoKuduExample {

  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: SparkStreamingIntoKuduExample <masterUrl> <hostname> " +
        "<port> <kuduMaster> <kuduTable>")
      System.exit(1)
    }

    val master = args(0)
    val host = args(1)
    val port = args(2).toInt
    val kuduMaster = args(3)
    val tableName = args(4)

    val spark = SparkSession
      .builder
      .appName("SparkStreamingIntoKuduExample")
      .master(master)
      .config("spark.network.timeout", "3700s")
      .config("spark.executor.heartbeatInterval", "3600s")
      .getOrCreate()

    import spark.implicits._

    // Create DataFrame representing the stream of input lines from connection to host:port
    val lines = spark.readStream
      .format("socket")
      .option("host", host)
      .option("port", port)
      .load()
    val staticData = Seq(("one", 1), ("two", 2)).toDF("value", "count")

    // Split the lines into words
    val words = lines.as[String].flatMap(_.split(" "))

    // Start running the query that prints the running counts to the console
    val query = words.join(staticData, Seq("value"), "left_outer")
      .writeStream
      .foreach(new KuduForeachWriter(kuduMaster, tableName))
      .outputMode("update")
      .start()

    query.awaitTermination()
  }
}
// scalastyle:on println
