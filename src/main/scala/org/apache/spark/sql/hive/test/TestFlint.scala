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
package org.apache.spark.sql.hive.test

import java.io.File

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.language.implicitConversions

import org.apache.hadoop.hive.ql.exec.FunctionRegistry

import org.apache.spark.sql.SQLConf
import org.apache.spark.{SparkConf, SparkContext}

import org.apache.spark.sql.flint.FlintContextTrait
import com.intel.ssg.bdt.spark.sql.CalciteDialect

object TestFlint
  extends TestFlintContext(
    new SparkContext(
      System.getProperty("spark.sql.test.master", "local[32]"),
      "TestSQLContext",
      new SparkConf()
        .set("spark.sql.test", "")
        .set("spark.sql.hive.metastore.barrierPrefixes",
          "org.apache.spark.sql.hive.execution.PairSerDe")
        // SPARK-8910
        .set("spark.ui.enabled", "false")
        .set("spark.sql.dialect", "com.intel.ssg.bdt.spark.sql.CalciteDialect")))

/**
 * A locally running test instance of Spark's Hive execution engine.
 *
 * Data from [[testTables]] will be automatically loaded whenever a query is run over those tables.
 * Calling [[reset]] will delete all tables and other state in the database, leaving the database
 * in a "clean" state.
 *
 * TestHive is singleton object version of this class because instantiating multiple copies of the
 * hive metastore seems to lead to weird non-deterministic failures.  Therefore, the execution of
 * test cases that rely on TestHive must be serialized.
 */
class TestFlintContext(sc: SparkContext) extends TestHiveContext(sc) with FlintContextTrait {
  self =>

  protected[sql] override lazy val conf: SQLConf = new SQLConf {
    // The super.getConf(SQLConf.DIALECT) is "sql" by default, we need to set it as "hiveql"
    override def dialect: String =
      super.getConf(SQLConf.DIALECT, classOf[CalciteDialect].getCanonicalName)
    override def caseSensitiveAnalysis: Boolean = getConf(SQLConf.CASE_SENSITIVE, false)

    clear()

    override def clear(): Unit = {
      super.clear()

      TestFlintContext.overrideConfs.map {
        case (key, value) => setConfString(key, value)
      }
    }
  }

  private val loadedTables = new collection.mutable.HashSet[String]

  override def loadTestTable(name: String) {
    if (!(loadedTables contains name)) {
      // Marks the table as loaded first to prevent infinite mutually recursive table loading.
      loadedTables += name
      logDebug(s"Loading test table $name")
      val createCmds =
        testTables.get(name).map(_.commands).getOrElse(sys.error(s"Unknown test table $name"))
      createCmds.foreach(_())

      if (cacheTables) {
        cacheTable(name)
      }
    }
  }

  override def reset() {
    try {
      // HACK: Hive is too noisy by default.
      org.apache.log4j.LogManager.getCurrentLoggers.asScala.foreach { log =>
        val logger = log.asInstanceOf[org.apache.log4j.Logger]
        if (!logger.getName.contains("org.apache.spark")) {
          logger.setLevel(org.apache.log4j.Level.WARN)
        }
      }

      cacheManager.clearCache()
      loadedTables.clear()
      catalog.cachedDataSourceTables.invalidateAll()
      catalog.client.reset()
      catalog.unregisterAllTables()

      FunctionRegistry.getFunctionNames.asScala.filterNot(originalUDFs.contains(_)).
        foreach { udfName => FunctionRegistry.unregisterTemporaryUDF(udfName) }

      // Some tests corrupt this value on purpose, which breaks the RESET call below.
      hiveconf.set("fs.default.name", new File(".").toURI.toString)
      // It is important that we RESET first as broken hooks that might have been set could break
      // other sql exec here.
      executionHive.runSqlHive("RESET")
      metadataHive.runSqlHive("RESET")
      // For some reason, RESET does not reset the following variables...
      // https://issues.apache.org/jira/browse/HIVE-9004
      runSqlHive("set hive.table.parameters.default=")
      runSqlHive("set datanucleus.cache.collections=true")
      runSqlHive("set datanucleus.cache.collections.lazy=true")
      // Lots of tests fail if we do not change the partition whitelist from the default.
      runSqlHive("set hive.metastore.partition.name.whitelist.pattern=.*")

      configure().foreach {
        case (k, v) =>
          metadataHive.runSqlHive(s"SET $k=$v")
      }
      defaultOverrides()

      runSqlHive("USE default")
    } catch {
      case e: Exception =>
        logError("FATAL ERROR: Failed to reset TestDB state.", e)
    }
  }
}

private[hive] object TestFlintContext {

  /**
   * A map used to store all confs that need to be overridden in sql/hive unit tests.
   */
  val overrideConfs: Map[String, String] =
    Map(
      // Fewer shuffle partitions to speed up testing.
      SQLConf.SHUFFLE_PARTITIONS.key -> "5"
    )
}
