/*
 * Copyright (2020) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.delta

import java.util.Locale

import org.apache.spark.sql.delta.sources.DeltaSQLConf
import org.apache.spark.sql.delta.test.DeltaSQLCommandTest
import io.delta.tables._
import org.apache.hadoop.fs.Path
import org.scalatest.Assertion

import org.apache.spark.sql._
import org.apache.spark.sql.delta.DeltaConfigs._
import org.apache.spark.sql.delta.catalog.DeltaTableV2
import org.apache.spark.sql.delta.commands.AlterTableSetPropertiesDeltaCommand
import org.apache.spark.sql.delta.util.DeltaBloomFilter
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.types.StructType

class MergeIntoScalaSuite extends MergeIntoSuiteBase  with DeltaSQLCommandTest {

  import testImplicits._

  test("basic scala API") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("key1", "value1"), Nil)  // target
      val source = Seq((1, 100), (3, 30)).toDF("key2", "value2")  // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, "key1 = key2")
        .whenMatched().updateExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .whenNotMatched().insertExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Nil)
    }
  }

  test("extended scala API") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40)).toDF("key1", "value1"), Nil)  // target
      val source = Seq((1, 100), (3, 30), (4, 41)).toDF("key2", "value2")  // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, "key1 = key2")
        .whenMatched("key1 = 4").delete()
        .whenMatched("key2 = 1").updateExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .whenNotMatched("key2 = 3").insertExpr(Map("key1" -> "key2", "value1" -> "value2"))
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Nil)
    }
  }

  test("extended scala API with Column") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40)).toDF("key1", "value1"), Nil)  // target
      val source = Seq((1, 100), (3, 30), (4, 41)).toDF("key2", "value2")  // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath)
        .merge(source, functions.expr("key1 = key2"))
        .whenMatched(functions.expr("key1 = 4")).delete()
        .whenMatched(functions.expr("key2 = 1"))
        .update(Map("key1" -> functions.col("key2"), "value1" -> functions.col("value2")))
        .whenNotMatched(functions.expr("key2 = 3"))
        .insert(Map("key1" -> functions.col("key2"), "value1" -> functions.col("value2")))
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Nil)
    }
  }

  test("updateAll and insertAll") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40), (5, 50)).toDF("key", "value"), Nil)
      val source = Seq((1, 100), (3, 30), (4, 41), (5, 51), (6, 60))
        .toDF("key", "value").createOrReplaceTempView("source")

      executeMerge(
        target = s"delta.`$tempPath` as t",
        source = "source s",
        condition = "s.key = t.key",
        update = "*",
        insert = "*")

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) ::    // Update
          Row(2, 20) ::     // No change
          Row(3, 30) ::     // Insert
          Row(4, 41) ::     // Update
          Row(5, 51) ::     // Update
          Row(6, 60) ::     // Insert
          Nil)
    }
  }

  test("updateAll and insertAll with columns containing dot") {
    withTable("source") {
      append(Seq((1, 10), (2, 20), (4, 40)).toDF("key", "the.value"), Nil) // target
      val source = Seq((1, 100), (3, 30), (4, 41)).toDF("key", "the.value") // source

      io.delta.tables.DeltaTable.forPath(spark, tempPath).as("t")
        .merge(source.as("s"), "t.key = s.key")
        .whenMatched()
        .updateAll()
        .whenNotMatched()
        .insertAll()
        .execute()

      checkAnswer(
        readDeltaTable(tempPath),
        Row(1, 100) :: // Update
          Row(2, 20) :: // No change
          Row(4, 41) :: // Update
          Row(3, 30) :: // Insert
          Nil)
    }
  }

  test("update with empty map should do nothing") {
    append(Seq((1, 10), (2, 20)).toDF("trgKey", "trgValue"), Nil) // target
    val source = Seq((1, 100), (3, 30)).toDF("srcKey", "srcValue") // source
    io.delta.tables.DeltaTable.forPath(spark, tempPath)
      .merge(source, "srcKey = trgKey")
      .whenMatched().updateExpr(Map[String, String]())
      .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .execute()

    checkAnswer(
      readDeltaTable(tempPath),
      Row(1, 10) ::       // Not updated since no update clause
      Row(2, 20) ::       // No change due to merge condition
      Row(3, 30) ::       // Not updated since no update clause
      Nil)

    // match condition should not be ignored when map is empty
    io.delta.tables.DeltaTable.forPath(spark, tempPath)
      .merge(source, "srcKey = trgKey")
      .whenMatched("trgKey = 1").updateExpr(Map[String, String]())
      .whenMatched().delete()
      .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
      .execute()

    checkAnswer(
      readDeltaTable(tempPath),
      Row(1, 10) ::     // Neither updated, nor deleted (condition is not ignored)
      Row(2, 20) ::     // No change due to merge condition
      Nil)              // Deleted (3, 30)
  }

  // Checks specific to the APIs that are automatically handled by parser for SQL
  test("check invalid merge API calls") {
    withTable("source") {
      append(Seq((1, 10), (2, 20)).toDF("trgKey", "trgValue"), Nil) // target
      val source = Seq((1, 100), (3, 30)).toDF("srcKey", "srcValue") // source

      // There must be at least one WHEN clause in a MERGE statement
      var e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .execute()
      }
      errorContains(e.getMessage, "There must be at least one WHEN clause in a MERGE query")

      // When there are 2 MATCHED clauses in a MERGE statement,
      // the first MATCHED clause must have a condition
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().delete()
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "the first MATCHED clause must have a condition")

      // There must be at most two WHEN clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenMatched("trgValue = 3").delete()
          .whenMatched("trgValue = 2")
          .updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue + 1"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "There must be at most two match clauses in a MERGE query")

      // INSERT can appear at most once in NOT MATCHED clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey + 1", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage,
        "INSERT, UPDATE and DELETE cannot appear twice in one MERGE query")

      // UPDATE can appear at most once in MATCHED clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched("trgKey = 1").updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenMatched("trgValue = 2")
          .updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue + 1"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage,
        "INSERT, UPDATE and DELETE cannot appear twice in one MERGE query")

      // DELETE can appear at most once in MATCHED clauses in a MERGE statement
      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched("trgKey = 1").delete()
          .whenMatched("trgValue = 2").delete()
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage,
        "INSERT, UPDATE and DELETE cannot appear twice in one MERGE query")

      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().updateExpr(Map("trgKey" -> "srcKey", "*" -> "*"))
          .whenNotMatched().insertExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .execute()
      }
      errorContains(e.getMessage, "cannot resolve `*`")

      e = intercept[AnalysisException] {
        io.delta.tables.DeltaTable.forPath(spark, tempPath)
          .merge(source, "srcKey = trgKey")
          .whenMatched().updateExpr(Map("trgKey" -> "srcKey", "trgValue" -> "srcValue"))
          .whenNotMatched().insertExpr(Map("*" -> "*"))
          .execute()
      }
      errorContains(e.getMessage, "cannot resolve `*`")
    }
  }

  test("merge after schema change") {
    withSQLConf((DeltaSQLConf.DELTA_SCHEMA_AUTO_MIGRATE.key, "true")) {
      withTempPath { targetDir =>
        val targetPath = targetDir.getCanonicalPath
        spark.range(10).write.format("delta").save(targetPath)
        val t = io.delta.tables.DeltaTable.forPath(spark, targetPath).as("t")
        assert(t.toDF.schema == StructType.fromDDL("id LONG"))

        // Do one merge to change the schema.
        t.merge(Seq((11L, "newVal11")).toDF("id", "newCol1").as("s"), "t.id = s.id")
          .whenMatched().updateAll()
          .whenNotMatched().insertAll()
          .execute()
        // assert(t.toDF.schema == StructType.fromDDL("id LONG, newCol1 STRING"))

        // SC-35564 - ideally this shouldn't throw an error, but right now we can't fix it without
        // causing a regression.
        val ex = intercept[Exception] {
          t.merge(Seq((12L, "newVal12")).toDF("id", "newCol2").as("s"), "t.id = s.id")
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()
        }
        ex.getMessage.contains("schema of your Delta table has changed in an incompatible way")
      }
    }
  }

  test("merge without table alias") {
    withTempDir { dir =>
      val location = dir.getAbsolutePath
      Seq((1, 1, 1), (2, 2, 2)).toDF("part", "id", "n").write
        .format("delta")
        .partitionBy("part")
        .save(location)
      val table = io.delta.tables.DeltaTable.forPath(spark, location)
      val data1 = Seq((2, 2, 4, 2), (9, 3, 6, 9), (3, 3, 9, 3)).toDF("part", "id", "n", "part2")
      table.alias("t").merge(
        data1,
        "t.part = part2")
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }
  }

  test("merge without table alias with pre-computed condition") {
    withTempDir { dir =>
      val location = dir.getAbsolutePath
      Seq((1, 1, 1), (2, 2, 2)).toDF("part", "id", "x").write
        .format("delta")
        .partitionBy("part")
        .save(location)
      val table = io.delta.tables.DeltaTable.forPath(spark, location)
      val tableDf = table.toDF
      val data1 = Seq((2, 2, 4), (2, 3, 6), (3, 3, 9)).toDF("part", "id", "x")
      table.merge(
        data1,
        tableDf("part") === data1("part") && tableDf("id") === data1("id"))
        .whenMatched().updateAll()
        .whenNotMatched().insertAll()
        .execute()
    }
  }

  override protected def executeMerge(
      target: String,
      source: String,
      condition: String,
      update: String,
      insert: String): Unit = {

    executeMerge(
      tgt = target,
      src = source,
      cond = condition,
      MergeClause(isMatched = true, condition = null, action = s"UPDATE SET $update"),
      MergeClause(isMatched = false, condition = null, action = s"INSERT $insert"))
  }

  override protected def executeMerge(
      tgt: String,
      src: String,
      cond: String,
      clauses: MergeClause*): Unit = {

    def parseTableAndAlias(tableNameWithAlias: String): (String, Option[String]) = {
      tableNameWithAlias.split(" ").toList match {
        case tableName :: Nil =>
          // 'MERGE INTO tableName' OR `MERGE INTO delta.`path`'
          tableName -> None
        case tableName :: alias :: Nil =>
          // 'MERGE INTO tableName alias' or 'MERGE INTO delta.`path` alias'
          tableName -> Some(alias)
        case list if list.size >= 3 && list(list.size - 2).toLowerCase(Locale.ROOT) == "as" =>
          // 'MERGE INTO ... AS alias'
          list.dropRight(2).mkString(" ").trim() -> Some(list.last)
        case list if list.size >= 2 =>
          // 'MERGE INTO ... alias'
          list.dropRight(1).mkString(" ").trim() -> Some(list.last)
        case _ =>
          fail(s"Could not build parse '$tableNameWithAlias' for table and optional alias")
      }
    }

    def buildClause(
      clause: MergeClause,
      mergeBuilder: DeltaMergeBuilder): DeltaMergeBuilder = {

      if (clause.isMatched) {
        val actionBuilder: DeltaMergeMatchedActionBuilder =
          if (clause.condition != null) mergeBuilder.whenMatched(clause.condition)
          else mergeBuilder.whenMatched()
        if (clause.action.startsWith("DELETE")) {   // DELETE clause
          actionBuilder.delete()
        } else {                                    // UPDATE clause
          val setColExprStr = clause.action.trim.stripPrefix("UPDATE SET")
          if (setColExprStr.trim == "*") {          // UPDATE SET *
            actionBuilder.updateAll()
          } else {                                  // UPDATE SET x = a, y = b, z = c
            val setColExprPairs = parseUpdate(setColExprStr.split(","))
            actionBuilder.updateExpr(setColExprPairs)
          }
        }
      } else {                                        // INSERT clause
        val actionBuilder: DeltaMergeNotMatchedActionBuilder =
          if (clause.condition != null) mergeBuilder.whenNotMatched(clause.condition)
          else mergeBuilder.whenNotMatched()
        val valueStr = clause.action.trim.stripPrefix("INSERT")
        if (valueStr.trim == "*") {                   // INSERT *
          actionBuilder.insertAll()
        } else {                                      // INSERT (x, y, z) VALUES (a, b, c)
          val valueColExprsPairs = parseInsert(valueStr, Some(clause))
          actionBuilder.insertExpr(valueColExprsPairs)
        }
      }
    }

    val deltaTable = {
      val (tableNameOrPath, optionalAlias) = parseTableAndAlias(tgt)
      var table = makeDeltaTable(tableNameOrPath)
      optionalAlias.foreach { alias => table = table.as(alias) }
      table
    }

    val sourceDataFrame: DataFrame = {
      val (tableOrQuery, optionalAlias) = parseTableAndAlias(src)
      var df =
        if (tableOrQuery.startsWith("(")) spark.sql(tableOrQuery) else spark.table(tableOrQuery)
      optionalAlias.foreach { alias => df = df.as(alias) }
      df
    }

    var mergeBuilder = deltaTable.merge(sourceDataFrame, cond)
    clauses.foreach { clause =>
      mergeBuilder = buildClause(clause, mergeBuilder)
    }
    mergeBuilder.execute()
    deltaTable.toDF
  }

  override def testNestedDataSupport(name: String, namePrefix: String = "nested data support")(
      source: String,
      target: String,
      update: Seq[String],
      insert: String = null,
      schema: StructType = null,
      result: String = null,
      errorStrs: Seq[String] = null): Unit = {

    require(result == null ^ errorStrs == null, "either set the result or the error strings")

    val testName =
      if (result != null) s"$namePrefix - $name" else s"$namePrefix - analysis error - $name"

    test(testName) {
      withJsonData(source, target, schema) { case (sourceName, targetName) =>
        val pathOrName = parsePath(targetName)
        val fieldNames = readDeltaTable(pathOrName).schema.fieldNames
        val keyName = s"`${fieldNames.head}`"
        val updateColExprMap = parseUpdate(update)
        val insertExprMaps = if (insert != null) {
          parseInsert(insert, None)
        } else {
          fieldNames.map { f => s"t.`$f`" -> s"s.`$f`" }.toMap
        }

        def execMerge() = {
          val t = makeDeltaTable(targetName)
          t.as("t")
            .merge(
              spark.table(sourceName).as("s"),
              s"s.$keyName = t.$keyName")
            .whenMatched().updateExpr(updateColExprMap)
            .whenNotMatched().insertExpr(insertExprMaps)
            .execute()
        }

        if (result != null) {
          execMerge()
          checkAnswer(readDeltaTable(pathOrName), spark.read.json(strToJsonSeq(result).toDS))
        } else {
          val e = intercept[AnalysisException] { execMerge() }
          errorStrs.foreach { s => errorContains(e.getMessage, s) }
        }
      }
    }
  }

  private def makeDeltaTable(nameOrPath: String): DeltaTable = {
    val isPath: Boolean = nameOrPath.startsWith("delta.")
    if (isPath) {
      val path = nameOrPath.stripPrefix("delta.`").stripSuffix("`")
      io.delta.tables.DeltaTable.forPath(spark, path)
    } else {
      DeltaTableTestUtils.createTable(spark.table(nameOrPath), DeltaLog.forTable(spark, nameOrPath))
    }
  }

  private def parseUpdate(update: Seq[String]): Map[String, String] = {
    update.map { _.split("=").toList }.map {
      case setCol :: setExpr :: Nil => setCol.trim -> setExpr.trim
      case _ => fail("error parsing update actions " + update)
    }.toMap
  }

  private def parseInsert(valueStr: String, clause: Option[MergeClause]): Map[String, String] = {
    valueStr.split("VALUES").toList match {
      case colsStr :: exprsStr :: Nil =>
        def parse(str: String): Seq[String] = {
          str.trim.stripPrefix("(").stripSuffix(")").split(",").map(_.trim)
        }
        val cols = parse(colsStr)
        val exprs = parse(exprsStr)
        require(cols.size == exprs.size,
          s"Invalid insert action ${clause.get.action}: cols = $cols, exprs = $exprs")
        cols.zip(exprs).toMap

      case list =>
        fail(s"Invalid insert action ${clause.get.action} split into $list")
    }
  }

  private def parsePath(nameOrPath: String): String = {
    if (nameOrPath.startsWith("delta.`")) {
      nameOrPath.stripPrefix("delta.`").stripSuffix("`")
    } else nameOrPath
  }

  val defaultDeltaConf: Map[DeltaConfig[_], Any] = Map(
    LOG_RETENTION -> "interval 5 days"
    , CHECKPOINT_INTERVAL -> 10
    , ENABLE_EXPIRED_LOG_CLEANUP -> true
    , TOMBSTONE_RETENTION -> "interval 1 week"
    , AUTO_OPTIMIZE -> true
    , SYMLINK_FORMAT_MANIFEST_ENABLED -> true
    , DATA_SKIPPING_NUM_INDEXED_COLS -> -1
    , ENABLE_BLOOM_FILTER -> true
    , ID_COLS -> "id"
  )
  val FILE_NAME = "__fileName__"

  def getDeltaConf: Map[String, String] = {
    defaultDeltaConf.map {
      case (kk, vv) => kk.key -> vv.toString
    }
  }

  def getBloomFilterDF(deltaLog: DeltaLog): DataFrame = {
    val bfPath = new Path(
      deltaLog.dataPath
      , DeltaConfigs.BLOOM_FILTER_LOCATION.fromMetaData(deltaLog.snapshot.metadata)
    )
    spark.read.format("parquet")
      .load(bfPath.toUri.getPath)
  }


  def compareAndGetDeltaFilesAndBloomFiles(): Int = {
    val deltaLog = DeltaLog.forTable(spark, tempPath)
    val dataFrame = readDeltaTable(tempPath)
      .withColumn(FILE_NAME, input_file_name())
    val dataFiles = dataFrame.select(FILE_NAME).distinct().collect()
      .map(x => new Path(x.getAs[String](FILE_NAME)).getName).toSeq.sorted
    val bfDF = getBloomFilterDF(deltaLog)
    bfDF.show()
    val bloomFiles = bfDF.select("fileName").collect()
      .map(x => new Path(x.getAs[String]("fileName")).getName).toSeq.sorted
    assert(bloomFiles == dataFiles, s"bloom files $bloomFiles expect data files $dataFiles")
    dataFiles.length
  }

  def checkDeltaBloomProperties(): Unit = {
    val deltaLog = DeltaLog.forTable(spark, tempPath)
    assert(DeltaBloomFilter.checkBloomFilterIfExist(deltaLog),
      s"bloom filter ${BLOOM_FILTER_LOCATION.fromMetaData(deltaLog.snapshot.metadata)} " +
        s"no match with version ${deltaLog.snapshot.version}")
    assert(DeltaConfigs.ID_COLS.fromMetaData(deltaLog.snapshot.metadata) == Seq("id")
      , "idCols should be update!")
  }

  test("bloom filter no config - scala API") {
    withTable("source") {
      append(Seq((1, "spark"), (2, "hadoop"),
        (3, "hdfs"), (4, "hive"), (5, "dfs")).toDF("id", "name"))
      var deltaLog = DeltaLog.forTable(spark, tempPath)
      assert(!DeltaBloomFilter.checkBloomFilterIfExist(deltaLog),
        s"bloom filter should not exist, " +
            s"expect ${BLOOM_FILTER_LOCATION.fromMetaData(deltaLog.snapshot.metadata)}")

      val deltaTableV2: DeltaTableV2 = DeltaTableV2(spark, new Path(tempPath))
      AlterTableSetPropertiesDeltaCommand(deltaTableV2, getDeltaConf).run(spark)

      spark.read.format("delta").load(tempPath).repartition(2)
          .write
          .option("dataChange", "false")
          .format("delta")
          .mode("overwrite")
          .save(tempPath)

      checkDeltaBloomProperties()
      compareAndGetDeltaFilesAndBloomFiles()
    }
  }

  test("bloom filter config - scala API") {
    withTable("source") {
      withSQLConf(DeltaSQLConf.DELTA_BLOOM_FILTER_ENABLE.key -> "true"
        , DeltaSQLConf.DELTA_ID_COLS.key -> "id") {
        append(Seq((1, "spark"), (2, "hadoop"),
          (3, "hdfs"), (4, "hive"), (5, "dfs"), (6, "fs")).toDF("id", "name")
            .repartition(3))

        var deltaLog = DeltaLog.forTable(spark, tempPath)

        checkDeltaBloomProperties()
        compareAndGetDeltaFilesAndBloomFiles()


        var bfDF = getBloomFilterDF(deltaLog)
        logConsole(s"bloom filter file: ")
        bfDF.show(false)

        val df = readDeltaTable(tempPath)
            .withColumn(FILE_NAME, input_file_name)

        df.show(false)

        checkDeltaBloomProperties()
        assert(compareAndGetDeltaFilesAndBloomFiles() == 3
          , "file num should be 3 after repartition write")

        val source = Seq((1, "chc"), (3, ""), (7, "")).toDF("id", "name")  // source
        io.delta.tables.DeltaTable.forPath(spark, tempPath).as("t")
            .merge(source.as("s"), "s.id = t.id")
            .whenMatched("s.id = 2").delete()
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()

        deltaLog = DeltaLog.forTable(spark, tempPath)
        bfDF = getBloomFilterDF(deltaLog)
        logConsole(s"bloom filter file: ")
        bfDF.show(false)

        val dataFrame = readDeltaTable(tempPath)
            .withColumn(FILE_NAME, input_file_name())
        dataFrame.show(false)

        checkDeltaBloomProperties()
        assert(compareAndGetDeltaFilesAndBloomFiles() == 3
          , "file num should be balance with delete update insert")

        val sourceOnlyDelete = Seq((2, "ddd"), (5, ""), (7, "")).toDF("id", "name")
        io.delta.tables.DeltaTable.forPath(spark, tempPath).as("t")
            .merge(sourceOnlyDelete.as("s"), "s.id = t.id")
            .whenMatched().delete()
            .execute()

        deltaLog = DeltaLog.forTable(spark, tempPath)
        bfDF = getBloomFilterDF(deltaLog)
        logConsole(s"bloom filter file: ")
        bfDF.show(false)

        val dataFrameDeleted = readDeltaTable(tempPath)
            .withColumn(FILE_NAME, input_file_name())
        dataFrameDeleted.show(false)

        checkDeltaBloomProperties()
        assert(compareAndGetDeltaFilesAndBloomFiles() == 1
          , "file num should be decrease when delete")

        val sourceOnlyAdd = Seq((3, "ddd"), (5, ""), (4, "")).toDF("id", "name")
        io.delta.tables.DeltaTable.forPath(spark, tempPath).as("t")
            .merge(sourceOnlyAdd.as("s"), "s.id = t.id")
            .whenMatched().updateAll()
            .whenNotMatched().insertAll()
            .execute()

        deltaLog = DeltaLog.forTable(spark, tempPath)
        bfDF = getBloomFilterDF(deltaLog)
        logConsole(s"bloom filter file: ")
        bfDF.show(false)

        val dataFrameAdded = readDeltaTable(tempPath)
            .withColumn(FILE_NAME, input_file_name())
        dataFrameAdded.show(false)

        checkDeltaBloomProperties()
        assert(compareAndGetDeltaFilesAndBloomFiles() == 1
          , "data only append, file num should not be increase")

        checkAnswer(
          dataFrameAdded.drop(FILE_NAME),
          Seq(
            Row(3, "ddd")
            , Row(5, "")
            , Row(4, "")
            , Row(1, "chc")
            , Row(6, "fs")
          )
        )
      }
    }
  }
}
