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

import java.time.Instant

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hudi.common.HoodieTestDataGenerator
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.table.timeline.HoodieTimeline
import org.apache.hudi.config.HoodieWriteConfig
import org.apache.hudi.{DataSourceReadOptions, DataSourceWriteOptions, HoodieDataSourceHelpers}
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.streaming.{OutputMode, ProcessingTime}
import org.junit.jupiter.api.Assertions.{assertEquals, assertTrue}
import org.junit.jupiter.api.io.TempDir
import org.junit.jupiter.api.{BeforeEach, Test}

import scala.collection.JavaConversions._
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration.Duration
import scala.concurrent.{Await, Future}

/**
 * Basic tests on the spark datasource
 */
class TestDataSource {

  var spark: SparkSession = null
  var dataGen: HoodieTestDataGenerator = null
  val commonOpts = Map(
    "hoodie.insert.shuffle.parallelism" -> "4",
    "hoodie.upsert.shuffle.parallelism" -> "4",
    DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY -> "_row_key",
    DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY -> "partition",
    DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY -> "timestamp",
    HoodieWriteConfig.TABLE_NAME -> "hoodie_test"
  )
  var basePath: String = null
  var srcPath: String = null
  var fs: FileSystem = null

  @BeforeEach def initialize(@TempDir tempDir: java.nio.file.Path) {
    spark = SparkSession.builder
      .appName("Hoodie Datasource test")
      .master("local[2]")
      .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .getOrCreate
    dataGen = new HoodieTestDataGenerator()
    basePath = tempDir.toAbsolutePath.toString + "/base"
    srcPath = tempDir.toAbsolutePath.toString + "/src"
    fs = FSUtils.getFs(basePath, spark.sparkContext.hadoopConfiguration)
  }

  @Test def testShortNameStorage() {
    // Insert Operation
    val records = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records, 2))
    inputDF.write.format("hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
  }

  @Test def testFullBootstrapCOWNonHiveStylePartitioned(): Unit = {
    val timestamp = Instant.now.toEpochMilli
    val numRecords = 100
    val partitionPaths = List("2020-04-01", "2020-04-02", "2020-04-03")
    val jsc = JavaSparkContext.fromSparkContext(spark.sparkContext)

    var sourceDF = HoodieTestDataGenerator.generateTestRawTripDataset(timestamp, numRecords, partitionPaths, jsc,
      spark.sqlContext)
    sourceDF = sourceDF.drop("tip_history")

    // Writing data for each partition instead of using partitionBy to avoid hive-style partitioning and hence
    // have partitioned columns stored in the data file
    partitionPaths.foreach(partitionPath => {
      sourceDF
        .filter(sourceDF("datestr").equalTo(lit(partitionPath)))
        .write
        .format("parquet")
        .mode(SaveMode.Overwrite)
        .save(srcPath + "/" + partitionPath)
    })

    // Perform bootstrap
    val bootstrapDF = spark.emptyDataFrame
    bootstrapDF.write
      .format("hudi")
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.BOOTSTRAP_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(HoodieWriteConfig.SOURCE_BASE_PATH_PROP, srcPath)
      .option(HoodieWriteConfig.BOOTSTRAP_KEYGEN_CLASS, "org.apache.hudi.keygen.SimpleKeyGenerator")
      .option(HoodieWriteConfig.BOOTSTRAP_MODE_SELECTOR, "org.apache.hudi.client.bootstrap.selector.FullBootstrapModeSelector")
      .option(HoodieWriteConfig.FULL_BOOTSTRAP_INPUT_PROVIDER,"org.apache.hudi.bootstrap.SparkDataSourceBasedFullBootstrapInputProvider")
      .mode(SaveMode.Overwrite)
      .save(basePath)

    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(HoodieTimeline.FULL_BOOTSTRAP_INSTANT_TS, commitInstantTime1)

    // Read bootstrapped table and verify count
    val hoodieROViewDF1 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF1.count())

    // Perform upsert
    val updateTimestamp = Instant.now.toEpochMilli
    val numRecordsUpdate = 10
    var updateDF = HoodieTestDataGenerator.generateTestRawTripDataset(updateTimestamp, numRecordsUpdate, partitionPaths, jsc,
      spark.sqlContext)
    updateDF = updateDF.drop("tip_history")

    updateDF.write
      .format("hudi")
      .option(HoodieWriteConfig.TABLE_NAME, "hoodie_test")
      .option("hoodie.upsert.shuffle.parallelism", "4")
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.UPSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.STORAGE_TYPE_OPT_KEY, DataSourceWriteOptions.COW_STORAGE_TYPE_OPT_VAL)
      .option(DataSourceWriteOptions.RECORDKEY_FIELD_OPT_KEY, "_row_key")
      .option(DataSourceWriteOptions.PRECOMBINE_FIELD_OPT_KEY, "timestamp")
      .option(DataSourceWriteOptions.PARTITIONPATH_FIELD_OPT_KEY, "datestr")
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(1, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, commitInstantTime1).size())

    // Read table after upsert and verify count
    val hoodieROViewDF2 = spark.read.format("hudi").load(basePath + "/*")
    assertEquals(numRecords, hoodieROViewDF2.count())
    assertEquals(numRecordsUpdate, hoodieROViewDF2.filter(s"timestamp == $updateTimestamp").count())

    // incrementally pull only changes in the bootstrap commit, which would pull all the initial records written
    // during bootstrap
    val hoodieIncViewDF1 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)

    assertEquals(numRecords, hoodieIncViewDF1.count())
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime1, countsPerCommit(0).get(0))

    // incrementally pull only changes in the latest commit, which would pull only the updated records in the
    // latest commit
    val hoodieIncViewDF2 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(numRecordsUpdate, hoodieIncViewDF2.count())
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/2020-04-02/*")
      .load(basePath)

    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2020-04-02")).count(),
      hoodieIncViewDF3.count())
  }

  @Test def testCopyOnWriteStorage() {
    // Insert Operation
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))
    val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)

    // Read RO View
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*");
    assertEquals(100, hoodieROViewDF1.count())

    val records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("001", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    // Upsert Operation
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .mode(SaveMode.Append)
      .save(basePath)

    val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").size())

    // Read RO View
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*");
    assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated

    // Read Incremental View
    // we have 2 commits, try pulling the first commit (which is not the latest)
    val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, basePath, "000").get(0);
    val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
      .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
      .load(basePath);
    assertEquals(100, hoodieIncViewDF1.count()) // 100 initial inserts must be pulled
    var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(firstCommit, countsPerCommit(0).get(0))

    // pull the latest commit
    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath);

    assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
    countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect();
    assertEquals(1, countsPerCommit.length)
    assertEquals(commitInstantTime2, countsPerCommit(0).get(0))

    // pull the latest commit within certain partitions
    val hoodieIncViewDF3 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .option(DataSourceReadOptions.INCR_PATH_GLOB_OPT_KEY, "/2016/*/*/*")
      .load(basePath);
    assertEquals(hoodieIncViewDF2.filter(col("_hoodie_partition_path").contains("2016")).count(), hoodieIncViewDF3.count())
  }

  @Test def testMergeOnReadStorage() {
    // Bulk Insert Operation
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("001", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option("hoodie.compact.inline", "false") // else fails due to compaction & deltacommit instant times being same
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .option(DataSourceWriteOptions.TABLE_TYPE_OPT_KEY, DataSourceWriteOptions.MOR_TABLE_TYPE_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)

    assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, basePath, "000"))

    // Read RO View
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi").load(basePath + "/*/*/*/*")
    assertEquals(100, hoodieROViewDF1.count()) // still 100, since we only updated
  }

  @Test def testDropInsertDup(): Unit = {
    val insert1Cnt = 10
    val insert2DupKeyCnt = 9
    val insert2NewKeyCnt = 2

    val totalUniqueKeyToGenerate = insert1Cnt + insert2NewKeyCnt
    val allRecords =  dataGen.generateInserts("001", totalUniqueKeyToGenerate)
    val inserts1 = allRecords.subList(0, insert1Cnt)
    val inserts2New = dataGen.generateSameKeyInserts("002", allRecords.subList(insert1Cnt, insert1Cnt + insert2NewKeyCnt))
    val inserts2Dup = dataGen.generateSameKeyInserts("002", inserts1.subList(0, insert2DupKeyCnt))

    val records1 = DataSourceTestUtils.convertToStringList(inserts1).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))
    inputDF1.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.OPERATION_OPT_KEY, DataSourceWriteOptions.INSERT_OPERATION_OPT_VAL)
      .mode(SaveMode.Overwrite)
      .save(basePath)
    val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(insert1Cnt, hoodieROViewDF1.count())

    val commitInstantTime1 = HoodieDataSourceHelpers.latestCommit(fs, basePath)
    val records2 = DataSourceTestUtils
      .convertToStringList(inserts2Dup ++ inserts2New)
      .toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    inputDF2.write.format("org.apache.hudi")
      .options(commonOpts)
      .option(DataSourceWriteOptions.INSERT_DROP_DUPS_OPT_KEY, "true")
      .mode(SaveMode.Append)
      .save(basePath)
    val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
      .load(basePath + "/*/*/*/*")
    assertEquals(hoodieROViewDF2.count(), totalUniqueKeyToGenerate)

    val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
      .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
      .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
      .load(basePath)
    assertEquals(hoodieIncViewDF2.count(), insert2NewKeyCnt)
  }

  //@Test (TODO: re-enable after fixing noisyness)
  def testStructuredStreaming(): Unit = {
    fs.delete(new Path(basePath), true)
    val sourcePath = basePath + "/source"
    val destPath = basePath + "/dest"
    fs.mkdirs(new Path(sourcePath))

    // First chunk of data
    val records1 = DataSourceTestUtils.convertToStringList(dataGen.generateInserts("000", 100)).toList
    val inputDF1: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records1, 2))

    // Second chunk of data
    val records2 = DataSourceTestUtils.convertToStringList(dataGen.generateUpdates("001", 100)).toList
    val inputDF2: Dataset[Row] = spark.read.json(spark.sparkContext.parallelize(records2, 2))
    val uniqueKeyCnt = inputDF2.select("_row_key").distinct().count()

    // define the source of streaming
    val streamingInput =
      spark.readStream
      .schema(inputDF1.schema)
      .json(sourcePath)

    val f1 = Future {
      println("streaming starting")
    //'writeStream' can be called only on streaming Dataset/DataFrame
      streamingInput
        .writeStream
        .format("org.apache.hudi")
        .options(commonOpts)
        .trigger(new ProcessingTime(100))
        .option("checkpointLocation", basePath + "/checkpoint")
        .outputMode(OutputMode.Append)
        .start(destPath)
        .awaitTermination(10000)
      println("streaming ends")
    }

    val f2 = Future {
      inputDF1.write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      Thread.sleep(3000)
      assertTrue(HoodieDataSourceHelpers.hasNewCommits(fs, destPath, "000"))
      val commitInstantTime1: String = HoodieDataSourceHelpers.latestCommit(fs, destPath)
      // Read RO View
      val hoodieROViewDF1 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assert(hoodieROViewDF1.count() == 100)

      inputDF2.write.mode(SaveMode.Append).json(sourcePath)
      // wait for spark streaming to process one microbatch
      Thread.sleep(10000)
      val commitInstantTime2: String = HoodieDataSourceHelpers.latestCommit(fs, destPath)

      assertEquals(2, HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").size())
      // Read RO View
      val hoodieROViewDF2 = spark.read.format("org.apache.hudi")
        .load(destPath + "/*/*/*/*")
      assertEquals(100, hoodieROViewDF2.count()) // still 100, since we only updated


      // Read Incremental View
      // we have 2 commits, try pulling the first commit (which is not the latest)
      val firstCommit = HoodieDataSourceHelpers.listCommitsSince(fs, destPath, "000").get(0)
      val hoodieIncViewDF1 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, "000")
        .option(DataSourceReadOptions.END_INSTANTTIME_OPT_KEY, firstCommit)
        .load(destPath)
      assertEquals(100, hoodieIncViewDF1.count())
      // 100 initial inserts must be pulled
      var countsPerCommit = hoodieIncViewDF1.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(firstCommit, countsPerCommit(0).get(0))

      // pull the latest commit
      val hoodieIncViewDF2 = spark.read.format("org.apache.hudi")
        .option(DataSourceReadOptions.QUERY_TYPE_OPT_KEY, DataSourceReadOptions.QUERY_TYPE_INCREMENTAL_OPT_VAL)
        .option(DataSourceReadOptions.BEGIN_INSTANTTIME_OPT_KEY, commitInstantTime1)
        .load(destPath)

      assertEquals(uniqueKeyCnt, hoodieIncViewDF2.count()) // 100 records must be pulled
      countsPerCommit = hoodieIncViewDF2.groupBy("_hoodie_commit_time").count().collect()
      assertEquals(1, countsPerCommit.length)
      assertEquals(commitInstantTime2, countsPerCommit(0).get(0))
    }

    Await.result(Future.sequence(Seq(f1, f2)), Duration.Inf)

  }
}
