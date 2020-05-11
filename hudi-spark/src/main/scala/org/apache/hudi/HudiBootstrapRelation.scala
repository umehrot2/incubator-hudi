/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hudi

import org.apache.hadoop.fs.Path
import org.apache.hudi.common.fs.FSUtils
import org.apache.hudi.common.model.{HoodieBaseFile, HoodieRecord}
import org.apache.hudi.common.table.HoodieTableMetaClient
import org.apache.hudi.common.table.view.HoodieTableFileSystemView
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.{FileStatusCache, InMemoryFileIndex, PartitionedFile}
import org.apache.spark.sql.execution.datasources.parquet.ParquetFileFormat
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.sources.{BaseRelation, Filter, PrunedFilteredScan}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

import scala.collection.JavaConverters._

class HudiBootstrapRelation(@transient val _sqlContext: SQLContext,
                            val userSchema: StructType,
                            val globPaths: Seq[Path],
                            val metaClient: HoodieTableMetaClient,
                            val optParams: Map[String, String]) extends BaseRelation
  with PrunedFilteredScan with Logging {

  val fileIndex: HudiBootstrapFileIndex = buildFileIndex()

  val skeletonSchema: StructType = StructType(Seq(
    StructField(HoodieRecord.COMMIT_TIME_METADATA_FIELD, StringType, nullable = true),
    StructField(HoodieRecord.COMMIT_SEQNO_METADATA_FIELD, StringType, nullable = true),
    StructField(HoodieRecord.RECORD_KEY_METADATA_FIELD, StringType, nullable = true),
    StructField(HoodieRecord.PARTITION_PATH_METADATA_FIELD, StringType, nullable = true),
    StructField(HoodieRecord.FILENAME_METADATA_FIELD, StringType, nullable = true)
  ))

  var dataSchema: StructType = _

  var completeSchema: StructType = _

  override def sqlContext: SQLContext = _sqlContext

  override val needConversion: Boolean = false

  override def schema: StructType = {
    if (completeSchema == null) {
      inferFullSchema()
    }
    completeSchema
  }

  /**
    * Implementing PrunedScan to support column pruning, by reading only the required columns from the parquet files
    * instead by passing them down to the ParquetFileFormat.
    *
    * TODO: To get better performance with Filters we should implement PrunedFilteredScan push filters down to the
    * parquet files. But this is much more tricky to implement because then with filters being pushed down, unequal
    * number od rows may be returned by external data reader, and skeleton file readers. Merging in this scenario
    * will become much more complicated.
    *
    * @param requiredColumns This contains the columns user has passed in select() or filter() operations on the
    *                        dataframe
    * @return
    */
  override def buildScan(requiredColumns: Array[String], filters: Array[Filter]): RDD[Row] = {
    logInfo("Starting scan..")
    filters.foreach(filter => logInfo("Obtained filter: " + filter.references.mkString(",") + " "
      + filter.getClass))

    // Compute splits
    val bootstrapSplits = fileIndex.files.map(hoodieBaseFile => {
      var skeletonFile: Option[PartitionedFile] = Option.empty
      var dataFile: PartitionedFile = null

      if (hoodieBaseFile.getExternalBaseFile.isPresent) {
        skeletonFile = Option(PartitionedFile(InternalRow.empty, hoodieBaseFile.getPath, 0, hoodieBaseFile.getFileLen))
        dataFile = PartitionedFile(InternalRow.empty, hoodieBaseFile.getExternalBaseFile.get().getPath, 0,
          hoodieBaseFile.getExternalBaseFile.get().getFileLen)
      } else {
        dataFile = PartitionedFile(InternalRow.empty, hoodieBaseFile.getPath, 0, hoodieBaseFile.getFileLen)
      }
      HudiBootstrapSplit(dataFile, skeletonFile)
    })
    val tableState = HudiBootstrapTableState(bootstrapSplits)

    // Get required schemas for column pruning
    val requiredDataSchema = StructType(dataSchema.filter(field => requiredColumns.contains(field.name)))
    val requiredSkeletonSchema = StructType(skeletonSchema.filter(field => requiredColumns.contains(field.name)))
    val requiredRegularSchema = StructType(requiredColumns.map(col => {
      completeSchema.find(_.name == col).get
    }))

    // Prepare readers for reading data file and skeleton files
    val dataReadFunction = new ParquetFileFormat()
        .buildReaderWithPartitionValues(
          sparkSession = _sqlContext.sparkSession,
          dataSchema = dataSchema,
          partitionSchema = StructType(Seq.empty),
          requiredSchema = requiredDataSchema,
          filters = Nil,
          options = Map.empty,
          hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
        )

    val skeletonReadFunction = new ParquetFileFormat()
      .buildReaderWithPartitionValues(
        sparkSession = _sqlContext.sparkSession,
        dataSchema = skeletonSchema,
        partitionSchema = StructType(Seq.empty),
        requiredSchema = requiredSkeletonSchema,
        filters = Nil,
        options = Map.empty,
        hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf()
      )

    val regularReadFunction = new ParquetFileFormat()
      .buildReaderWithPartitionValues(
        sparkSession = _sqlContext.sparkSession,
        dataSchema = completeSchema,
        partitionSchema = StructType(Seq.empty),
        requiredSchema = requiredRegularSchema,
        filters = filters,
        options = Map.empty,
        hadoopConf = _sqlContext.sparkSession.sessionState.newHadoopConf())

    val rdd = new HudiBootstrapRDD(_sqlContext.sparkSession, dataReadFunction, skeletonReadFunction,
      regularReadFunction, requiredDataSchema, requiredSkeletonSchema, requiredColumns, tableState)

    logInfo("Number of partitions for HudiBootstrapRDD => " + rdd.partitions.length)
    rdd.asInstanceOf[RDD[Row]]
  }

  def inferFullSchema(): StructType = {
    logInfo("Inferring schema..")

    // We need to infer schema from the external data files and then merge the skeleton schema which is fixed
    // to get the complete schema
    val fs = FSUtils.getFs(globPaths.head.toString, _sqlContext.sparkContext.hadoopConfiguration)

    val headFile = fileIndex.files.head
    if (headFile.getExternalBaseFile.isPresent) {
      // Get the data schema from external file and merge with skeleton schema
      val externalFileStatus = fs.listStatus(new Path(headFile.getExternalBaseFile.get().getPath))
      val inferredDataSchema = new ParquetFileFormat().inferSchema(
        _sqlContext.sparkSession,
        optParams,
        externalFileStatus
      )

      logInfo("Inferred schema from external file => " + inferredDataSchema.get.toString())
      dataSchema = inferredDataSchema.get
      completeSchema = StructType(skeletonSchema.fields ++ dataSchema.fields)
      logInfo("Data schema => " + dataSchema.toString())
      logInfo("Complete schema => " + completeSchema.toString())
    } else {
      // Get the merged schema from regular file and filter out the skeleton fields to get just data schema
      val regularFileStatus = Array(headFile.getFileStatus)
      val inferredDataSchema = new ParquetFileFormat().inferSchema(
        _sqlContext.sparkSession,
        optParams,
        regularFileStatus
      )

      logInfo("Inferred schema from regular file => " + inferredDataSchema.get.toString())
      completeSchema = inferredDataSchema.get
      dataSchema = StructType(completeSchema.filterNot(field => skeletonSchema.fieldNames.contains(field.name)))
      logInfo("Data schema => " + dataSchema.toString())
      logInfo("Complete schema => " + completeSchema.toString())
    }
    completeSchema
  }

  def buildFileIndex(): HudiBootstrapFileIndex = {
    logInfo("Building file index..")
    val inMemoryFileIndex = createInMemoryFileIndex(globPaths)
    val fileStatuses = inMemoryFileIndex.allFiles()

    if (fileStatuses.isEmpty) {
      throw new RuntimeException("No files found for reading.")
    }

    val fsView = new HoodieTableFileSystemView(metaClient, metaClient.getActiveTimeline.getCommitsTimeline
      .filterCompletedInstants, fileStatuses.toArray)
    val latestFiles: List[HoodieBaseFile] = fsView.getLatestBaseFiles.iterator().asScala.toList
    latestFiles.foreach(file => logInfo("Skeleton file path: " + file.getPath))
    latestFiles.filter(_.getExternalBaseFile.isPresent).foreach(file => {
      logInfo("External data file path: " + file.getExternalBaseFile.get().getPath)
    })

    HudiBootstrapFileIndex(latestFiles)
  }

  private def createInMemoryFileIndex(globbedPaths: Seq[Path]): InMemoryFileIndex = {
    val fileStatusCache = FileStatusCache.getOrCreate(_sqlContext.sparkSession)
    new InMemoryFileIndex(_sqlContext.sparkSession, globbedPaths, Map(), Option.empty, fileStatusCache)
  }
}

case class HudiBootstrapFileIndex(files: List[HoodieBaseFile])

case class HudiBootstrapTableState(files: List[HudiBootstrapSplit])

case class HudiBootstrapSplit(dataFile: PartitionedFile, skeletonFile: Option[PartitionedFile])
