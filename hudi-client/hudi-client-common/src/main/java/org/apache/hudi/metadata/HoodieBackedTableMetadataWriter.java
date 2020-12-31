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

package org.apache.hudi.metadata;

import org.apache.hudi.avro.model.HoodieCleanMetadata;
import org.apache.hudi.avro.model.HoodieCleanerPlan;
import org.apache.hudi.avro.model.HoodieMetadataRecord;
import org.apache.hudi.avro.model.HoodieRestoreMetadata;
import org.apache.hudi.avro.model.HoodieRollbackMetadata;
import org.apache.hudi.client.common.HoodieEngineContext;
import org.apache.hudi.common.config.HoodieMetadataConfig;
import org.apache.hudi.common.config.SerializableConfiguration;
import org.apache.hudi.common.fs.ConsistencyGuardConfig;
import org.apache.hudi.common.fs.FSUtils;
import org.apache.hudi.common.model.HoodieCleaningPolicy;
import org.apache.hudi.common.model.HoodieCommitMetadata;
import org.apache.hudi.common.model.HoodieFileFormat;
import org.apache.hudi.common.model.HoodiePartitionMetadata;
import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieTableType;
import org.apache.hudi.common.model.HoodieWriteStat;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.common.table.timeline.HoodieActiveTimeline;
import org.apache.hudi.common.table.timeline.HoodieInstant;
import org.apache.hudi.common.table.timeline.HoodieTimeline;
import org.apache.hudi.common.table.timeline.TimelineMetadataUtils;
import org.apache.hudi.common.table.timeline.versioning.TimelineLayoutVersion;
import org.apache.hudi.common.util.CleanerUtils;
import org.apache.hudi.common.util.HoodieTimer;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.common.util.ValidationUtils;
import org.apache.hudi.common.util.collection.Pair;
import org.apache.hudi.config.HoodieCompactionConfig;
import org.apache.hudi.config.HoodieMetricsConfig;
import org.apache.hudi.config.HoodieWriteConfig;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.exception.HoodieIOException;
import org.apache.hudi.exception.HoodieMetadataException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.hudi.metadata.HoodieTableMetadata.METADATA_TABLE_NAME_SUFFIX;
import static org.apache.hudi.metadata.HoodieTableMetadata.NON_PARTITIONED_NAME;
import static org.apache.hudi.metadata.HoodieTableMetadata.SOLO_COMMIT_TIMESTAMP;

/**
 * Writer implementation backed by an internal hudi table. Partition and file listing are saved within an internal MOR table
 * called Metadata Table. This table is created by listing files and partitions (first time)
 * and kept in sync using the instants on the main dataset.
 */
public abstract class HoodieBackedTableMetadataWriter implements HoodieTableMetadataWriter {

  private static final Logger LOG = LogManager.getLogger(HoodieBackedTableMetadataWriter.class);

  protected HoodieWriteConfig metadataWriteConfig;
  protected HoodieWriteConfig datasetWriteConfig;
  protected String tableName;

  protected HoodieBackedTableMetadata metadata;
  protected HoodieTableMetaClient metaClient;
  protected Option<HoodieMetadataMetrics> metrics;
  protected boolean enabled;
  protected SerializableConfiguration hadoopConf;
  protected final transient HoodieEngineContext engineContext;

  protected HoodieBackedTableMetadataWriter(Configuration hadoopConf, HoodieWriteConfig writeConfig, HoodieEngineContext engineContext) {
    this.datasetWriteConfig = writeConfig;
    this.engineContext = engineContext;
    this.hadoopConf = new SerializableConfiguration(hadoopConf);

    if (writeConfig.useFileListingMetadata()) {
      this.tableName = writeConfig.getTableName() + METADATA_TABLE_NAME_SUFFIX;
      this.metadataWriteConfig = createMetadataWriteConfig(writeConfig);
      enabled = true;

      // Inline compaction and auto clean is required as we dont expose this table outside
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isAutoClean(), "Cleaning is controlled internally for Metadata table.");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.isInlineCompaction(), "Compaction is controlled internally for metadata table.");
      // Metadata Table cannot have metadata listing turned on. (infinite loop, much?)
      ValidationUtils.checkArgument(this.metadataWriteConfig.shouldAutoCommit(), "Auto commit is required for Metadata Table");
      ValidationUtils.checkArgument(!this.metadataWriteConfig.useFileListingMetadata(), "File listing cannot be used for Metadata Table");

      initRegistry();
      HoodieTableMetaClient datasetMetaClient = new HoodieTableMetaClient(hadoopConf, datasetWriteConfig.getBasePath());
      initialize(engineContext, datasetMetaClient);
      if (enabled) {
        // (re) init the metadata for reading.
        initTableMetadata();

        // This is always called even in case the table was created for the first time. This is because
        // initFromFilesystem() does file listing and hence may take a long time during which some new updates
        // may have occurred on the table. Hence, calling this always ensures that the metadata is brought in sync
        // with the active timeline.
        HoodieTimer timer = new HoodieTimer().startTimer();
        syncFromInstants(datasetMetaClient);
        metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.SYNC_STR, timer.endTimer()));
      }
    } else {
      enabled = false;
      this.metrics = Option.empty();
    }
  }

  protected abstract void initRegistry();

  /**
   * Create a {@code HoodieWriteConfig} to use for the Metadata Table.
   *
   * @param writeConfig {@code HoodieWriteConfig} of the main dataset writer
   */
  private HoodieWriteConfig createMetadataWriteConfig(HoodieWriteConfig writeConfig) {
    int parallelism = writeConfig.getMetadataInsertParallelism();

    // Create the write config for the metadata table by borrowing options from the main write config.
    HoodieWriteConfig.Builder builder = HoodieWriteConfig.newBuilder()
        .withTimelineLayoutVersion(TimelineLayoutVersion.CURR_VERSION)
        .withConsistencyGuardConfig(ConsistencyGuardConfig.newBuilder()
            .withConsistencyCheckEnabled(writeConfig.getConsistencyGuardConfig().isConsistencyCheckEnabled())
            .withInitialConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getInitialConsistencyCheckIntervalMs())
            .withMaxConsistencyCheckIntervalMs(writeConfig.getConsistencyGuardConfig().getMaxConsistencyCheckIntervalMs())
            .withMaxConsistencyChecks(writeConfig.getConsistencyGuardConfig().getMaxConsistencyChecks())
            .build())
        .withMetadataConfig(HoodieMetadataConfig.newBuilder().enable(false).build())
        .withAutoCommit(true)
        .withAvroSchemaValidate(true)
        .withEmbeddedTimelineServerEnabled(false)
        .withAssumeDatePartitioning(false)
        .withPath(HoodieTableMetadata.getMetadataTableBasePath(writeConfig.getBasePath()))
        .withSchema(HoodieMetadataRecord.getClassSchema().toString())
        .forTable(tableName)
        .withCompactionConfig(HoodieCompactionConfig.newBuilder()
            .withAsyncClean(writeConfig.isMetadataAsyncClean())
            // we will trigger cleaning manually, to control the instant times
            .withAutoClean(false)
            .withCleanerParallelism(parallelism)
            .withCleanerPolicy(HoodieCleaningPolicy.KEEP_LATEST_COMMITS)
            .retainCommits(writeConfig.getMetadataCleanerCommitsRetained())
            .archiveCommitsWith(writeConfig.getMetadataMinCommitsToKeep(), writeConfig.getMetadataMaxCommitsToKeep())
            // we will trigger compaction manually, to control the instant times
            .withInlineCompaction(false)
            .withMaxNumDeltaCommitsBeforeCompaction(writeConfig.getMetadataCompactDeltaCommitMax()).build())
        .withParallelism(parallelism, parallelism)
        .withDeleteParallelism(parallelism)
        .withRollbackParallelism(parallelism)
        .withFinalizeWriteParallelism(parallelism);

    if (writeConfig.isMetricsOn()) {
      HoodieMetricsConfig.Builder metricsConfig = HoodieMetricsConfig.newBuilder()
          .withReporterType(writeConfig.getMetricsReporterType().toString())
          .withExecutorMetrics(writeConfig.isExecutorMetricsEnabled())
          .on(true);
      switch (writeConfig.getMetricsReporterType()) {
        case GRAPHITE:
          metricsConfig.onGraphitePort(writeConfig.getGraphiteServerPort())
              .toGraphiteHost(writeConfig.getGraphiteServerHost())
              .usePrefix(writeConfig.getGraphiteMetricPrefix());
          break;
        case JMX:
          metricsConfig.onJmxPort(writeConfig.getJmxPort())
              .toJmxHost(writeConfig.getJmxHost());
          break;
        case DATADOG:
          // TODO:
          break;
        case CONSOLE:
        case INMEMORY:
          break;
        default:
          throw new HoodieMetadataException("Unsupported Metrics Reporter type " + writeConfig.getMetricsReporterType());
      }

      builder.withMetricsConfig(metricsConfig.build());
    }

    return builder.build();
  }

  public HoodieWriteConfig getWriteConfig() {
    return metadataWriteConfig;
  }

  public HoodieTableMetadata metadata() {
    return metadata;
  }

  /**
   * Initialize the metadata table if it does not exist. Update the metadata to bring it in sync with the file system.
   *
   * This can happen in two ways:
   * 1. If the metadata table did not exist, then file and partition listing is used
   * 2. If the metadata table exists, the instants from active timeline are read in order and changes applied
   *
   * The above logic has been chosen because it is faster to perform #1 at scale rather than read all the Instants
   * which are large in size (AVRO or JSON encoded and not compressed) and incur considerable IO for de-serialization
   * and decoding.
   */
  protected abstract void initialize(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient);

  private void initTableMetadata() {
    this.metadata = new HoodieBackedTableMetadata(hadoopConf.get(), datasetWriteConfig.getBasePath(), datasetWriteConfig.getSpillableMapBasePath(),
        datasetWriteConfig.useFileListingMetadata(), datasetWriteConfig.getFileListingMetadataVerify(), false,
        datasetWriteConfig.shouldAssumeDatePartitioning());
    this.metaClient = metadata.getMetaClient();
  }

  protected void bootstrapIfNeeded(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) throws IOException {
    HoodieTimer timer = new HoodieTimer().startTimer();
    boolean exists = datasetMetaClient.getFs().exists(new Path(metadataWriteConfig.getBasePath(), HoodieTableMetaClient.METAFOLDER_NAME));
    if (!exists) {
      // Initialize for the first time by listing partitions and files directly from the file system
      bootstrapFromFilesystem(engineContext, datasetMetaClient);
      metrics.ifPresent(m -> m.updateMetrics(HoodieMetadataMetrics.INITIALIZE_STR, timer.endTimer()));
    }
  }

  /**
   * Initialize the Metadata Table by listing files and partitions from the file system.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void bootstrapFromFilesystem(HoodieEngineContext engineContext, HoodieTableMetaClient datasetMetaClient) throws IOException {
    ValidationUtils.checkState(enabled, "Metadata table cannot be initialized as it is not enabled");

    // If there is no commit on the dataset yet, use the SOLO_COMMIT_TIMESTAMP as the instant time for initial commit
    // Otherwise, we use the timestamp of the instant which does not have any non-completed instants before it.
    Option<HoodieInstant> latestInstant = Option.empty();
    boolean foundNonComplete = false;
    for (HoodieInstant instant : datasetMetaClient.getActiveTimeline().getInstants().collect(Collectors.toList())) {
      if (!instant.isCompleted()) {
        foundNonComplete = true;
      } else if (!foundNonComplete) {
        latestInstant = Option.of(instant);
      }
    }

    String createInstantTime = latestInstant.map(HoodieInstant::getTimestamp).orElse(SOLO_COMMIT_TIMESTAMP);
    LOG.info("Creating a new metadata table in " + metadataWriteConfig.getBasePath() + " at instant " + createInstantTime);

    HoodieTableMetaClient.initTableType(hadoopConf.get(), metadataWriteConfig.getBasePath(),
        HoodieTableType.MERGE_ON_READ, tableName, "archived", HoodieMetadataPayload.class.getName(),
        HoodieFileFormat.HFILE.toString());
    initTableMetadata();

    // List all partitions in the basePath of the containing dataset
    FileSystem fs = datasetMetaClient.getFs();
    FileSystemBackedTableMetadata fileSystemBackedTableMetadata = new FileSystemBackedTableMetadata(hadoopConf, datasetWriteConfig.getBasePath(),
        datasetWriteConfig.shouldAssumeDatePartitioning());
    List<String> partitions = fileSystemBackedTableMetadata.getAllPartitionPaths();
    LOG.info("Initializing metadata table by using file listings in " + partitions.size() + " partitions");

    // List all partitions in parallel and collect the files in them
    int parallelism =  Math.max(partitions.size(), 1);
    List<Pair<String, FileStatus[]>> partitionFileList = engineContext.map(partitions, partition -> {
      FileStatus[] statuses = fileSystemBackedTableMetadata.getAllFilesInPartition(new Path(datasetWriteConfig.getBasePath(), partition));
      return Pair.of(partition, statuses);
    }, parallelism);

    // Create a HoodieCommitMetadata with writeStats for all discovered files
    int[] stats = {0};
    HoodieCommitMetadata commitMetadata = new HoodieCommitMetadata();

    partitionFileList.forEach(t -> {
      final String partition = t.getKey();
      try {
        if (!fs.exists(new Path(datasetWriteConfig.getBasePath(), partition + Path.SEPARATOR + HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE))) {
          return;
        }
      } catch (IOException e) {
        throw new HoodieMetadataException("Failed to check partition " + partition, e);
      }

      // Filter the statuses to only include files which were created before or on createInstantTime
      Arrays.stream(t.getValue()).filter(status -> {
        String filename = status.getPath().getName();
        if (filename.equals(HoodiePartitionMetadata.HOODIE_PARTITION_METAFILE)) {
          return false;
        }
        if (HoodieTimeline.compareTimestamps(FSUtils.getCommitTime(filename), HoodieTimeline.GREATER_THAN,
            createInstantTime)) {
          return false;
        }
        return true;
      }).forEach(status -> {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPath(partition + Path.SEPARATOR + status.getPath().getName());
        writeStat.setPartitionPath(partition);
        writeStat.setTotalWriteBytes(status.getLen());
        commitMetadata.addWriteStat(partition, writeStat);
        stats[0] += 1;
      });

      // If the partition has no files then create a writeStat with no file path
      if (commitMetadata.getWriteStats(partition) == null) {
        HoodieWriteStat writeStat = new HoodieWriteStat();
        writeStat.setPartitionPath(partition);
        commitMetadata.addWriteStat(partition, writeStat);
      }
    });

    LOG.info("Committing " + partitionFileList.size() + " partitions and " + stats[0] + " files to metadata");
    update(commitMetadata, createInstantTime);
  }

  /**
   * Sync the Metadata Table from the instants created on the dataset.
   *
   * @param datasetMetaClient {@code HoodieTableMetaClient} for the dataset
   */
  private void syncFromInstants(HoodieTableMetaClient datasetMetaClient) {
    ValidationUtils.checkState(enabled, "Metadata table cannot be synced as it is not enabled");

    try {
      List<HoodieInstant> instantsToSync = metadata.findInstantsToSync(datasetMetaClient);
      if (instantsToSync.isEmpty()) {
        return;
      }

      LOG.info("Syncing " + instantsToSync.size() + " instants to metadata table: " + instantsToSync);

      // Read each instant in order and sync it to metadata table
      final HoodieActiveTimeline timeline = datasetMetaClient.getActiveTimeline();
      for (HoodieInstant instant : instantsToSync) {
        LOG.info("Syncing instant " + instant + " to metadata table");
        ValidationUtils.checkArgument(instant.isCompleted(), "Only completed instants can be synced.");

        switch (instant.getAction()) {
          case HoodieTimeline.CLEAN_ACTION:
            HoodieCleanMetadata cleanMetadata = CleanerUtils.getCleanerMetadata(datasetMetaClient, instant);
            update(cleanMetadata, instant.getTimestamp());
            break;
          case HoodieTimeline.DELTA_COMMIT_ACTION:
          case HoodieTimeline.COMMIT_ACTION:
          case HoodieTimeline.COMPACTION_ACTION:
            HoodieCommitMetadata commitMetadata = HoodieCommitMetadata.fromBytes(
                timeline.getInstantDetails(instant).get(), HoodieCommitMetadata.class);
            update(commitMetadata, instant.getTimestamp());
            break;
          case HoodieTimeline.ROLLBACK_ACTION:
            HoodieRollbackMetadata rollbackMetadata = TimelineMetadataUtils.deserializeHoodieRollbackMetadata(
                timeline.getInstantDetails(instant).get());
            update(rollbackMetadata, instant.getTimestamp());
            break;
          case HoodieTimeline.RESTORE_ACTION:
            HoodieRestoreMetadata restoreMetadata = TimelineMetadataUtils.deserializeHoodieRestoreMetadata(
                timeline.getInstantDetails(instant).get());
            update(restoreMetadata, instant.getTimestamp());
            break;
          case HoodieTimeline.SAVEPOINT_ACTION:
            // Nothing to be done here
            break;
          default:
            throw new HoodieException("Unknown type of action " + instant.getAction());
        }
      }
      // re-init the table metadata, for any future writes.
      initTableMetadata();
    } catch (IOException ioe) {
      throw new HoodieIOException("Unable to sync instants from data to metadata table.", ioe);
    }
  }

  /**
   * Update from {@code HoodieCommitMetadata}.
   *
   * @param commitMetadata {@code HoodieCommitMetadata}
   * @param instantTime Timestamp at which the commit was performed
   */
  @Override
  public void update(HoodieCommitMetadata commitMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = new LinkedList<>();
    List<String> allPartitions = new LinkedList<>();
    commitMetadata.getPartitionToWriteStats().forEach((partitionStatName, writeStats) -> {
      final String partition = partitionStatName.equals("") ? NON_PARTITIONED_NAME : partitionStatName;
      allPartitions.add(partition);

      Map<String, Long> newFiles = new HashMap<>(writeStats.size());
      writeStats.forEach(hoodieWriteStat -> {
        String pathWithPartition = hoodieWriteStat.getPath();
        if (pathWithPartition == null) {
          throw new HoodieMetadataException("Unable to find path in write stat to update metadata table " + hoodieWriteStat);
        }

        int offset = partition.equals(NON_PARTITIONED_NAME) ? 0 : partition.length() + 1;
        String filename = pathWithPartition.substring(offset);
        ValidationUtils.checkState(!newFiles.containsKey(filename), "Duplicate files in HoodieCommitMetadata");
        newFiles.put(filename, hoodieWriteStat.getTotalWriteBytes());
      });

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(
          partition, Option.of(newFiles), Option.empty());
      records.add(record);
    });

    // New partitions created
    HoodieRecord record = HoodieMetadataPayload.createPartitionListRecord(new ArrayList<>(allPartitions));
    records.add(record);

    LOG.info("Updating at " + instantTime + " from Commit/" + commitMetadata.getOperationType()
        + ". #partitions_updated=" + records.size());
    commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime);
  }

  /**
   * Update from {@code HoodieCleanerPlan}.
   *
   * @param cleanerPlan {@code HoodieCleanerPlan}
   * @param instantTime Timestamp at which the clean plan was generated
   */
  @Override
  public void update(HoodieCleanerPlan cleanerPlan, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};
    cleanerPlan.getFilePathsToBeDeletedPerPartition().forEach((partition, deletedPathInfo) -> {
      fileDeleteCount[0] += deletedPathInfo.size();

      // Files deleted from a partition
      List<String> deletedFilenames = deletedPathInfo.stream().map(p -> new Path(p.getFilePath()).getName())
          .collect(Collectors.toList());
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(deletedFilenames));
      records.add(record);
    });

    LOG.info("Updating at " + instantTime + " from CleanerPlan. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime);
  }

  /**
   * Update from {@code HoodieCleanMetadata}.
   *
   * @param cleanMetadata {@code HoodieCleanMetadata}
   * @param instantTime Timestamp at which the clean was completed
   */
  @Override
  public void update(HoodieCleanMetadata cleanMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    List<HoodieRecord> records = new LinkedList<>();
    int[] fileDeleteCount = {0};

    cleanMetadata.getPartitionMetadata().forEach((partition, partitionMetadata) -> {
      // Files deleted from a partition
      List<String> deletedFiles = partitionMetadata.getSuccessDeleteFiles();
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.empty(),
          Option.of(new ArrayList<>(deletedFiles)));

      records.add(record);
      fileDeleteCount[0] += deletedFiles.size();
    });

    LOG.info("Updating at " + instantTime + " from Clean. #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileDeleteCount[0]);
    commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime);
  }

  /**
   * Update from {@code HoodieRestoreMetadata}.
   *
   * @param restoreMetadata {@code HoodieRestoreMetadata}
   * @param instantTime Timestamp at which the restore was performed
   */
  @Override
  public void update(HoodieRestoreMetadata restoreMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    restoreMetadata.getHoodieRestoreMetadata().values().forEach(rms -> {
      rms.forEach(rm -> processRollbackMetadata(rm, partitionToDeletedFiles, partitionToAppendedFiles));
    });
    commitRollback(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Restore");
  }

  /**
   * Update from {@code HoodieRollbackMetadata}.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param instantTime Timestamp at which the rollback was performed
   */
  @Override
  public void update(HoodieRollbackMetadata rollbackMetadata, String instantTime) {
    if (!enabled) {
      return;
    }

    Map<String, Map<String, Long>> partitionToAppendedFiles = new HashMap<>();
    Map<String, List<String>> partitionToDeletedFiles = new HashMap<>();
    processRollbackMetadata(rollbackMetadata, partitionToDeletedFiles, partitionToAppendedFiles);
    commitRollback(partitionToDeletedFiles, partitionToAppendedFiles, instantTime, "Rollback");
  }

  /**
   * Extracts information about the deleted and append files from the {@code HoodieRollbackMetadata}.
   *
   * During a rollback files may be deleted (COW, MOR) or rollback blocks be appended (MOR only) to files. This
   * function will extract this change file for each partition.
   *
   * @param rollbackMetadata {@code HoodieRollbackMetadata}
   * @param partitionToDeletedFiles The {@code Map} to fill with files deleted per partition.
   * @param partitionToAppendedFiles The {@code Map} to fill with files appended per partition and their sizes.
   */
  private void processRollbackMetadata(HoodieRollbackMetadata rollbackMetadata,
                                       Map<String, List<String>> partitionToDeletedFiles,
                                       Map<String, Map<String, Long>> partitionToAppendedFiles) {
    rollbackMetadata.getPartitionMetadata().values().forEach(pm -> {
      final String partition = pm.getPartitionPath();

      if (!pm.getSuccessDeleteFiles().isEmpty()) {
        if (!partitionToDeletedFiles.containsKey(partition)) {
          partitionToDeletedFiles.put(partition, new ArrayList<>());
        }

        // Extract deleted file name from the absolute paths saved in getSuccessDeleteFiles()
        List<String> deletedFiles = pm.getSuccessDeleteFiles().stream().map(p -> new Path(p).getName())
            .collect(Collectors.toList());
        partitionToDeletedFiles.get(partition).addAll(deletedFiles);
      }

      if (!pm.getAppendFiles().isEmpty()) {
        if (!partitionToAppendedFiles.containsKey(partition)) {
          partitionToAppendedFiles.put(partition, new HashMap<>());
        }

        // Extract appended file name from the absolute paths saved in getAppendFiles()
        pm.getAppendFiles().forEach((path, size) -> {
          partitionToAppendedFiles.get(partition).merge(new Path(path).getName(), size, (oldSize, newSizeCopy) -> {
            return size + oldSize;
          });
        });
      }
    });
  }

  /**
   * Create file delete records and commit.
   *
   * @param partitionToDeletedFiles {@code Map} of partitions and the deleted files
   * @param instantTime Timestamp at which the deletes took place
   * @param operation Type of the operation which caused the files to be deleted
   */
  private void commitRollback(Map<String, List<String>> partitionToDeletedFiles,
                              Map<String, Map<String, Long>> partitionToAppendedFiles, String instantTime,
                              String operation) {
    List<HoodieRecord> records = new LinkedList<>();
    int[] fileChangeCount = {0, 0}; // deletes, appends

    partitionToDeletedFiles.forEach((partition, deletedFiles) -> {
      // Rollbacks deletes instants from timeline. The instant being rolled-back may not have been synced to the
      // metadata table. Hence, the deleted filed need to be checked against the metadata.
      try {
        FileStatus[] existingStatuses = metadata.fetchAllFilesInPartition(new Path(metadata.getDatasetBasePath(), partition));
        Set<String> currentFiles =
            Arrays.stream(existingStatuses).map(s -> s.getPath().getName()).collect(Collectors.toSet());

        int origCount = deletedFiles.size();
        deletedFiles.removeIf(f -> !currentFiles.contains(f));
        if (deletedFiles.size() != origCount) {
          LOG.warn("Some Files to be deleted as part of " + operation + " at " + instantTime + " were not found in the "
              + " metadata for partition " + partition
              + ". To delete = " + origCount + ", found=" + deletedFiles.size());
        }

        fileChangeCount[0] += deletedFiles.size();

        Option<Map<String, Long>> filesAdded = Option.empty();
        if (partitionToAppendedFiles.containsKey(partition)) {
          filesAdded = Option.of(partitionToAppendedFiles.remove(partition));
        }

        HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, filesAdded,
            Option.of(new ArrayList<>(deletedFiles)));
        records.add(record);
      } catch (IOException e) {
        throw new HoodieMetadataException("Failed to commit rollback deletes at instant " + instantTime, e);
      }
    });

    partitionToAppendedFiles.forEach((partition, appendedFileMap) -> {
      fileChangeCount[1] += appendedFileMap.size();

      // Validate that no appended file has been deleted
      ValidationUtils.checkState(
          !appendedFileMap.keySet().removeAll(partitionToDeletedFiles.getOrDefault(partition, Collections.emptyList())),
          "Rollback file cannot both be appended and deleted");

      // New files added to a partition
      HoodieRecord record = HoodieMetadataPayload.createPartitionFilesRecord(partition, Option.of(appendedFileMap),
          Option.empty());
      records.add(record);
    });

    LOG.info("Updating at " + instantTime + " from " + operation + ". #partitions_updated=" + records.size()
        + ", #files_deleted=" + fileChangeCount[0] + ", #files_appended=" + fileChangeCount[1]);
    commit(records, MetadataPartitionType.FILES.partitionPath(), instantTime);
  }

  /**
   * Commit the {@code HoodieRecord}s to Metadata Table as a new delta-commit.
   *
   */
  protected abstract void commit(List<HoodieRecord> records, String partitionName, String instantTime);
}