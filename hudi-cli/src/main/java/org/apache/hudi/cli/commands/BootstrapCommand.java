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

package org.apache.hudi.cli.commands;

import org.apache.hudi.avro.model.BootstrapIndexInfo;
import org.apache.hudi.cli.HoodieCLI;
import org.apache.hudi.cli.HoodiePrintHelper;
import org.apache.hudi.cli.TableHeader;
import org.apache.hudi.cli.commands.SparkMain.SparkCommand;
import org.apache.hudi.cli.utils.InputStreamConsumer;
import org.apache.hudi.cli.utils.SparkUtil;
import org.apache.hudi.common.bootstrap.index.BootstrapIndex;
import org.apache.hudi.common.model.BootstrapSourceFileMapping;
import org.apache.hudi.common.model.HoodieFileGroupId;
import org.apache.hudi.common.table.HoodieTableMetaClient;
import org.apache.hudi.utilities.UtilHelpers;

import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.launcher.SparkLauncher;
import org.apache.spark.util.Utils;
import org.springframework.shell.core.CommandMarker;
import org.springframework.shell.core.annotation.CliCommand;
import org.springframework.shell.core.annotation.CliOption;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

import scala.collection.JavaConverters;

/**
 * CLI command to perform bootstrap action & display bootstrap index.
 */
@Component
public class BootstrapCommand implements CommandMarker {

  private static final Logger LOG = LogManager.getLogger(BootstrapCommand.class);

  @CliCommand(value = "bootstrap", help = "Run a bootstrap action for current Hudi table")
  public String bootstrap(
      @CliOption(key = {"sourcePath"}, mandatory = true, help = "Source data path of the table") final String sourcePath,
      @CliOption(key = {"recordKeyColumns"}, mandatory = true, help = "Record key columns for bootstrap data") final String recordKeyCols,
      @CliOption(key = {"parallelism"}, unspecifiedDefaultValue = "1500", help = "Bootstrap writer parallelism") final int parallelism,
      @CliOption(key = {"selectorClass"}, unspecifiedDefaultValue = "org.apache.hudi.client.bootstrap.selector.MetadataOnlyBootstrapModeSelector",
          help = "Selector class for bootstrap") final String selectorClass,
      @CliOption(key = {"schema"}, unspecifiedDefaultValue = "", help = "Schema of the source data file") final String schema)
      throws IOException, InterruptedException, URISyntaxException {

    boolean initialized = HoodieCLI.initConf();
    HoodieCLI.initFS(initialized);

    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();

    String sparkPropertiesPath =
        Utils.getDefaultPropertiesFile(JavaConverters.mapAsScalaMapConverter(System.getenv()).asScala());

    SparkLauncher sparkLauncher = SparkUtil.initLauncher(sparkPropertiesPath);

    String cmd = SparkCommand.BOOTSTRAP.toString();

    sparkLauncher.addAppArgs(cmd, metaClient.getTableConfig().getTableName(), metaClient.getBasePath(), sourcePath, recordKeyCols, String.valueOf(parallelism), selectorClass, schema);
    UtilHelpers.validateAndAddProperties(new String[] {}, sparkLauncher);
    Process process = sparkLauncher.launch();
    InputStreamConsumer.captureOutput(process);
    int exitCode = process.waitFor();
    if (exitCode != 0) {
      return "Failed to import source data to hudi dataset";
    }
    return "Imported source data as hudi dataset";
  }

  @CliCommand(value = "show bootstrap index mapping", help = "Show bootstrap index mapping")
  public String showBootstrapIndexMapping(
      @CliOption(key = {"partitionPath"}, unspecifiedDefaultValue = "", help = "A valid paritition path") String partition,
      @CliOption(key = {"fileIds"}, unspecifiedDefaultValue = "", help = "Valid fileIds split by comma") String fileIds,
      @CliOption(key = {"limit"}, unspecifiedDefaultValue = "-1", help = "Limit rows to be displayed") Integer limit,
      @CliOption(key = {"sortBy"}, unspecifiedDefaultValue = "", help = "Sorting Field") final String sortByField,
      @CliOption(key = {"desc"}, unspecifiedDefaultValue = "false", help = "Ordering") final boolean descending,
      @CliOption(key = {"headeronly"}, unspecifiedDefaultValue = "false", help = "Print Header Only")
      final boolean headerOnly) {

    if (partition.isEmpty() && !fileIds.isEmpty()) {
      throw new IllegalStateException("Both paritionPath and fileIds are required");
    }
    BootstrapIndex.IndexReader indexReader = createBootstrapIndexReader();

    // TODO tmp solution because the indexedPartition name is not clean
    // List<String> indexedPartitions = indexReader.getIndexedPartitions();
    List<String> indexedPartitions = indexReader.getIndexedPartitions().stream()
        .map(p -> p.split("//")[0].substring(5)).collect(Collectors.toList());

    if (!partition.isEmpty() && !indexedPartitions.contains(partition)) {
      return partition + " is not an valid indexed partition";
    }

    List<BootstrapSourceFileMapping> mappingList = new ArrayList<>();
    if (!fileIds.isEmpty()) {
      List<HoodieFileGroupId> fileGroupIds = Arrays.stream(fileIds.split(","))
          .map(fileId -> new HoodieFileGroupId(partition, fileId)).collect(Collectors.toList());
      mappingList.addAll(indexReader.getSourceFileMappingForFileIds(fileGroupIds).values());
    } else if (!partition.isEmpty()) {
      mappingList.addAll(indexReader.getSourceFileMappingForPartition(partition));
    } else {
      for (String part : indexedPartitions) {
        mappingList.addAll(indexReader.getSourceFileMappingForPartition(part));
      }
    }

    final List<Comparable[]> rows = convertBootstrapSourceFileMapping(mappingList);
    final TableHeader header = new TableHeader()
        .addTableHeaderField("Hudi Partition")
        .addTableHeaderField("FileId")
        .addTableHeaderField("Source File Base Path")
        .addTableHeaderField("Source File Parition")
        .addTableHeaderField("Source File Path");

    return HoodiePrintHelper.print(header, new HashMap<>(), sortByField, descending,
        limit, headerOnly, rows);
  }

  @CliCommand(value = "show indexed partitions", help = "Show bootstrap indexed partitions")
  public String showIndexedPartitions() {

    BootstrapIndex.IndexReader indexReader = createBootstrapIndexReader();
    List<String> indexedPartitions = indexReader.getIndexedPartitions();

    String[] header = new String[] {"Indexed partitions"};
    String[][] rows = new String[indexedPartitions.size()][1];
    for (int i = 0; i < indexedPartitions.size(); i++) {
      rows[i][0] = indexedPartitions.get(i);
    }
    return HoodiePrintHelper.print(header, rows);
  }

  @CliCommand(value = "show bootstrap info", help = "Show bootstrap index info")
  public String showBootstrapIndexInfo() {

    BootstrapIndex.IndexReader indexReader = createBootstrapIndexReader();
    BootstrapIndexInfo indexInfo = indexReader.getIndexInfo();

    String[] header = new String[] {"Version", "Source Base Path", "Created Timestamp", "Number of keys"};
    String[][] rows = {{String.valueOf(indexInfo.getVersion()), indexInfo.getSourceBasePath(),
        String.valueOf(indexInfo.getCreatedTimestamp()), String.valueOf(indexInfo.getNumKeys())}};

    return HoodiePrintHelper.print(header, rows);
  }

  private BootstrapIndex.IndexReader createBootstrapIndexReader() {
    HoodieTableMetaClient metaClient = HoodieCLI.getTableMetaClient();
    BootstrapIndex index = BootstrapIndex.getBootstrapIndex(metaClient);
    if (!index.checkIndex()) {
      throw new IllegalStateException("This is not a bootstraped Hudi table. Don't have any index info");
    }
    return index.createReader();
  }

  private List<Comparable[]> convertBootstrapSourceFileMapping(List<BootstrapSourceFileMapping> mappingList) {
    final List<Comparable[]> rows = new ArrayList<>();
    for (BootstrapSourceFileMapping mapping : mappingList) {
      rows.add(new Comparable[] {mapping.getHudiPartitionPath(), mapping.getHudiFileId(),
          mapping.getSourceBasePath(), mapping.getSourcePartitionPath(), mapping.getSourceFileStatus().getPath().getUri()});
    }
    return rows;
  }
}
