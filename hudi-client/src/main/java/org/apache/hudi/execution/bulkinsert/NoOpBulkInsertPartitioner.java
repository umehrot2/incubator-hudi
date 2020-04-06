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

package org.apache.hudi.execution.bulkinsert;

import org.apache.hudi.common.model.HoodieRecord;
import org.apache.hudi.common.model.HoodieRecordPayload;
import org.apache.hudi.exception.HoodieException;
import org.apache.hudi.table.UserDefinedBulkInsertPartitioner;
import org.apache.spark.api.java.JavaRDD;

/**
 * NoOpBulkInsertPartitioner returns the input JavaRDD as it is.
 * In order to use NoOpBulkInsertPartitioner,
 * input DataFrame should be repartitioned to the same number of hoodie.bulkinsert.shuffle.parallelism
 * or at least smaller than hoodie.bulkinsert.shuffle.parallelism
 *
 * e.g)
 * For Partitioned table
 *   val parallelism = options.get("hoodie.bulkinsert.shuffle.parallelism")
 *   val partitionKey = options.get("hoodie.datasource.write.partitionpath.field")
 *   inputDataFrame.repartition(parallelism, col(partitionKey)).write.format("org.apache.hudi").options(options).save(path)
 * For Non-Partitioned table
 *   inputDataFrame.repartition(parallelism).write.format("org.apache.hudi").options(options).save(path)
 */
public final class NoOpBulkInsertPartitioner<T extends HoodieRecordPayload> implements UserDefinedBulkInsertPartitioner<T> {

  @Override
  public JavaRDD<HoodieRecord<T>> repartitionRecords(JavaRDD<HoodieRecord<T>> records, int outputSparkPartitions) {
    final int inputSparkPartitions = records.partitions().size();
    if (inputSparkPartitions > outputSparkPartitions) {
      throw new HoodieException("the number of the records partitions (" + inputSparkPartitions
          + ") should be equal or smaller than outputSparkPartitions (" + outputSparkPartitions + ")");
    }
    return records;
  }
}
