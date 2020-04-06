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
import org.apache.spark.Partition;
import org.apache.spark.api.java.JavaRDD;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class TestNoOpBulkInsertPartitioner {

  @Mock JavaRDD<HoodieRecord<HoodieRecordPayload>> inputRdd;
  @Mock List<Partition> inputPartitions;

  @Test
  public void noOpBulkInsertPartitionerShouldReturnsSameInputRdd() {
    final int numberOfPartitions = 100;
    mockInputRdd(numberOfPartitions);
    NoOpBulkInsertPartitioner<HoodieRecordPayload> partitioner = new NoOpBulkInsertPartitioner<>();
    JavaRDD<HoodieRecord<HoodieRecordPayload>> outputRdd = partitioner.repartitionRecords(inputRdd, numberOfPartitions);

    assertThat(outputRdd, is(equalTo(inputRdd)));
  }

  @Test
  public void noOpBulkInsertPartitionerShouldWorkWhenInputHasSmallerPartitionsThanOutputPartitions() {
    mockInputRdd(10);
    NoOpBulkInsertPartitioner<HoodieRecordPayload> partitioner = new NoOpBulkInsertPartitioner<>();
    JavaRDD<HoodieRecord<HoodieRecordPayload>> outputRdd = partitioner.repartitionRecords(inputRdd, 100);

    assertThat(outputRdd, is(equalTo(inputRdd)));
  }

  @Test(expected = HoodieException.class)
  public void noOpBulkInsertPartitionerShouldFailWhenInputHasLargerPartitionsThanOutputPartitions() {
    mockInputRdd(101);
    NoOpBulkInsertPartitioner<HoodieRecordPayload> partitioner = new NoOpBulkInsertPartitioner<>();
    partitioner.repartitionRecords(inputRdd, 100);
  }

  private void mockInputRdd(final int numberOfPartitions) {
    when(inputPartitions.size()).thenReturn(numberOfPartitions);
    when(inputRdd.partitions()).thenReturn(inputPartitions);
  }
}
