/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network;

import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionProvider;
import org.apache.flink.runtime.io.network.partition.ResultSubpartitionView;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel.BufferAndAvailability;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannelID;

import javax.annotation.Nullable;

import java.io.IOException;

/**
 * Simple wrapper for the partition readerQueue iterator, which increments a sequence number for
 * each returned buffer and remembers the receiver ID.
 */
public interface NetworkSequenceViewReader {

    void requestSubpartitionView(
            ResultPartitionProvider partitionProvider,
            ResultPartitionID resultPartitionId,
            int subPartitionIndex)
            throws IOException;

    @Nullable
    BufferAndAvailability getNextBuffer() throws IOException;

    /** Returns true if the producer backlog need to be announced to the consumer. */
    boolean needAnnounceBacklog();

    /**
     * The credits from consumer are added in incremental way.
     *
     * @param creditDeltas The credit deltas
     */
    void addCredit(int creditDeltas);

    /**
     * Notify the id of required segment from consumer.
     *
     * @param segmentId The id of required segment.
     */
    void notifyRequiredSegmentId(int segmentId);

    /** Resumes data consumption after an exactly once checkpoint. */
    void resumeConsumption();

    /** Acknowledges all the user records are processed. */
    void acknowledgeAllRecordsProcessed();

    /**
     * Checks whether this reader is available or not and returns the backlog at the same time.
     *
     * @return A boolean flag indicating whether the reader is available together with the backlog.
     */
    ResultSubpartitionView.AvailabilityWithBacklog getAvailabilityAndBacklog();

    boolean isRegisteredAsAvailable();

    /**
     * Updates the value to indicate whether the reader is enqueued in the pipeline or not.
     *
     * @param isRegisteredAvailable True if this reader is already enqueued in the pipeline.
     */
    void setRegisteredAsAvailable(boolean isRegisteredAvailable);

    boolean isReleased();

    void releaseAllResources() throws IOException;

    Throwable getFailureCause();

    InputChannelID getReceiverId();

    void notifyNewBufferSize(int newBufferSize);
}
