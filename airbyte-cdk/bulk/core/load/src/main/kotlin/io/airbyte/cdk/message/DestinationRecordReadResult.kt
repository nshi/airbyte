package io.airbyte.cdk.message

import io.airbyte.cdk.util.ShardedIndex

sealed class DestinationRecordReadResult

data class IndexedDestinationRecord(
    val index: ShardedIndex,
    val sizeBytes: Long,
    val record: DestinationRecord,
): DestinationRecordReadResult()

data class EndOfStream(
    val index: ShardedIndex,
): DestinationRecordReadResult()

data object Timeout: DestinationRecordReadResult()

data object EndOfChunk: DestinationRecordReadResult()
