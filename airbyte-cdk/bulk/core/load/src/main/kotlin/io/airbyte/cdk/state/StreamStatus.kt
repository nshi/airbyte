package io.airbyte.cdk.state

import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong

data class StreamStatus(
    val recordsPublished: AtomicLong = AtomicLong(0),
    val recordsConsumed: AtomicLong = AtomicLong(0),
    val totalBytes: AtomicLong = AtomicLong(0),
    val enqueuedSize: AtomicLong = AtomicLong(0),
    val publishComplete: AtomicBoolean = AtomicBoolean(false),
    val consumptionComplete: AtomicBoolean = AtomicBoolean(false),
    val lastCheckpoint: AtomicLong = AtomicLong(0L),
    val closed: AtomicBoolean = AtomicBoolean(false),
)
