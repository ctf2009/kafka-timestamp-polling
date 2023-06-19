package com.ctf.consumer.processing

import org.apache.kafka.clients.consumer.Consumer
import org.apache.kafka.common.TopicPartition
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.KafkaAdmin
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.LocalDateTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.concurrent.locks.ReentrantLock

@Service
class KafkaOffsetProcessor(
    consumerFactory: ConsumerFactory<String, String>, private val adminClient: KafkaAdmin
) {

    val consumer: Consumer<String, String> = consumerFactory.createConsumer()
    val lock: ReentrantLock = ReentrantLock()

    fun processOffsets(topicName: String, from: LocalDateTime, to: LocalDateTime? = null): List<String> {
        val returnedMessages = mutableListOf<String>()

        try {
            lock.lock()

            val fromMillis = getMillisFromLocalDateTime(from)
            val toMillis = getMillisFromLocalDateTime(to)

            val topicPartitions = getTopicPartitionsForTopic(topicName, consumer)
            val earliestOffsetsToSearch = getEarliestOffsetsToSearch(topicPartitions, fromMillis!!, consumer)
            val latestOffsetsToSearch = getLatestOffsetsToSearch(topicPartitions, toMillis, consumer)

            // Need to check for each partition if the earliest and latest are the same. If they are we filter out
            val filteredPartitions = topicPartitions.filter {
                earliestOffsetsToSearch[it] != latestOffsetsToSearch[it]
            }

            // Consume
            filteredPartitions.forEach {
                consumer.assign(listOf(it))
                val earliest = earliestOffsetsToSearch[it]!!
                val latest = latestOffsetsToSearch[it]!!

                consumer.seek(it, earliest)
                while (consumer.position(it) < latest) {
                    val results = consumer.poll(Duration.ofSeconds(5))
                    results.forEach recordLoop@{ record ->
                        println("Record timestamp is ${record.timestamp()}")

                        if (!recordWithinBounds(record.timestamp(), fromMillis, toMillis)) {
                            println("Record is not within bounds")
                            return@recordLoop
                        }

                        returnedMessages.add(record.value())
                    }
                }
            }

            return returnedMessages
        } finally {
            lock.unlock()
        }
    }

    private fun getEarliestOffsetsToSearch(
        topicPartitions: List<TopicPartition>, fromMillis: Long, consumer: Consumer<String, String>
    ): Map<TopicPartition, Long> {
        val earliestOffsets = consumer.beginningOffsets(topicPartitions)
        println("From Millis = $fromMillis")
        val result = consumer.offsetsForTimes(topicPartitions.associateWith { fromMillis })
        return result.mapValues { it.value?.offset() ?: earliestOffsets[it.key]!! }
    }

    private fun getLatestOffsetsToSearch(
        topicPartitions: List<TopicPartition>, toMillis: Long?, consumer: Consumer<String, String>
    ): Map<TopicPartition, Long> {
        val latestOffsets = consumer.endOffsets(topicPartitions)
        toMillis?.let {
            val result = consumer.offsetsForTimes(topicPartitions.associateWith { toMillis })
            return result.mapValues { it.value?.offset() ?: latestOffsets[it.key]!! }
        } ?: run {
            println("To DateTime not provided, defaulting to latest offsets per partition")
            return latestOffsets
        }
    }

    private fun getTopicPartitionsForTopic(topic: String, consumer: Consumer<String, String>) =
        consumer.partitionsFor(topic).map { TopicPartition(it.topic(), it.partition()) }

    private fun getMillisFromLocalDateTime(localDateTime: LocalDateTime?) =
        localDateTime?.let { ZonedDateTime.of(localDateTime, ZoneId.systemDefault()).toInstant().toEpochMilli() }

    private fun recordTimeBelowToDate(recordTimestampMillis: Long, toMillis: Long?): Boolean =
        toMillis?.let { recordTimestampMillis <= it } ?: true

    private fun recordWithinBounds(recordTimestampMillis: Long, fromMillis: Long, toMillis: Long?) =
        (recordTimestampMillis >= fromMillis) and recordTimeBelowToDate(recordTimestampMillis, toMillis)
}


