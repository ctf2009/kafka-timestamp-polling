package com.ctf.consumer

import com.ctf.consumer.processing.KafkaOffsetProcessor
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.producer.ProducerRecord
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.boot.test.context.TestConfiguration
import org.springframework.context.ApplicationContext
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Import
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.test.context.EmbeddedKafka
import org.springframework.test.annotation.DirtiesContext
import org.springframework.test.context.ActiveProfiles
import org.springframework.util.Assert
import java.time.Instant
import java.time.ZoneId
import java.time.ZonedDateTime

private const val TOPIC_NAME = "test-topic"

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.DEFINED_PORT)
@Import(ConsumerPollingApplicationTests.KafkaTestTopicConfiguration::class)
@ActiveProfiles("test")
@EmbeddedKafka
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
class ConsumerPollingApplicationTests {

    @Autowired
    private lateinit var context: ApplicationContext

    @Autowired
    private lateinit var processor: KafkaOffsetProcessor

    @Autowired
    private lateinit var kafkaTemplate: KafkaTemplate<String, String>

    @Test
    fun contextLoads() {
        println(context)
    }

    @Test
    fun `Return empty list when topic is completely empty with from date only`() {
        val messageDateTime = ZonedDateTime.of(2023, 6, 10, 7, 30, 0, 0, ZoneId.systemDefault())
        val searchFromDateTime = messageDateTime.minusDays(20)

        val result = processor.processOffsets(
            topicName = TOPIC_NAME,
            from = searchFromDateTime.toLocalDateTime()
        )

        Assert.isTrue(result.isEmpty(), "Returned result list should be empty")
    }

    @Test
    fun `Return empty list when topic is completely empty when to date is also provided`() {
        val messageDateTime = ZonedDateTime.of(2023, 6, 10, 7, 30, 0, 0, ZoneId.systemDefault())
        val searchFromDateTime = messageDateTime.minusDays(20)
        val searchToDateTime = messageDateTime.plusDays(10)

        val result = processor.processOffsets(
            topicName = TOPIC_NAME,
            from = searchFromDateTime.toLocalDateTime(),
            to = searchToDateTime.toLocalDateTime()
        )

        Assert.isTrue(result.isEmpty(), "Returned result list should be empty")
    }

    @Test
    fun `Process with from date before first message timestamp returns record`() {
        val messageDateTime = ZonedDateTime.of(2023, 6, 10, 7, 30, 0, 0, ZoneId.systemDefault())
        val searchFromDateTime = messageDateTime.minusDays(20)

        println("Message Date Time = $messageDateTime")
        println("Search From Date Time = $searchFromDateTime")

        givenProducerSendsMessage(TOPIC_NAME, "Test Message Should Return", messageDateTime.toInstant())

        val result = processor.processOffsets(
            topicName = TOPIC_NAME,
            from = searchFromDateTime.toLocalDateTime()
        )

        Assert.isTrue(result.isNotEmpty(), "Returned result list should not be empty")
    }

    @Test
    fun `Process with from date after first message timestamp does not return`() {
        val messageDateTime = ZonedDateTime.of(2023, 6, 10, 7, 30, 0, 0, ZoneId.systemDefault())
        val searchFromDateTime = messageDateTime.plusDays(20)

        println("Message Date Time = $messageDateTime")
        println("Search From Date Time = $searchFromDateTime")

        givenProducerSendsMessage(TOPIC_NAME, "Test Message Should Not Return", messageDateTime.toInstant())

        val result = processor.processOffsets(
            topicName = TOPIC_NAME,
            from = searchFromDateTime.toLocalDateTime()
        )

        Assert.isTrue(result.isEmpty(), "Returned result list should be empty")
    }

    @Test
    fun `Process with from and to date only returns a subset of results`() {
        val messageOneDateTime = ZonedDateTime.of(2023, 6, 10, 7, 30, 0, 0, ZoneId.systemDefault())
        val messageTwoDateTime = messageOneDateTime.plusDays(2)
        val messageThreeDateTime = messageOneDateTime.plusDays(5)
        val messageFourDateTime = messageOneDateTime.plusDays(6)

        val searchFromDateTime = messageOneDateTime.minusDays(1)
        val searchToDateTime = messageOneDateTime.plusDays(3)

        givenProducerSendsMessage(TOPIC_NAME, "Test Message 1 Should Return", messageOneDateTime.toInstant())
        givenProducerSendsMessage(TOPIC_NAME, "Test Message 2 Should Return", messageTwoDateTime.toInstant())
        givenProducerSendsMessage(TOPIC_NAME, "Test Message 3 Should Not Return", messageThreeDateTime.toInstant())
        givenProducerSendsMessage(TOPIC_NAME, "Test Message 4 Should Not Return", messageFourDateTime.toInstant())

        val result = processor.processOffsets(
            topicName = TOPIC_NAME,
            from = searchFromDateTime.toLocalDateTime(),
            to = searchToDateTime.toLocalDateTime()
        )

        Assert.isTrue(result.size == 2, "Returned result list should only contain 2 results")
    }

    @Test
    fun `Process with from and to date where the first offset is not 0`() {
        val messageOneDateTime = ZonedDateTime.of(2023, 6, 10, 7, 30, 0, 0, ZoneId.systemDefault())
        val messageTwoDateTime = messageOneDateTime.plusDays(2)
        val messageThreeDateTime = messageOneDateTime.plusDays(5)
        val messageFourDateTime = messageOneDateTime.plusDays(7)

        val searchFromDateTime = messageOneDateTime.plusDays(1)
        val searchToDateTime = messageOneDateTime.plusDays(6)

        givenProducerSendsMessage(TOPIC_NAME, "Test Message 1 Should Not Return", messageOneDateTime.toInstant())
        givenProducerSendsMessage(TOPIC_NAME, "Test Message 2 Should Return", messageTwoDateTime.toInstant())
        givenProducerSendsMessage(TOPIC_NAME, "Test Message 3 Should Return", messageThreeDateTime.toInstant())
        givenProducerSendsMessage(TOPIC_NAME, "Test Message 4 Should Not Return", messageFourDateTime.toInstant())

        val result = processor.processOffsets(
            topicName = TOPIC_NAME,
            from = searchFromDateTime.toLocalDateTime(),
            to = searchToDateTime.toLocalDateTime()
        )

        Assert.isTrue(result.size == 2, "Returned result list should only contain 2 results")
    }

    private fun givenProducerSendsMessage(topic: String, message: String, instant: Instant) {
        println("Instant = $instant, Millis = ${instant.toEpochMilli()} \n\n")
        val record = ProducerRecord<String, String>(topic, 0, instant.toEpochMilli(), null, message)
        kafkaTemplate.send(record).get()
    }

    @TestConfiguration
    class KafkaTestTopicConfiguration() {

        @Bean
        fun topic(): NewTopic {
            return NewTopic(TOPIC_NAME, 1, 1).configs(
                mutableMapOf(
                    "message.timestamp.type" to "CreateTime "
                )
            )
        }
    }
}
