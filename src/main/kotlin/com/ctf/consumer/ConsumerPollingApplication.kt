package com.ctf.consumer

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class ConsumerPollingApplication

fun main(args: Array<String>) {
	runApplication<ConsumerPollingApplication>(*args)
}
