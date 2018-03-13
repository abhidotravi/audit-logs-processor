package com.mapr.audit

case class ProcessorConfig(cluster: String,
                           stream: String,
                           broker: String,
                           group: String,
                           keyDeserializer: String,
                           valueDeserializer: String,
                           offsetReset: String,
                           pollTimeout: String,
                           batchInterval: String)
