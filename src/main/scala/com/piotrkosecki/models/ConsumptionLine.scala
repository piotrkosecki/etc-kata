package com.piotrkosecki.models

import java.util.UUID

import org.joda.time.DateTime


case class ConsumptionLine(sensorId: UUID, timestamp: DateTime, consumption: BigDecimal) {

  assert(consumption >= 0)

}
