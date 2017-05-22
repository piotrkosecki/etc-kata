package com.piotrkosecki.services

import java.util.UUID

import akka.NotUsed
import akka.stream.scaladsl.Flow
import com.piotrkosecki.models.{ConsumptionLine, TimePeriod}
import org.joda.time.DateTime

import scala.util.Try


object FlowService {
  def parse: Flow[String, ConsumptionLine, NotUsed] = Flow[String].map { line =>
    Try {
      val split = line.split(",")
      ConsumptionLine(UUID.fromString(split(0)), DateTime.parse(split(1)), BigDecimal(split(2)))
    }
  }.filter(_.isSuccess).map(_.get)

  def sensorFilter(id: UUID): Flow[ConsumptionLine, ConsumptionLine, NotUsed] = Flow[ConsumptionLine].filter(_.sensorId == id)

  def timeFilter(timePeriod: TimePeriod): Flow[ConsumptionLine, ConsumptionLine, NotUsed] = Flow[ConsumptionLine].filter { consumptionLine =>
    val time = consumptionLine.timestamp.toDateTime.toLocalTime
    time.getHourOfDay >= timePeriod.from && time.getHourOfDay <= timePeriod.to
  }

  def sumConsumptions: Flow[ConsumptionLine, BigDecimal, NotUsed] = Flow[ConsumptionLine].fold(BigDecimal(0))(_ + _.consumption)

  def averageConsumption: Flow[ConsumptionLine, BigDecimal, NotUsed] = Flow[ConsumptionLine].fold((BigDecimal(0), 0)) {
    case ((acc, counter), curr) =>
      (acc + curr.consumption, counter + 1)
  }.map {
    case (sum, count) => sum / count
  }

  def printSink(msg: String)(result: BigDecimal): Unit = println(msg + ": \t" + result / 1000 + " kWh")

}
