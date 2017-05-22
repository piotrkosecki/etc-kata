package com.piotrkosecki

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{Broadcast, GraphDSL, RunnableGraph, Sink, Source}
import com.piotrkosecki.models.{ConsumptionLine, TimePeriod}
import com.piotrkosecki.services.FlowService._


object ConsumptionCalculation {

  implicit val system = ActorSystem("system")
  implicit val materializer = ActorMaterializer()

  def main(args: Array[String]): Unit = {

    val sensorId = UUID.fromString("b08c6195-8cd9-43ab-b94d-e0b887dd73d2")

    val firstTimePeriod = TimePeriod(0, 7)
    val secondTimePeriod = TimePeriod(8, 15)
    val thirdTimePeriod = TimePeriod(16, 23)

    val consumptionData: Iterator[String] = io.Source.fromResource("consumption_data.csv").getLines()

    val g = RunnableGraph.fromGraph(GraphDSL.create() { implicit builder =>

      import GraphDSL.Implicits._

      //Source
      val source: Outlet[String] = builder.add(Source.fromIterator(() => consumptionData)).out

      // Flows
      val parserFlow: FlowShape[String, ConsumptionLine] = builder.add(parse)

      val sensorFilterFlow: FlowShape[ConsumptionLine, ConsumptionLine] = builder.add(sensorFilter(sensorId))

      val broadcastFanOut: UniformFanOutShape[ConsumptionLine, ConsumptionLine] = builder.add(Broadcast[ConsumptionLine](4))

      val timeFilterFlow1: FlowShape[ConsumptionLine, ConsumptionLine] = builder.add(timeFilter(firstTimePeriod))

      val timeFilterFlow2: FlowShape[ConsumptionLine, ConsumptionLine] = builder.add(timeFilter(secondTimePeriod))

      val timeFilterFlow3: FlowShape[ConsumptionLine, ConsumptionLine] = builder.add(timeFilter(thirdTimePeriod))

      val sumFlow: FlowShape[ConsumptionLine, BigDecimal] = builder.add(sumConsumptions)

      //unfortunately I need to register separate flows for average calculation
      val avgFlow1: FlowShape[ConsumptionLine, BigDecimal] = builder.add(averageConsumption)

      val avgFlow2: FlowShape[ConsumptionLine, BigDecimal] = builder.add(averageConsumption)

      val avgFlow3: FlowShape[ConsumptionLine, BigDecimal] = builder.add(averageConsumption)

      //Sinks

      val avgSink: Inlet[BigDecimal] = builder.add(Sink.foreach(printSink("average"))).in
      val sum1Sink: Inlet[BigDecimal] = builder.add(Sink.foreach(printSink("first period"))).in
      val sum2Sink: Inlet[BigDecimal] = builder.add(Sink.foreach(printSink("second period"))).in
      val sum3Sink: Inlet[BigDecimal] = builder.add(Sink.foreach(printSink("third period"))).in

      // Graph

      source ~> parserFlow ~> sensorFilterFlow ~> broadcastFanOut
      sum1Sink <~ avgFlow1 <~ timeFilterFlow1 <~ broadcastFanOut
      sum2Sink <~ avgFlow2 <~ timeFilterFlow2 <~ broadcastFanOut
      sum3Sink <~ avgFlow3 <~ timeFilterFlow3 <~ broadcastFanOut
      avgSink <~ sumFlow <~ broadcastFanOut

      ClosedShape
    })

    g.run()

  }

}


