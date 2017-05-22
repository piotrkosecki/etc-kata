package services

import java.util.UUID

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import com.piotrkosecki.models.{ConsumptionLine, TimePeriod}
import com.piotrkosecki.services.FlowService
import org.joda.time.DateTime
import org.scalatest.concurrent.ScalaFutures
import org.scalatest.{Matchers, WordSpecLike}

import scala.concurrent.Future


class ConsumptionCalculationSpec extends WordSpecLike with Matchers with ScalaFutures {

  trait Context {

    // implicit actor system
    implicit val system = ActorSystem("system")

    // implicit actor materializer
    implicit val materializer = ActorMaterializer()

    val sensorUuid = UUID.fromString("7dd8f7f3-95e3-4b14-bd8c-1b9eb7056d7a")

    val threeOclock = DateTime.parse("2017-03-30T03:00:00Z")

    val nineOclock = DateTime.parse("2017-03-30T09:00:00Z")

    val seventeenOclock = DateTime.parse("2017-03-30T17:00:00Z")

    val sevenToEleven = TimePeriod(7, 11)

    val exampleLine = ConsumptionLine(sensorUuid, threeOclock, 50)

  }

  "A stream" should {

    "use only correct lines" in new Context {
      val exampleCsv: String =
        """
          |aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa,2017-03-30T03:00:00Z,50
          |7dd8f7f3-95e3-4b14-bd8c-1b9eb7056d7a,2017-03-30T03:00:00Z,50
          |7dd8f7f3-95e3-4b14-bd8c-1b9eb7056d7a,asdasdasdasdasdasd,33
          |7dd8f7f3-95e3-4b14-bd8c-1b9eb7056d7a,2017-03-30T03:00:00Z,asdasdasd
        """.stripMargin

      val result: Future[List[ConsumptionLine]] = Source
        .fromIterator(() => exampleCsv.split("\n").toIterator)
        .via(FlowService.parse)
        .toMat(Sink.fold(List[ConsumptionLine]()) { case (xs, x) => x :: xs })(Keep.right)
        .run()


      result.futureValue.headOption should be(Some(exampleLine))
      result.futureValue.size should be(1)

    }

    "use only data from correct sensor" in new Context {
      val result: Future[List[ConsumptionLine]] = Source
        .fromIterator(() =>
          Iterator(
            exampleLine,
            exampleLine.copy(sensorId = UUID.fromString("5ae4913d-c415-4753-90de-14b8b3c792e3")),
            exampleLine.copy(sensorId = UUID.fromString("9d892b7c-ec67-43ac-a15c-4a76b3d3fc8b"))
          ))
        .via(FlowService.sensorFilter(sensorUuid))
        .toMat(Sink.fold(List[ConsumptionLine]()) { case (xs, x) => x :: xs })(Keep.right)
        .run()

      result.futureValue.forall(_.sensorId == sensorUuid) should be(true)
    }

    "filter data from correct hours" in new Context {
      val result: Future[List[ConsumptionLine]] = Source
        .fromIterator(() =>
          Iterator(
            exampleLine,
            exampleLine.copy(timestamp = nineOclock),
            exampleLine.copy(timestamp = seventeenOclock)
          ))
        .via(FlowService.timeFilter(sevenToEleven))
        .toMat(Sink.fold(List[ConsumptionLine]()) { case (xs, x) => x :: xs })(Keep.right)
        .run()

      result.futureValue.headOption should be(Some(exampleLine.copy(timestamp = nineOclock)))

      result.futureValue.size should be(1)

    }


    "sum correctly" in new Context {
      val result: Future[BigDecimal] = Source
        .fromIterator(() =>
          Iterator(
            exampleLine,
            exampleLine,
            exampleLine
          ))
        .via(FlowService.sumConsumptions)
        .toMat(Sink.fold(BigDecimal(0))(_ + _))(Keep.right)
        .run()

      result.futureValue should be(BigDecimal(150))

    }

    "calculate average correctly" in new Context {
      val result: Future[BigDecimal] = Source
        .fromIterator(() =>
          Iterator(
            exampleLine.copy(consumption = BigDecimal(33)),
            exampleLine.copy(consumption = BigDecimal(66)),
            exampleLine.copy(consumption = BigDecimal(99))
          ))
        .via(FlowService.averageConsumption)
        .toMat(Sink.fold(BigDecimal(0))(_ + _))(Keep.right)
        .run()

      result.futureValue should be(BigDecimal(66))

    }

  }


}
