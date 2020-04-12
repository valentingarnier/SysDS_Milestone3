import scala.io.Source
import scala.util.parsing.combinator.RegexParsers

case class AppAttempt(id: String, user: String, startTime: String, endTime: String, containers: List[(String, String)], errorCategory: Int, exceptionClass: String, stage: Int, sourceCodeLine: Int)


object Milestone3Parser extends RegexParsers {

  def attemptId = "appattempt_1580812675067_\\d+_\\d+".r

  def int = "-?\\d+".r ^^ (_.toInt)

  def user = "[a-z]+".r

  def host = "iccluster\\d+.iccluster.epfl.ch".r 

  def time: Parser[String] = raw"(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3})".r

  def containerHostList = repsep( container ~ ("->" ~> host), ",") ^^ {_.map{case c ~ h => c -> h}}

  def container = "container_e02_1580812675067_\\d+_\\d+_\\d+".r

  def exception = "([a-zA-Z_$][a-zA-Z\\d_$]*\\.)*[a-zA-Z_$][a-zA-Z\\d_$]*".r


  override val skipWhitespace = true

  def appAttempt =
    "AppAttempt" ~ ":" ~> attemptId ~
      ("User" ~ ":" ~> user) ~ 
      ("StartTime" ~ ":" ~> time) ~
      ("EndTime" ~ ":" ~> time) ~
      ("Containers" ~ ":" ~> containerHostList) ~
      ("ErrorCategory" ~ ":" ~> int) ~
      ("Exception" ~ ":" ~> ("N/A" | exception)) ~
      ("Stage" ~ ":"  ~> int) ~
      ("SourceCodeLine" ~ ":" ~> int) ^^ { case id ~ name ~ st ~ et ~  cl ~ ec ~ ex ~ stage ~ scl   => AppAttempt(id, name, st, et, cl, ec.toInt, ex, stage.toInt, scl.toInt) }

  def m3 = rep(appAttempt)

  def handleError(s: String, msg: String, line: Int, col: Int): Unit = {
    val faultyLine = s.split("\n")(line - 1)
    val after = faultyLine.substring(col - 1)
    val before = faultyLine.take(col - 1)
    println(s"Error $msg in while parsing line $line at column $col :: " + before + "^" + after)
  }

  def apply(s: String) = parseAll(m3, s) match {
    case Success(result, _) => Some(result)
    case Failure(msg, next) => handleError(s, msg, next.pos.line, next.pos.column); None
    case Error(msg, next) => handleError(s, msg, next.pos.line, next.pos.column); None

  }

  def main(args: Array[String]) = {

    if (args.length == 0) {
      System.err.println("Provide path to answers.txt file as argument")
    } else {
      println("\n" + args(0))
      val input = Source.fromFile(args(0)).getLines().mkString("\n")
      val res = apply(input)
      if (res.isDefined) {
        println("SUCCESS")
       }
    }
  }
}
