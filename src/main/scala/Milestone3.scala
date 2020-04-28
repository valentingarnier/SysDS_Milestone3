import org.apache.spark
import org.apache.spark.{SparkConf, SparkContext}

import scala.collection.immutable.Stream.Empty

object Milestone3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Milestone3")
    val sc = SparkContext.getOrCreate(conf)

    val resourceManager = args(0)
    val appLogs = args(1)
    val startId = args(2).toInt
    val endId = args(3).toInt


    val lines = sc.textFile(resourceManager)

    /*
    Extract infos from the log:
    After an analysis of the log, I've noticed 4 regex that would capture the most important informations
    in the log.
    * PatternStartTimeAttempt serves to capture the start time of each application attempt
    * userAppId uses the keywords java:activateApplications(911) and application_1580812675067_() to find
      the link between each applications and users in the right timestamp
    * Similarly, containers info contains keywords FiCaSchedulerNode.java:allocateContainer(169) then i simply
      Regex the line and capture important informations such as attempt, id, containerID and host
     */

    val patternStartTimeAttempt = """(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}).+appattempt_1580812675067_(\d+)_0000(\d+).+from\sALLOCATED\sto\sLAUNCHED.+""".r
    val userAppId = """(.+java:activateApplications\(911\)\)).+application_1580812675067_(\d+)\sfrom\suser:\s(\w+).+""".r
    val container = """(.+FiCaSchedulerNode.java:allocateContainer\(169\)\)).+container_e02_1580812675067_(\d+)_(\d+)_(\d+).+host.(\w+.\w+.\w+.\w+).+""".r
    val endDateStatus = """(\d{4}-\d{2}-\d{2}\s\d{2}:\d{2}:\d{2},\d{3}).+appattempt_1580812675067_(\d+)_0000(\d+).+from\sFINAL_SAVING\sto\s(\w+).+""".r
    val exitCode = """(.+\(RMAppAttemptImpl.java:rememberTargetTransitionsAndStoreState\(1412\)\)).+appattempt_1580812675067_(\d+)_0000(\d+).+exit status: (-?\d+)""".r

    /*
    These functions just check if a line match previous regex, and extract tuples so that we end up with RDD of tuples
    */
    def parseLogContainers(line: String): (Int, Int, Int, String) = {
      val container(logMessage, appId, attempt, containerID, host) = line
      (appId.toInt, attempt.toInt, containerID.toInt, host)
    }
    def parseLogStart(line: String): (Int, Int, String) = {
      val patternStartTimeAttempt(date, appId, attempt) = line
      (appId.toInt, attempt.toInt, date)
    }
    def parseLogEnd(line: String): (Int, Int, String, String) = {
      val endDateStatus(date, appId, appAttempt, finalState) = line
      (appId.toInt, appAttempt.toInt, date, finalState)
    }
    def parseLogExitCode(line: String): (Int, Int, Int) = {
      val exitCode(logMessage, appId, appAttempt, code) = line
      (appId.toInt, appAttempt.toInt, code.toInt)
    }
    def parseLogUser(line: String): (Int, String) = {
      val userAppId(logMessage, appId, user) = line
      (appId.toInt, user)
    }

    /*
    These 4 lists allows me to separate the data into groups. I simply filter each lines and map according to
    Each function above, to obtain tuples. I finally group those tuples so that making a join will be possible
    later on.
    */
    val containers = lines.filter(l => l.matches(container.toString()))
              .mapPartitions(p => p.toList.map(l => parseLogContainers(l)).iterator)
              .groupBy(pair => (pair._1, pair._2))

    val startTimes = lines.filter(l => l.matches(patternStartTimeAttempt.toString()))
              .mapPartitions(p => p.toList.map(l => parseLogStart(l)).iterator)
              .map{case (appId, appAttempt, date) => (appId, appAttempt) -> date}

    val appUsers = lines.filter(l => l.matches(userAppId.toString()))
              .mapPartitions(p => p.toList.map(l => parseLogUser(l)).iterator).distinct

    val exitCodes = lines.filter(l => l.matches(exitCode.toString()))
              .map(l => parseLogExitCode(l))
              .map{case (appId, appAttempt, code) => (appId, appAttempt) -> code}

    val endStatus = lines.filter(l => l.matches(endDateStatus.toString()))
              .map(l => parseLogEnd(l))
              .map{case (appId, appAttempt, date, finalStatus) => (appId, appAttempt) -> (date, finalStatus)}

    val endDates = exitCodes.join(endStatus).filter{case (_, (code, (_,status))) => status=="FAILED" || code !=0}
        .map{case (key, (code, (date,status))) => key -> date}

    /*
    First i join start dates and end dates.
    Hence: StartJoinEnd looks like ((appID, attempt), (StartDate, EndDate))
    */
    val startJoinEnd = startTimes.join(endDates)

    /*
    Next I join with containers on the AppId. Hence I have: ((AppId, attempt), (Dates, Status), List(containerID, Host))
    To prepare for the join with users, it is necessary to move the attempt number to the values, we will then move it back
    to the key after joining with users.
     */
    val datesJoinContainers = startJoinEnd.join(containers).mapValues {
      case (l, buff) => (l, buff.toList.map(x => (x._3, x._4)))
    }.map {
      case (key,value) =>
        (key._1, (key._2, value))
    }

    /*
    Final RDD: We join with users. AllData looks something like: ((AppId, User, AppAttempt), List(Dates, Status), List(ContainerID, Hosts)), ...)
     */
    val allData = datesJoinContainers.join(appUsers).map {
      case (key, values) => ((key, values._2, values._1._1), values._1._2)
    }.filter(x => x._1._1 >= startId && x._1._1 <= endId)

    //allData.collect.foreach(println)

    //===========================================================================

    //Return (Error category, error, stage, source line code)
    def getError(logs: List[String]): (Int, String, Int, Int) = {
      val regex_error = """.+INFO ApplicationMaster: Final app status: FAILED.+\(reason:.*?:\s(.*?):.+""".r
      val regex_dag = """.+INFO DAGScheduler:.+Stage\s(\d+)\s\(.+:(\d+)\) failed in(.*)""".r
      val regex_appName = """.+INFO SparkContext: Submitted application:.+\s(\w+)$""".r

      def parseAppName(line: String): (String) = {
        val regex_appName(appName) = line
        (appName)
      }

      def parseErrAppMaster(line: String): (String) = {
        val regex_error(exception) = line
        (exception)
      }

      def parseScheduler(line: String): (Int, Int, Int) = {
        val container = """.+container_e02_1580812675067_\d+_\d+_(\d{6}).+""".r
        val regex_dag(stage, lineCode, logMessage) = line

        if (line.matches(container.toString())) {
          val container(executor) = logMessage
          (stage.toInt, lineCode.toInt, executor.toInt)
        } else {
          (stage.toInt, lineCode.toInt, -1)
        }

      }

      def getErrorInfo(keyWord: String, appName: String, txt: String): (String, Int, Int) = { //(ErrorType, ErrorCodeLine, -1 if unknown |0 if error from scala|1 if error from spark)
        val errorThread = txt.split(keyWord)(1).split("""\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}""".r.toString())(0).split("\n").toList
        val errorType = errorThread(1).split(":")(0)

        val regex_codeLine = (""".+at\s""" + appName + """\$\$anonfun\$\d+\.apply\(""" + appName + """\.scala:(\d+)\)""").r

        def parseCodeLine(line: String): (Int) = {
          val regex_codeLine(lineNbr) = line
          (lineNbr.toInt)
        }

        val lineMatch = errorThread.filter(l => l.matches(regex_codeLine.toString()))

        lineMatch match {
          case Nil => (errorType, -1, -1)
          case x => {
            val header = x.head
            val codeLine = parseCodeLine(header)
            val source = errorThread(errorThread.indexOf(header)-1)
            source match {
              case u if u.contains("org.apache.spark") => (errorType, codeLine, 1)
              case u if u.contains("scala.") => (errorType, codeLine, 0)
              case _ => (errorType, codeLine, -1)
            }
          }
        }
      }

      val driver = logs(0).split('\n').toList

      val exception = driver.filter(l => l.matches(regex_error.toString())).map(x => parseErrAppMaster(x)).head

      if (exception == "java.lang.ClassNotFoundException") {
        (1, exception, -1, -1)
      }
      else {
        val appName = driver.filter(l => l.matches(regex_appName.toString())).map(x => parseAppName(x)).head

        if (exception == "org.apache.spark.SparkException") {
          val scheduler_info = driver.filter(l => l.matches(regex_dag.toString())).map(x => parseScheduler(x)).head
          val executor = scheduler_info._3

          if (executor == -1) {
            (4, exception, scheduler_info._1, scheduler_info._2)
          } else {
            val execlog = logs(executor - 1)
            val errorInfo = getErrorInfo("ERROR Executor", appName, execlog)
            errorInfo match {
              case (errorType, -1, _) => (6, errorType, scheduler_info._1, scheduler_info._2)
              case (errorType, codeLine, 1) => (6, errorType, scheduler_info._1, codeLine)
              case (errorType, codeLine, 0) => (5, errorType, scheduler_info._1, codeLine)
              case (errorType, codeLine, _) => (9, errorType, scheduler_info._1, codeLine)
            }
          }
        } else {
          val execlog = logs(0)
          val errorInfo = getErrorInfo("ERROR ApplicationMaster", appName, execlog)
          errorInfo match {
            case (errorType, codeLine, 1) => (7, errorType, -1, codeLine)
            case (errorType, codeLine, 0) => (3, errorType, -1, codeLine)
            case (errorType, codeLine, _) => (9, errorType, -1, codeLine)
          }

        }
      }
    }

    val regex_gen = """.+container_e02_1580812675067_(\d+)_(\d+)_(\d+)\son.+""".r

    def parseLogFile(log: List[String]): ((Int, Int), (Int)) = {
      val loginfo = log.filter(l => l.matches(regex_gen.toString()))
      val regex_gen(appId, appAttempt, container) = loginfo(0)
      ((appId.toInt, appAttempt.toInt), (container.toInt))
    }

    val rdd = sc.wholeTextFiles(appLogs).flatMap{case (_, txt) => txt.split("Container:")}.filter(x => x!= "")
                                        .map(x => x.split("\n").toList)
                                        .map(x => parseLogFile(x) -> x.mkString("\n"))
                                        .map(x => (x._1._1._1, x._1._1._2) -> (x._1._2, x._2))
                                        .filter(x => x._1._1 >= startId && x._1._1 <= endId)
                                        .groupBy(_._1)
                                        .mapValues{
                                          case(buff) => buff.toList.map(x => x._2)
                                                            .sortBy(_._1)
                                                            .map(x => x._2)
                                        }
                                        .filter(x => x._2(0).contains("INFO ApplicationMaster: Final app status: FAILED"))
                                        .map(x => x._1 -> getError(x._2))
    rdd.foreach(println)
    //println(getError("5021", "01"))
  }
}
