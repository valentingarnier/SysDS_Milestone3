import java.io._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.TextInputFormat
import org.apache.spark.{SparkConf, SparkContext}

import scala.util.matching.Regex

object Milestone3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local[*]").setAppName("Milestone3") //For cluster
    val sc = SparkContext.getOrCreate(conf)

    val resourceManager = args(0)
    val appLogs = args(1)
    val startId = args(2).toInt
    val endId = args(3).toInt

    //========================================== WORK ON RESOURCE MANAGER LOG ==========================================
    val lines = sc.textFile(resourceManager)

    /*
    Extract infos from the log:
    After an analysis of the log, we've noticed 4 regex that would capture the most important informations
    in the ressource manager's log.
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
    These 4 lists allows us to separate the data into groups. we simply filter each lines and map according to
    Each function above, to obtain tuples. we finally group those tuples so that making a join will be possible
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

    /*
    First, we join exitCodes with endStatus so that we are sure to have app that failed with exit code non Null.
    Next we don't need anymore the code and the status since we filtered with them
     */
    val endDates = exitCodes.join(endStatus)
      .filter{ case (_, (code, (_, status))) => status == "FAILED" || code != 0 }
      .map{ case (key, (code, (date, status))) => key -> date }

    /*
    We join start dates and end dates.
    Hence: StartJoinEnd looks like ((appID, attempt), (StartDate, EndDate))
    */
    val startJoinEnd = startTimes.join(endDates)

    /*
    Next we join with containers on the AppId. Hence we have: ((AppId, attempt), (Dates, Status), List(containerID, Host))
    To prepare for the join with users, it is necessary to move the attempt number to the values, we will then move it back
    to the key after joining with users.
     */
    val datesJoinContainers = startJoinEnd.join(containers)
      .mapValues { case (l, buff) => (l, buff.toList.map(x => (x._3, x._4))) }
      .map { case (key,value) => (key._1, (key._2, value)) }

    /*
    Resource manager RDD: We join with users. AppInfos looks something like: ((AppId, AppAttempt), (User, (StartDate, EndDate), List(ContainerID, Hosts)), ...))
     */
    val appInfos = datesJoinContainers.join(appUsers).map {
      case (key, values) => (key, values._1._1) -> (values._2, values._1._2)
    }.filter(x => x._1._1 >= startId && x._1._1 <= endId)

    //========================================== WORK ON APPLICATION LOGS ==========================================
    //TODO: -Categorie 4 n'est pas bien finished -Implementer la categorie 8
    /*
    These 3 regex will allow us to find insights in the driver log.
     */
    val regex_error = """.+INFO ApplicationMaster: Final app status: FAILED.+\(reason:.*?:\s(.*?):.+""".r
    val regex_error2 = """.+INFO ApplicationMaster: Final app status: FAILED.+\(reason:.*?\s(.*?).+""".r
    val regex_dag = """.+INFO DAGScheduler:.+Stage\s(\d+)\s\(.+:(\d+)\) failed in(.*)""".r
    val regex_appName = """.+INFO SparkContext: Submitted application:.+\s(\w+)""".r

    /*
    Functions to parse the regex above and to return tuples
     */
    def parseAppName(line: String): String = {
      val regex_appName(appName) = line
      appName
    }
    def parseErrAppMaster(line: String): String = {
      val regex_error(exception) = line
      exception
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
    /*
    Helper function for errorInThread
     */
    def parseCodeLine(line: String, regex: Regex): Int = {
      val regex(lineNbr) = line
      lineNbr.toInt
    }
    /*
    Parameters: -keyword: Where to split the log to work on following lines
                -appName:  the application's name
                -txt: A log in which we will find the line that cause troubles and the error type
    Return: (ErrorType, ErrorCodeLine, -1 if unknown |0 if error from scala|1 if error from spark)
     */
    def errorInThread(keyword: String, appName: String, txt: String): (String, Int, Int) = {
      //errorThread contains all lines that follow the keyword in txt (to examine the thread)
      val errorThread = txt.split(keyword)(1).split("""\d{2}/\d{2}/\d{2}\s\d{2}:\d{2}:\d{2}""".r.toString())(0).split("\n").toList
      //Format of a error thread always have the type of error as its second line (so second element in errorThread)
      val errorType = errorThread.tail.head.split(":").head

      val regex_codeLine = (""".+at\s""" + appName + """\$\$anonfun\$\d+\.apply\(""" + appName + """\.scala:(\d+)\)""").r
      val regex_codeLine2 = (""".+at\s""" + appName + """\$\.main\(""" + appName + """\.scala:(\d+)\)""").r
      val lineMatch = errorThread.filter(l => l.matches(regex_codeLine.toString()))
      val lineMatch2 = errorThread.filter(l => l.matches(regex_codeLine2.toString()))

      //LineMatch2 is only here for error category 2 since it has a different format for spotting the problematic line.
      lineMatch match {
        case Nil => {
          lineMatch2 match {
            case Nil => (errorType, -1, -1)
            case x :: tail => (errorType, parseCodeLine(x, regex_codeLine2), -1)
          }
        }
        case x :: tail => {
          val codeLine = parseCodeLine(x, regex_codeLine)
          //To know whether the thread has a spark or scala/java error, we match the source and return the according digit (0 or 1).
          val source = errorThread(errorThread.indexOf(x) - 1)
          source match {
            case u if u.contains("org.apache.spark") => (errorType, codeLine, 1)
            case u if u.contains("scala.") => (errorType, codeLine, 0)
            case _ => (errorType, codeLine, -1)
          }
        }
      }
    }

    /*
    Parameters: -logs: Contains every log for each app (Driver + executors (containers))
    Return: (Error category, error, stage, source line code)
     */
    def getErrorInfos(logs: List[String]): (Int, String, Int, Int) = {
      //The log regarding the driver is always with ID 000001 and we sort it before calling the function
      val driver = logs.head.split('\n').toList
      val list_exception = driver.filter(l => l.matches(regex_error.toString())).map(x => parseErrAppMaster(x))
      var exception = ""
      if (list_exception.isEmpty) {
        exception = "Exception"
      }
      else {
        exception = list_exception.head
      }
      /*From here we start returning elements to print for each app

       */
      if (exception == "java.lang.ClassNotFoundException") {
        //if category 1, nothing more to be done, no stage or line involved in this kind of error
        (1, exception, -1, -1)
      }
      else {
        val list_app_name = driver.filter(l => l.matches(regex_appName.toString())).map(x => parseAppName(x))
        if (list_app_name.isEmpty) {
          return (9, exception, -1, -1)
        }
        val appName = list_app_name.head

        //This IF separates spark related exceptions and non-spark ones.
        if (exception == "org.apache.spark.SparkException") {
          /*Scheduler_info is codeLine and stage that appears in the driver (DAGScheduler).
          If we have no codeLine error or stage in executors
          We know we can return the ones present in the driver by order of priority.
         */
          val scheduler_info = driver.filter(l => l.matches(regex_dag.toString())).map(x => parseScheduler(x)).head
          //Application that are like app2 contains an error Utils with an uncaught exception task result getter
          //Which results from an error while transfering data from the driver and the executors
          if (scheduler_info._3 == -1) {
            if (driver.exists(_.contains("ERROR Utils: Uncaught exception in thread task-result-getter-"))) {
              val errorInfo = errorInThread("ERROR Utils: Uncaught exception in thread task-result-getter-", appName, logs.head)
              errorInfo match {
                case (errorType, _, _) => (4, errorType, scheduler_info._1, scheduler_info._2)
              }
            }
            else if (driver.exists(_.contains("is bigger than spark.driver.maxResultSize"))) {
              (4, exception, scheduler_info._1, scheduler_info._2)
            }
            else (8, exception, scheduler_info._1, scheduler_info._2)
          }

          //If we have a spark exception and it is not a category 4, we know we have to go deep inside failed executor's log.
          else {
            val failedExecutorLog = logs(scheduler_info._3 - 1)
            val errorInfo = errorInThread("ERROR Executor", appName, failedExecutorLog)
            errorInfo match {
              case (errorType, -1, _) => (6, errorType, scheduler_info._1, scheduler_info._2) //Shuffling data (cat 6)
              case (errorType, codeLine, 1) => (6, errorType, scheduler_info._1, codeLine) //Error reading input inside executor (cat 6)
              case (errorType, codeLine, 0) => (5, errorType, scheduler_info._1, codeLine) //0 = scala problem inside executor -> cat 5
              case (errorType, codeLine, _) => (9, errorType, scheduler_info._1, codeLine) //Unknown
            }
          }
        //Now if the driver's failure is not a spark exception, no need to go inside executors we have all info inside the driverLog.
        } else {
          val driverLog = logs.head
          //Work with driverLog
          val errorInfo = errorInThread("ERROR ApplicationMaster", appName, driverLog)
          errorInfo match {
            case (errorType, codeLine, 1) => (7, errorType, -1, codeLine) //Spark operation in the driver
            case (errorType, codeLine, 0) => (3, errorType, -1, codeLine) //Non-spark java/scala code at the driver
            case ("org.apache.hadoop.mapred.InvalidInputException", codeLine, _) => (2, "org.apache.hadoop.mapred.InvalidInputException", -1, codeLine)
            case (errorType, codeLine, _) => (9, errorType, -1, codeLine)
          }
        }
      }
    }

    /*
    This function create a sort of header for a log. It retrives the appID, appAttempt, containerID
    That will be present on the first line of each log
     */
    def parseLogFile(log: List[String]): ((Int, Int), (Int)) = {
      val regex_gen = """.+container_e02_1580812675067_(\d+)_(\d+)_(\d+)\son.+""".r
      val logInfo = log.filter(l => l.matches(regex_gen.toString()))

      val regex_gen(appId, appAttempt, container) = logInfo.head
      ((appId.toInt, appAttempt.toInt), container.toInt)
    }

    val applogs_conf = new Configuration(sc.hadoopConfiguration)
    applogs_conf.set("textinputformat.record.delimiter", "Container:")

    //Here we parse the big aggregated log file.
    sc.hadoopConfiguration.set("textinputformat.record.delimiter", "Container:")
    val errorCategories = sc.textFile(appLogs)
      //Split on each containers
      .filter(!_.isEmpty)
      //Now we have an RDD with a string containing the log for each container.
      //map it to each line inside the string
      .map(x => x.split("\n").toList)
      //Append the header for each app altogether with the container log.
      .map(x => parseLogFile(x) -> x.mkString("\n"))
      //Rearrange like (appID, appAttempt), (ContainerID, containerLog)
      .map(x => (x._1._1._1, x._1._1._2) -> (x._1._2, x._2))
      //Take only the one that we are interested in
      .filter(x => x._1._1 >= startId && x._1._1 <= endId)
      //Voir avec 4275
      //GroupBy AppID, AppAttempt.
      .groupBy(_._1)
      //Now for each (containerID, containerLog)
      .mapValues {
        //Just take the containerID and log part since groupBy will put AppID and AppAttempt inside the values
        buff =>
          buff.toList.map(x => x._2)
            //Sort by container ID: VERY IMPORTANT: The first one (000001) will be the driver log always
            .sortBy(_._1)
            //remove containerID
            .map(x => x._2)
      }
      //Here we have RDD[(AppID, AppAttempt), List(DriverLog, Executor1Log ...)]
      //Take only the ones who failed based on the driver
      .filter(x => x._2.head.contains("INFO ApplicationMaster: Final app status: FAILED"))
      //Use our big functions above to gather all the required informations
      .map(x => x._1 -> getErrorInfos(x._2))

    //Now rdd is ready to be joined with the RDD that we built in Milestone1 (allData) on (AppID, AppAttempt)
    //This is why it was important to keep AppAttempt
    //val final_rdd = appInfos.join(errorCategories)
    errorCategories.foreach(println)




    //====================================== WRITING ANSWERS =============================================



    // Writing the final_rdd to answers.txt :
    // verify if foreach writes the rdd in the same order as it is stored
    // verify if containers are sorted (if not, sort val containers)
    /*val file = new File("answers.txt")
    val bw = new BufferedWriter(new FileWriter(file))

    final_rdd.foreach{x =>
      val appId = x._1._1
      val appAttempt = x._1._2
      bw.write("AppAttempt : appattempt_1580812675067_" + f"${appId}%04d" + "_" + f"${appAttempt}%06d" + "\n")
      bw.write("User : " + x._2._1 + "\n")
      bw.write("StartTime : " + x._2._1._2._1 +"\n")
      bw.write("EndTime : " + x._2._1._2._1 + "\n")
      val containers = x._2._1._2._2.map{x =>
        "container_e02_1580812675067_" + f"${appId}%04d" + "_" + f"${appAttempt}%02d" + "_" + x._1 + " -> " + x._2 + "\n"}
      bw.write("Containers : " + containers.mkString(", ") + "\n")
      bw.write("ErrorCategory : " + x._2._2._1 + "\n")
      bw.write("Exception : " + x._2._2._2 + "\n")
      bw.write("Stage : " + x._2._2._3 + "\n")
      bw.write("SourceCodeLine : " + x._2._2._4 + "\n")
      bw.write("\n")
    }

    bw.close()*/
  }
}
