package pws.netops.fpp.grads

import java.io.File
import java.text.SimpleDateFormat
import org.sellmerfud.optparse._
import pws.netops.common.log._
import pws.netops.common.net.REST
import pws.netops.common.system.IO.{ writeFile }
import scala.sys.process._
import scala.util.{ Failure, Success }
import scala.xml._

import pws.netops.fpp.{ Common, Options }


object MainCondor {

  var gradsScript = ""
  var cycle = ""
  var propertiesFiles: List[String] = Nil
  var valid_dt = 0l
  var runId = -1l
  var debug = false
  var updateInterval = false

  def main(args: Array[String]): Unit = {
    val cycleMatcher = """(\d{4}\d{2}\d{2}\d{2})""".r
    val cli = new OptionParser

    cli.reqd("-c", "--cycle=CYCLE", "Model Cycle Designation") { c: String =>
      {
        cycle = c match {
          case cycleMatcher(c) => c
          case _ => throw new OptionParserException("'%s' not a valid cycle identifier".format(c))
        }
      }
    }
    cli.reqd("-p", "--properties=PROPERTIESFILE", "Properties File Path") { p: String =>
      {
        val f = new File(p)
        if (!f.exists) {
          throw new OptionParserException("Properties File:%s does not exist".format(p))
        }
        if (!f.canRead) {
          throw new OptionParserException("Can not read Properties File:%s".format(p))
        }
        propertiesFiles = p :: propertiesFiles
      }
    }

    cli.reqd("-v", "--valid_dt=VALID_DT", "Valid Date Time in millis") { v: Long =>
      {
        valid_dt = v
      }
    }

    cli.reqd("-r", "--ru-id=RUNID", "Run ID as assigned by FOS") { r: Long =>
      {
        runId = r
      }
    }

    cli.bool("-d", "--debug", "Run in debug mode, which doesn't chain sections") { d: Boolean =>
      debug = d
    }

    cli.bool("-u", "--update-interval",
      "Update interval count in FOS. Default: false") { u => updateInterval = u }

    try {
      val cliArgs = cli.parse(args)
    } catch {
      case e: OptionParserException => {
        println(e.getMessage)
        println(cli)
        sys.exit(1)
      }
    }

    if (propertiesFiles.length == 0) {
      println("missing argument: -p PROPERTIESFILE | --properties=PROPERTIESFILE")
      println(cli)
      sys.exit(2)
    }

    if (cycle.equals("")) {
      println("missing argument: -c CYCLE | --cycle=CYCLE")
      println(cli)
      sys.exit(2)
    }

    if (valid_dt == 0) {
      println("missing argument: -v VALID_DT | --valid_dt=VALID_DT")
      println(cli)
      sys.exit(2)
    }

    if (runId < 0) {
      println("missing argument: -r RUN_ID | --run-id=RUN_ID")
      println(cli)
      sys.exit(2)
    }

    val opts = Options.addRunId(runId, Options.load(cycle, propertiesFiles))

    Log.initialize("GradsCondor", opts.logLevel)
    Log.addFileHandler(Common.mkLogPath(opts, "GradsCondor", Some(valid_dt),
      Some(gradsScript)), Append, DateStamp)

    startGrads(opts, valid_dt)
  }


  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")

  def startGrads(opts: Options, valid_dt: Long) {
    val l = Log.getLogger

    val postWorkingDir = opts.productOpts.postWorkingDir + "/" +
      postDirFmt.format(valid_dt)

    def gradsFilter(script: String): Boolean = {
      IntervalTaskFactory.genFileList(valid_dt, script, opts.cycle) match {
        case Some(x) => true
        case None => false
      }
    }

    // Make the grads script list

    // Load the FOS params and regions
    val (params, regions) = GradsParams.load(s"${opts.productOpts.installFQP}/config/${opts.productOpts.productName}.grparam") match {
      case Failure(e) => throw e
      case Success(t) => t
    }
    val gradsScripts = {
      val internalMap = params.v  // The Map inside this data structure
      val allGrads = internalMap.keySet  // We just need the keys, which are the grads scripts,..
      allGrads.filter(gradsFilter)  // ..that pass the IntervalTaskFactory check
    }
    val regionNames = {
      val internalMap = regions.v  // The Map inside this data structure
      internalMap.keySet.toList  // We just need the keys, which are the region names
    }

    val listFileLines = for {
      gs <- gradsScripts
      r <- regionNames
    } yield s"-g ${gs} -R ${r}"

    // This file is what xargs is iterating over
    val gradsListFile = s"${postWorkingDir}/grads_scripts_list"

    // Save it to a file, newline separated
    writeFile(gradsListFile, listFileLines.mkString("\n"))


    // Execute the xargs command, passing the grads script list file
    val cmd =  s"xargs -P ${opts.gradsOpts.gradsProcessors} -a ${gradsListFile} -n 4 " +
               s"${opts.productOpts.installFQP}/bin/fpp-gradsexec " +
               Options.mkPropsArgs(opts) +
               s"-c ${opts.cycle} " +
               s"-v $valid_dt " +
               s"-r ${opts.runId}"

    val result = Seq("/bin/bash", "-c", cmd).! == 0

    // Check the return code from those, if they succeeded:
    if (opts.publishOpts.publish)
      updateRun (opts.runId, opts.publishOpts.FOSRestURL)

    if (!result)
      l.severe("Grads Condor main xargs failed")
  }


  def updateRun (runId: Long, restURL: String): Either[Exception, String] = {
    val l = Log.getLogger

    val body = String.format("<update><availableinterval><run_id>" +
        runId +
        "</run_id></availableinterval></update>");
    l.info(s"REST add run URL: ${restURL}")
    l.info(s"REST add run post body:\n${body}")
    REST.httpPost(body, restURL) match {
      case Right(response) => {

        l.info(s"REST.http post response:\n${response}")
        val xmlData = XML.loadString(response)
        val id = xmlData \\ "id"
        
        Right(id.text.toString())
      }
      case Left(e) => Left(e)
    }
  }     

}
