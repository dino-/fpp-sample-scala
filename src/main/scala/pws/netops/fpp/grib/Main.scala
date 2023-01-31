package pws.netops.fpp.grib

import java.io.File
import java.text.SimpleDateFormat
import org.sellmerfud.optparse._
import pws.netops.common.log._
import pws.netops.common.system.CondorSubmit.{ mkSubmitDataNoShared,
  writeSubmitFile, submit }

import pws.netops.fpp.{ Common, Options }


object Main {

  def main(args: Array[String]): Unit = {
    val cycleMatcher = """(\d{4}\d{2}\d{2}\d{2})""".r
    var cycle = ""
    var propertiesFiles: List[String] = Nil
    var valid_dt = 0l
    var runId = -1l
    val cli = new OptionParser
    var debug = false

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

    cli.reqd("-v", "--valid_dt=VALID_DT",
      "Valid Date Time in millis as long") { v: Long =>

        {
          valid_dt = v
        }
      }

    cli.bool("-d", "--debug", "Run in debug mode, which doesn't chain sections") { d: Boolean =>
      debug = d
    }

    cli.reqd("-r", "--ru-id=RUNID", "Run ID as assigned by FOS") { r: Long =>
      {
        runId = r
      }
    }

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

    Log.initialize("GribConverter", opts.logLevel)
    Log.addFileHandler(
      Common.mkLogPath(opts, "GribConverter", Some(valid_dt)),
      Append, DateStamp)
    val l = Log.getLogger

    (GribConverter.doWork(opts, valid_dt), debug) match {
      case (Some(msg), _) => l.severe(msg)
      case (None, true) => l.finest("Debug enabled, not sumbitting Grads Jobs")
      case (None, false) => {
        l.info("Starting grads jobs")
        startGrads(opts, valid_dt)
      }
    }
  }


  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")

  def startGrads(opts: Options, valid_dt: Long) {
    val l = Log.getLogger

    val postWorkingDir = opts.productOpts.postWorkingDir + "/" +
      postDirFmt.format(valid_dt)

    val submitPathPrefix = s"${postWorkingDir}/condor_grads.master"
    l.fine(s"Creating template submit file for ${submitPathPrefix}")

    val submitContents = mkSubmitDataNoShared(
      s"${opts.productOpts.installFQP}/bin/fpp-gradscondor",
      Options.mkPropsArgs(opts) +
        s"-c ${opts.cycle} " +
        s"-r ${opts.runId} " +
        s"-v $valid_dt ",
      submitPathPrefix,
      opts.gradsOpts.gradsCondorRAM)

    val result = submitContents flatMap writeSubmitFile flatMap submit

    if (result.isFailure)
      l.severe(s"Condor submit failed!\n${result.get}")
  }

}
