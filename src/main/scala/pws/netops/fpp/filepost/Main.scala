package pws.netops.fpp.filepost

import java.io.File
import org.sellmerfud.optparse._
import pws.netops.common.log._

import pws.netops.fpp.{ Common, Options }


object Main {
  def main(args: Array[String]): Unit = {
    val cycleMatcher = """(\d{4}\d{2}\d{2}\d{2})""".r
    var cycle = ""
    var propertiesFiles: List[String] = Nil
    var valid_dt = 0l
    val cli = new OptionParser
    var gradsScript = ""
    var region = ""
    var runId = -1l
    var updateInterval = false

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

    cli.reqd("-g", "--gradsscript=GRADS_SCRIPT", "Name of grad script to run") { g: String =>
      {
        gradsScript = g
      }
    }

    cli.reqd("-R", "--region=REGION_NAME", "Name of the region for this grads script run")
      { reg: String =>
        {
          region = reg
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

    cli.bool("-u", "--update-interval",
      "Update interval count in FOS. Default: false")
      { u => updateInterval = u }

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

    if (gradsScript.equals("")) {
      println("missing argument: -g GRADS_SCRIPT | --gradsscript=GRADS_SCRIPT")
      println(cli)
      sys.exit(2)
    }

    if (region.equals("")) {
      println("missing argument: -R REGION_NAME | --region=REGION_NAME")
      println(cli)
      sys.exit(2)
    }

    val opts = Options.addRunId(runId, Options.load(cycle, propertiesFiles))

    Log.initialize("FilePoster", opts.logLevel)
    Log.addFileHandler(Common.mkLogPath(opts, "FilePoster", Some(valid_dt),
      Some(s"${region}.${gradsScript}")), Append, DateStamp)

    if (opts.publishOpts.publish)
      (new FilePoster(opts, valid_dt, gradsScript, region)).doWork
  }

}
