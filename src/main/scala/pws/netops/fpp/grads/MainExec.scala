package pws.netops.fpp.grads

import java.io.File
import java.text.SimpleDateFormat
import org.sellmerfud.optparse._
import pws.netops.common.log._
import scala.sys.process._

import pws.netops.fpp.{ Common, Options }


object MainExec {

  var gradsScript = ""
  var region = ""
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

    Log.initialize("GradsExec", opts.logLevel)
    Log.addFileHandler(Common.mkLogPath(opts, "GradsExec", Some(valid_dt),
      Some(s"${region}.${gradsScript}")), Append, DateStamp)
    val l = Log.getLogger

    (new GradsExec(opts, valid_dt, gradsScript, region)).doWork

    if (!debug && opts.publishOpts.publish)
      startFilePost(opts, valid_dt, gradsScript)
    else l.info(
      "publish turned off in config, file post not being performed")
  }


  val df = new SimpleDateFormat("yyyyMMdd_HHmm")
  val postDirFmt = new SimpleDateFormat("yyyy'-'MM'-'dd'_'HHmm")

  def startFilePost(opts: Options, valid_dt: Long, script: String) {
    val l = Log.getLogger

    // Use this for each submission
    val formattedVDT = df.format(valid_dt)

    val postWorkingDir = opts.productOpts.postWorkingDir + "/" +
      postDirFmt.format(valid_dt)


    l.info(s"Running file post for: ${script}")
   
    val cmd =  s"${opts.productOpts.installFQP}/bin/fpp-filepost " + 
               Options.mkPropsArgs(opts) +
               s"-c ${opts.cycle} " +
               s"-g ${script} " +
               s"-R ${region} " +
               s"-v $valid_dt " +
               s"-r ${opts.runId} " +
               (if (updateInterval) "-u " else "")
               
    
    val result =   Seq("/bin/bash","-c",cmd).! == 0

    if (!result) l.warning(s"File post failed for :${script}")
  }

}
