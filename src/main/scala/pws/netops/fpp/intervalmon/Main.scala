package pws.netops.fpp.intervalmon

import java.io.File
import org.sellmerfud.optparse._
import pws.netops.common.data.Time
import pws.netops.common.log._
import pws.netops.common.net.REST
import scala.xml._

import pws.netops.fpp.{ Common, Options }


object Main {

  def main(args: Array[String]): Unit = {
    val cycleMatcher = """(\d{4}\d{2}\d{2}\d{2})""".r
    var cycle = ""
    var propertiesFiles: List[String] = Nil
    var valid_dt = 0l
    val cli = new OptionParser
    var debug = false
    var dumpOptions = false

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

    cli.bool("-d", "--debug", "Run in debug mode, which doesn't chain sections") { d: Boolean =>
      debug = d
    }

    cli.bool("-o", "--options", "Dump the options object and stop processing") { o: Boolean =>
      dumpOptions = o
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

    val opts = Options.load(cycle, propertiesFiles)

    Log.initialize("IntervalMonitor", opts.logLevel)
    Log.addFileHandler(Common.mkLogPath(opts, "IntervalMonitor"),
      Append, DateStamp)

    val newOpts = if (!opts.publishOpts.publish || debug) {
      // We don't need to generate a run id as we're not publishing anything
      opts
    } else {
      addRun(s"PWS ${opts.productOpts.productName.toUpperCase}\\@${cycle.substring(0, 8)} ${cycle.substring(8, 10)}z",
        Time.cycleToInitTime(cycle), opts.productOpts.product,
        opts.publishOpts.FOSRestURL) match {
          case Right(response) => {
            println("Hydra.REST.addRun: RunID=" + response)
            try {
              Options.addRunId(response.toLong, opts)
            } catch {
              case e: Exception => {
                println("Unable to get run id from %s. Exception %s".format(
                  response, e.getMessage()))
                sys.exit(1)
              }
            }
          }
          case Left(e) => {
            println("Exception Getting Run ID: %s".format(e.getMessage))
            e.printStackTrace()
            sys.exit(2)
            opts
          }
      }
    }

    if (dumpOptions) {
      //println("Options:\n%s".format(newOpts.toStringWithIdent("\t")))
      println(newOpts)
      sys.exit(0)
    }

    try {
      IntervalMonitor.doWork(newOpts, debug)
    } catch {
      case e: Exception => {
        val l = Log.getLogger
        l.severe("Exception in IntervalMonitor: %s".format(e.getMessage))
        l.severe(e.getClass.getName)
        l.severe(e.getStackTraceString)
      }
    }
  }

  def addRun(name: String,
    initTime: Long,
    productId: Int,
    restURL: String): Either[Exception, String] = {

    val body = String.format("<insert><run><name>" + name +
      "</name><init_time>" + initTime +
      "</init_time><products><product>" + productId +
      "</product></products></run></insert>");
    println("Rest.addRun():URL=" + restURL)
    println("Rest.addRun():Post=" + body)
    REST.httpPost(body, restURL) match {
      case Right(response) => {

        println("REST.addRun():Response=" + response)
        val xmlData = XML.loadString(response)
        val id = xmlData \\ "id"

        Right(id.text.toString())
      }
      case Left(e) => Left(e)
    }
  }

}
