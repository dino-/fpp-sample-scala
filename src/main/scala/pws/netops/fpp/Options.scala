package pws.netops.fpp

import java.io.{ File, FileInputStream }
import java.util.logging.Level
import java.util.Properties


case class ProductOpts (
  productName: String,  // For IntervalMonitor addRun
  product: Int,  // product ID used in SQL DB 
  domain: Int,  // model domian number, normally something like 1 or 2
  priority: Int,  // run priority where 1 > 2 > n 
  environment: String,  // dev, staging, prod ,etc
  installFQP: String,  // Installation dir
  intervalFile:String,  // File where model intervals will be written
  cycleDir: String,  // the directory where the model output to post process
  postWorkingDir: String  // root directory where post processing will occur for a run
)


case class IntervalOpts (
  startInterval: Int,  // Interval to start grads execs and publish
  totalIntervals: Int,  // total number of intervals 
  intervalDuration: Int,  // how many minutes in an interval
  intervalMonTimeout: Long  // delay for waiting for the tailer
)


case class GribOpts (
  gribConvCondorRAM: Int,  // RAM for gribconv Condor job
  gribFileMinSize: Long  // The minimum size of a completed grib file
)


case class GradsOpts (
  gradsBaseDir: String,  // directory where grads executable and libs are installed
  gradsTemplatesDir: String,  // directory where the post proc templates are placed
  gradsExec: String,  // FQP to grads executable
  gradsExecTimeout: Long,  // Loop delay for waiting for files before grads command
  gradsProcessors: Int,  // Number of processors to pass to xargs from GradsCondorMain
  gradsCondorRAM: Int  // RAM for gradscondor Condor job
)


case class PublishOpts (
  publish: Boolean = false,
  publishDir: String = "",
  FOSRestURL: String // URL of webapp to add the run to FOS
)


case class Options (
  cycle: String,  // cycle of forecast in format YYYYMMDDHH
  productOpts: ProductOpts,
  intervalOpts: IntervalOpts,
  gribOpts: GribOpts,
  gradsOpts: GradsOpts,
  publishOpts: PublishOpts,
  logLevel: Level,
  runId: Long, // run_id that will be used to reference output in SQL DB
  propsFiles: List[String]
)


object Options {

  def load (cycle: String, propPaths: List[String]): Options = {
    // Load the files into a single Properties object
    val props = new Properties
    val propsFQPs = propPaths map ( propPath => {
      val pr = new Properties
      val f = new File(propPath)
      pr.load(new FileInputStream(f))
      props.putAll(pr)
      f.getCanonicalPath
    })

    // Set up for the variable substitutions
    val domain = props.getProperty("domain").toInt
    val priority = props.getProperty("priority").toInt
    val environment = props.getProperty("environment")


    val replacers = List (
      ("\\$\\{cycle\\}".r,        cycle),
      ("\\$\\{domain\\}".r,       "d%02d".format(domain)),
      ("\\$\\{priority\\}".r,     priority.toString),
      ("\\$\\{environment\\}".r,  environment)
    )

    def varSubst (startingString: String): String =
      replacers.foldLeft (startingString) ( (s, reReplT) => {
        val (re, replacement) = reReplT
        re.replaceAllIn(s, replacement)
      })


    // Construct the subparts from this Properties object
    val productOpts = ProductOpts (
      props.getProperty("productName"),
      props.getProperty("product").toInt,
      domain,
      priority,
      environment,
      varSubst(props.getProperty("installFQP")),
      props.getProperty("intervalFile"),
      varSubst(props.getProperty("cycleDir")),
      varSubst(props.getProperty("postWorkingDir"))
    )

    val intervalOpts = IntervalOpts (
      props.getProperty("startInterval", "0").toInt,
      props.getProperty("totalIntervals").toInt,
      props.getProperty("intervalDuration").toInt,
      props.getProperty("intervalMonTimeout", "0").toLong
    )

    val gribOpts = GribOpts (
      props.getProperty("gribConvCondorRAM").toInt,
      props.getProperty("gribFileMinSize").toLong
    )

    val gradsOpts = GradsOpts (
      props.getProperty("gradsBaseDir"),
      varSubst(props.getProperty("gradsTemplatesDir")),
      props.getProperty("gradsExec"),
      props.getProperty("gradsExecTimeout", "0").toLong,
      props.getProperty("gradsProcessors").toInt,
      props.getProperty("gradsCondorRAM").toInt
    )

    val publishOpts = PublishOpts (
      (props.getProperty("publish", "false").toLowerCase == "true"),
      varSubst(props.getProperty("publishDir", "")),
      props.getProperty("FOSRestURL", "")
    )

    // Construct and return the Options
    Options(
      cycle,
      productOpts,
      intervalOpts,
      gribOpts,
      gradsOpts,
      publishOpts,
      Level.parse(props.getProperty("logLevel", "FINEST")),
      0L,  // Default run id, same we'd use for debugging
      propsFQPs
    )
  }


  def addRunId(newRunId: Long, oldOpts: Options): Options =
    oldOpts.copy(runId = newRunId)


  /* Turn the list of props file paths in Options.propsFiles into
     a single string of -p switches, like this:

       List("/foo/bar", "/baz/qux") -> "-p /foo/bar -p /baz/qux "

     This is used when we construct command lines for each next task.
  */
  def mkPropsArgs (opts: Options): String =
    opts.propsFiles.map(path => s"-p ${path} ").mkString("")

}
