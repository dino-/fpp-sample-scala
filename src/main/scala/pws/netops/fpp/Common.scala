package pws.netops.fpp

import java.io.File
import java.text.SimpleDateFormat
import java.util.Date


object Common {

  val vdtNameFmt = new SimpleDateFormat("yyyyMMdd_HHmm")

  def mkLogPath (opts: Options, step: String,
    optVDT: Option[Long] = None,
    optScript: Option[String] = None): String = {

    // All logging happens under this directory..
    val prefix = s"${opts.productOpts.installFQP}/log/${opts.cycle}/${step}"

    // ..but there's a complicated hierarchy for all the
    // postprocessing pieces.
    val path = (optVDT, optScript) match {
      case (None          , _           ) =>
        s"${prefix}/${step}_${opts.cycle}.log"
      case (Some(valid_dt), None        ) => {
        val vdtS = vdtNameFmt.format(new Date(valid_dt))
        s"${prefix}/${step}_${opts.cycle}_${vdtS}.log"
      }
      case (Some(valid_dt), Some(script)) => {
        val vdtS = vdtNameFmt.format(new Date(valid_dt))
        s"${prefix}/${vdtS}/${step}_${opts.cycle}_${vdtS}_${script}.log"
      }
    }

    // The logging API doesn't create directory structures for you
    // Making sure it exists now
    (new File(path)).getParentFile.mkdirs

    // Finally, return the log file path we just constructed.
    path
  }

}
