package pws.netops.fpp.grads

import pws.netops.common.data.Time


object IntervalTaskFactory {

  def genFileList(valid_dt: Long, gradScript: String, cycle: String)
    : Option[List[String]] = {

    val fh = ((valid_dt - Time.cycleToInitTime(cycle))
      / (60 * 60 * 1000.0))

    def getNeededFiles(hours: Int): Option[List[String]] =
      if (hours > fh) None
      else Some(List(s"MINUS${hours}.ctl"))

    gradScript match {
      case s if s.contains("Pcpn1") => getNeededFiles(1)
      case s if s.contains("STtpcpnIn") => getNeededFiles(1)
      case s if s.contains("SReflectDbz") => getNeededFiles(1)
      case s if s.contains("1hsnow") => getNeededFiles(1)  // !
      case s if s.contains("ttsnow") => getNeededFiles(1)  // !
      case s if s.contains("Pcpn3") => getNeededFiles(3)
      case s if s.contains("3hsnow") => getNeededFiles(3)  // !
      case s if s.contains("Pcpn6") => getNeededFiles(6)
      case s if s.contains("6hsnow") => getNeededFiles(6)  // !
      case _ => Some(Nil)
    }

  }

}
