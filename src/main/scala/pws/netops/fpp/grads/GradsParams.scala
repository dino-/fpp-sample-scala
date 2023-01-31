package pws.netops.fpp.grads

import scala.io.Source
import scala.util.Try


object GradsParams {

  final case class FOSParams private[GradsParams] (v: Map[String, Int]) {
    def + (pair: (String, Int)): FOSParams = FOSParams(v + pair)
    def get (key: String): Int = v.getOrElse(key, -1)
  }


  final case class GradsRegions private[GradsParams]
    (v: Map[String, (Int, String)]) {

    def + (pair: (String, (Int, String))): GradsRegions =
      GradsRegions(v + pair)

    def get (key: String): (Int, String) = v.getOrElse(key, (-1, ""))
  }


  type FOSTuple = (FOSParams, GradsRegions)

  type Extracted = (
    String,   // type
    String,   // key
    Int,      // param/region
    String)   // grads params


  // Pattern for parsing *.grparam file lines
  val re = """^([^# ][^ ]*)[ ]+([^ ]+)[ ]+([^ ]+)[ ]*(.*)$""".r


  def load (path: String): Try[(FOSParams, GradsRegions)] = Try {

    // Parse a String into the 3 pieces of an FOS lookup datum
    // None means no parse was possible, as with blank lines and
    // comments.
    def parseLine (line: String): Option[Extracted] =
      line match {
        case re(ty, key, pOrR_S, grp) => Some((ty, key, pOrR_S.toInt, grp))
        case _                        => None
      }

    // Insert the data extracted from a line into the proper map
    // in the supplied tuple
    def insert (et: Extracted, ft: FOSTuple): FOSTuple = {
      val (ty, key, pOrR, grp) = et
      val (params, regions) = ft

      ty match {
        case "param" => (params + (key -> pOrR), regions)
        case "region" => (params, regions + (key -> (pOrR, grp)))
      }
    }

    // File parsing starts here

    // Get all lines from the file
    val lines = Source.fromFile(path).getLines.toList

    // Map parseLine over them, getting a list of Option[...]
    // Throw away the None-s
    val extracted = (lines map parseLine).flatten

    // Partition the extracted line data into the proper maps
    val empty: FOSTuple = (FOSParams(Map.empty), GradsRegions(Map.empty))
    extracted.foldRight (empty) (insert _)

  }

}
