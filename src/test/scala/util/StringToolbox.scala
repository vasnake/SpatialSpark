package util

object StringToolbox {

  case class Separators(v: String, next: Option[Separators] = None)

  object DefaultSeparators {
    implicit val commaColon = Separators(",", Some(Separators(":")))
  }

  implicit class RichString(val src: String) extends AnyVal {

    /**
      * Convert string to array of trimmed strings, empty items will be filter out.
      * @param sep split marker
      * @return empty array or array of trimmed strings
      */
    def splitTrim(implicit sep: Separators): Array[String] =
      src.trim.split("""\s*""" + sep.v + """\s*""").filter(_.nonEmpty)

  }
}
