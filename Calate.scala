package org.main.scala

object Calate {

  def intercalate[A](a: List[A], b: List[A]): List[A] = a match {
    case h :: t => h :: intercalate(b, t)
    case _ => b
  }

  /**
   * De-interlace two lists.
   *
   * E.g. extracalate(List(1,2,3,4,5)) == (List(1,3,5), List(2,4))
   */
  def extracalate[A](a: Seq[A]): (List[A], List[A]) =
    a.foldRight((List[A](), List[A]())) { case (b, (a1, a2)) => (b :: a2, a1) }

}
