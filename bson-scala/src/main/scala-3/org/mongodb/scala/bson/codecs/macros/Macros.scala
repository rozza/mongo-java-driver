package org.mongodb.scala.bson.codecs.macros


import org.mongodb.scala.bson.codecs.macros.Macros.isType
import scala.quoted.*

object Macros {


  private enum isType:
    case CaseClass, CaseObject, Sealed, Abstract, SealedAbstract, Map, Option, Tuple



  inline def isCaseClass[T]: Boolean = {
    ${ checkIsOfType[T]('{isType.CaseClass}) }
  }
//  inline def isCaseObject[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.CaseObject)
//  inline def isAbstract[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.Abstract)
//  inline def isSealed[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.Sealed)
//  inline def isSealedAbstract[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.SealedAbstract)
//  inline def isMap[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.Map)
//  inline def isOption[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.Option)
//  inline def isTuple[T](using Quotes)(using t: Type[T]): Boolean = checkIsOfType[T](isType.Tuple)

  private def checkIsOfType[T: Type](ofType: Expr[isType])(using Quotes): Expr[Boolean] = {
    import quotes.reflect.*

    val flags: Flags = TypeTree.of[T].symbol.flags
    val mapTypeSymbol = TypeRepr.of[collection.Map[_, _]].typeSymbol

    ofType match
      case isType.CaseClass => flags.is(Flags.Case)
      case isType.CaseObject => flags.is(Flags.Module | Flags.Module)
      case isType.Sealed => flags.is(Flags.Sealed)
      case isType.Abstract => flags.is(Flags.Abstract)
      case isType.SealedAbstract => flags.is(Flags.Sealed & Flags.Abstract)
      case isType.Map => TypeRepr.of[T].baseClasses.contains(mapTypeSymbol)
      case isType.Option =>  TypeRepr.of[T] == TypeRepr.of[Option]
      case isType.Tuple =>  TypeRepr.of[T] == TypeRepr.of[Tuple]
      
      
  }

//
//  def collectKnownSubtypes[T](using t: Type[T]): Seq[Symbol] = {
//    val parent: Symbol = TypeTree.of[T].symbol
//    val children: List[Symbol] = parent.children
//    val parentFlags = parent.flags
//    if (parentFlags.is(Flags.Case))
//      Set(parent)
//    else
//      children.flatMap { s =>
//        val flags = s.flags
//        if (flags.is(Flags.Sealed) && flags.is(Flags.Trait)) {
//          val childTpe: TypeTree = TypeIdent(s)
//          val childType: TypeRepr = childTpe.tpe
//          val subtypes = childType.asType match
//            case '[t] => collectKnownSubtypes[t]
//          subtypes
//        } else {
//          s :: Nil
//        }
//      }
//  }
//
////  val allSubclasses = collectKnownSubtypes[A]
////  val subClasses = collectKnownSubtypes[A].filter(s => s.flags.is(Flags.Case))
//
//  inline def knownTypes[T](using t: Type[T]): Seq[Symbol] = collectKnownSubtypes[T]

}
