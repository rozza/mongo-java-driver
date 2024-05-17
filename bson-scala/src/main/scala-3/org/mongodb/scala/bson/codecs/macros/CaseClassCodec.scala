/*
 * Copyright 2008-present MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.mongodb.scala.bson.codecs.macros

import org.bson.{BsonReader, BsonWriter}
import org.bson.codecs.{Codec, DecoderContext, EncoderContext}


case class Parameter()
case class PropertyModel(param: Parameter, fieldName: String, codec: Codec[Any])

case class CaseClassCodec[T](clazz: Class[T], primaryConstructor: () => T, propertyModels: List[PropertyModel], 
                             ignoreNone: Boolean = false) extends Codec[T] {

  override def decode(reader: BsonReader, decoderContext: DecoderContext): T = ???

  override def encode(writer: BsonWriter, value: T, encoderContext: EncoderContext): Unit = ???

  override def getEncoderClass: Class[T] = clazz
}


import scala.quoted.*


inline def createNewInstance[A <: AnyRef]: A = ${ makeNewInstance[A] }

def makeNewInstance[A <: AnyRef : Type](using Quotes): Expr[A] = {
  import quotes.reflect.*

  val primaryConstructor = TypeRepr.of[A].classSymbol.map(sym => Select(New(TypeTree.of[A]), sym.primaryConstructor)).getOrElse(???)

  // Declared types
  val mainType = TypeRepr.of[A]

  val stringType = TypeRepr.of[String]
  val mapTypeSymbol = TypeRepr.of[collection.Map[_, _]].typeSymbol

  // Names
  val classTypeName = mainType.typeSymbol.name


  // Type checkers
  inline def flags[T](using t: Type[T]): Flags = TypeTree.of[T].symbol.flags
  inline def isCaseClass[T](using t: Type[T]): Boolean = flags[T].is(Flags.Case)
  inline def isCaseObject[T](using t: Type[T]): Boolean = flags[T].is(Flags.Module) && isCaseClass[T]
  inline def isSealed[T](using t: Type[T]): Boolean = flags[T].is(Flags.Sealed)
  inline def isAbstract[T](using t: Type[T]): Boolean = flags[T].is(Flags.Abstract)
  inline def isAbstractSealed[T](using t: Type[T]): Boolean = flags[T].is(Flags.Abstract | Flags.Sealed)
  inline def isMap[T](using t: Type[T]): Boolean = TypeRepr.of[T].baseClasses.contains(mapTypeSymbol)
  inline def isOption[T](using t: Type[T]): Boolean = TypeRepr.of[T] == TypeRepr.of[Option]
  inline def isTuple[T](using t: Type[T]): Boolean = TypeRepr.of[T] == TypeRepr.of[Tuple]
  
  def collectKnownSubtypes[T](using t: Type[T]): Set[Symbol] = {
    val parent: Symbol = TypeTree.of[T].symbol
    val children: List[Symbol] = parent.children
    val parentFlags = parent.flags
    if (parentFlags.is(Flags.Case))
      Set(parent)
    else
      children.flatMap { s =>
        val flags = s.flags
        if (flags.is(Flags.Sealed) && flags.is(Flags.Trait)) {
          val childTpe: TypeTree = TypeIdent(s)
          val childType: TypeRepr = childTpe.tpe
          val subtypes = childType.asType match
            case '[t] => collectKnownSubtypes[t]
          subtypes
        } else {
          s :: Nil
        }
      }.toSet
  }

  val allSubclasses = collectKnownSubtypes[A]
  val subClasses = collectKnownSubtypes[A].filter(s => s.flags.is(Flags.Case))

  //if (isSealed[A] && subClasses.isEmpty) error(s"No known subclasses of the sealed ${if (flags[A].is(Flags.Trait)) "trait" else "class"}")
  val knownTypes = (Seq(mainType) ++ subClasses).reverse


//
//  val subClasses: List[Type[_]] = collectKnownSubtypes[A].filter(t => isCaseClass(t) || isCaseObject(t)).toList
//    allSubclasses(mainType.typeSymbol).map(_.asClass.toType).filter(t => isCaseClass(t) || isCaseObject(t)).toList
//  if (isSealed(mainType) && subClasses.isEmpty) {
//    c.abort(
//      c.enclosingPosition,
//      s"No known subclasses of the sealed ${if (mainType.typeSymbol.asClass.isTrait) "trait" else "class"}"
//    )
//  }
//  val knownTypes: List[Type[_]] = (mainType +: subClasses).filterNot(_.typeSymbol.isAbstract).reverse
//

//  // Type checkers
//  def isCaseClass(t: Type[_]): Boolean = {
//    // https://github.com/scala/bug/issues/7755
//    val _ = t.typeSymbol.typeSignature
//    t.typeSymbol.isClass && t.typeSymbol.asClass.isCaseClass && !t.typeSymbol.isModuleClass
//  }
//


  // Symbol.classSymbol($className).fieldMembers.map(_.name)
  // inline def getProps(inline className: String): Iterable[String] = ${getPropsImpl('className)}
  //
  //def getPropsImpl(className: Expr[String])(using Quotes): Expr[Iterable[String]] = {
  //  import quotes.reflect.*
  //
  //  Expr.ofSeq(
  //    Symbol.classSymbol(className.valueOrAbort).fieldMembers.map(s =>
  //      Literal(StringConstant(s.name)).asExprOf[String]
  //    )
  //  )
  //}


  //println(s" >>> $classTypeName, ${isCaseClass[A]}, $allSubclasses")



  Apply(primaryConstructor, List('{"Ross"}.asTerm, '{42}.asTerm)).asExprOf[A]

}
