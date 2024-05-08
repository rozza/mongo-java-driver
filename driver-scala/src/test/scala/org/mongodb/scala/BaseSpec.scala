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
package org.mongodb.scala

import org.scalactic.{ Normalization, Uniformity }
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

import java.lang.reflect.Member
import java.lang.reflect.Modifier.isPublic

abstract class BaseSpec extends AnyFlatSpec with Matchers {

  case class DefaultCaseClass(wrapped: String) {}

  private val ignoredJavaAndScalaMethods: Set[String] =
    getPublicFieldAndMethodNames(classOf[DefaultCaseClass], classOf[DefaultCaseClass.type]) ++
      Set("writeReplace", "getValue", "valueOf", "values", "equals", "toString", "hashCode")

  val normalized: Uniformity[Set[String]] = {
    new Uniformity[Set[String]] {

      override def normalizedOrSame(b: Any): Any = b match {
        case s: Set[String] => normalized(s)
        case _              => b
      }

      override def normalizedCanHandle(b: Any): Boolean = b.isInstanceOf[Set[String]]

      override def normalized(a: Set[String]): Set[String] =
        a
          .filter(!_.contains("$"))
          .diff(ignoredJavaAndScalaMethods)
          .map(f => if (f.startsWith("get")) f.substring(3) else f)
          .map(f => f.toLowerCase)
    }
  }

  def getPublicFieldAndMethodNames(classes: Class[_]*): Set[String] =
    getPublicFieldAndMethodNames(f => isPublic(f.getModifiers), classes: _*)

  def getPublicFieldAndMethodNames(filter: Member => Boolean, classes: Class[_]*): Set[String] = {
    classes.toSet.flatMap(clazz => {
      val methods = clazz.getDeclaredMethods.filter(filter)
        .map(_.getName)
        .toSet

      val fields = clazz.getFields.filter(filter)
        .map(_.getName)
        .toSet

      methods ++ fields
    })
  }

}
