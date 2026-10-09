/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.sql.catalyst.encoders

import scala.reflect.runtime.universe
import scala.reflect.runtime.universe.typeOf

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.ScalaReflection

/**
 * Lets the code shared by Scala 2.13 and Scala 3 ask for the type of an encoder with a
 * `TypeTag` context bound. Scala 2.13 uses the real `TypeTag`; see the Scala 3 version of this
 * object for how Scala 3 provides it.
 */
object EncoderTag {
  type TypeTag[T] = universe.TypeTag[T]

  def encoderFor[T: TypeTag]: AgnosticEncoder[T] = ScalaReflection.encoderFor[T]

  /** Whether the tag is a `Seq[Row]` (or a subtype of it). */
  def isSeqOfRow[T](tag: TypeTag[T]): Boolean = {
    val tpe = tag.tpe
    tpe <:< typeOf[Seq[_]] && tpe.typeArgs.nonEmpty && tpe.typeArgs.head =:= typeOf[Row]
  }
}
