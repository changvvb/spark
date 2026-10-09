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

import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.{IterableEncoder, UnboundRowEncoder}

/**
 * Lets the code shared by Scala 2.13 and Scala 3 ask for the type of an encoder with a
 * `TypeTag` context bound. Scala 3 cannot synthesize a `scala.reflect` `TypeTag`, so this
 * `TypeTag` holds the encoder derived at the call site, where the type is known.
 */
object EncoderTag {
  final class TypeTag[T](val encoder: AgnosticEncoder[T])

  object TypeTag {
    implicit inline def derived[T]: TypeTag[T] =
      new TypeTag(AgnosticDerivation.deriveAgnosticEncoder[T])
  }

  def encoderFor[T](implicit tag: TypeTag[T]): AgnosticEncoder[T] = tag.encoder

  /** Whether the tag is a `Seq[Row]` (or a subtype of it). */
  def isSeqOfRow[T](tag: TypeTag[T]): Boolean = tag.encoder match {
    case IterableEncoder(clsTag, UnboundRowEncoder, _, _) =>
      classOf[scala.collection.Seq[?]].isAssignableFrom(clsTag.runtimeClass)
    case _ => false
  }
}
