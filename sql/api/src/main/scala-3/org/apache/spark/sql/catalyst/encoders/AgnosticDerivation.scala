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

import javax.lang.model.SourceVersion

import scala.compiletime.{constValueTuple, erasedValue, error, summonFrom, summonInline}
import scala.deriving.Mirror
import scala.reflect.{ClassTag, NameTransformer}

import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.WalkedTypePath
import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders._
import org.apache.spark.sql.errors.ExecutionErrors
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.{CalendarInterval, VariantVal}

/**
 * Scala 3 replacement of `ScalaReflection.encoderFor`. It derives an [[AgnosticEncoder]] at
 * compile time from the shape of the type (using [[scala.deriving.Mirror]]), so it needs neither
 * a `TypeTag` nor runtime reflection to walk the type.
 *
 * The cases and their order follow `ScalaReflection.encoderFor`, so that both produce the same
 * encoder for the same type. Not supported yet: `scala.Enumeration`, circular references (they
 * fail at compile time as the inlining does not terminate) and types that are only
 * `DefinedByConstructorParams`.
 */
object AgnosticDerivation {

  inline def deriveAgnosticEncoder[T]: AgnosticEncoder[T] = deriveOf[T]

  inline def deriveOf[T]: AgnosticEncoder[T] = derive[T].asInstanceOf[AgnosticEncoder[T]]

  inline def derive[T]: AgnosticEncoder[?] = {
    inline erasedValue[T] match {
      // The Null case must come first, as Null is a subtype of every reference type.
      case _: Null => NullEncoder

      // Primitive encoders
      case _: Boolean => PrimitiveBooleanEncoder
      case _: Byte => PrimitiveByteEncoder
      case _: Short => PrimitiveShortEncoder
      case _: Int => PrimitiveIntEncoder
      case _: Long => PrimitiveLongEncoder
      case _: Float => PrimitiveFloatEncoder
      case _: Double => PrimitiveDoubleEncoder
      case _: java.lang.Boolean => BoxedBooleanEncoder
      case _: java.lang.Byte => BoxedByteEncoder
      case _: java.lang.Short => BoxedShortEncoder
      case _: java.lang.Integer => BoxedIntEncoder
      case _: java.lang.Long => BoxedLongEncoder
      case _: java.lang.Float => BoxedFloatEncoder
      case _: java.lang.Double => BoxedDoubleEncoder
      case _: Array[Byte] => BinaryEncoder

      // Enums
      case _: java.lang.Enum[?] => JavaEnumEncoder(summonInline[ClassTag[T]])
      case _: Enumeration#Value => error("scala.Enumeration is not supported yet")

      // Leaf encoders
      case _: String => StringEncoder
      case _: Decimal => DEFAULT_SPARK_DECIMAL_ENCODER
      case _: BigDecimal => DEFAULT_SCALA_DECIMAL_ENCODER
      case _: java.math.BigDecimal => DEFAULT_JAVA_DECIMAL_ENCODER
      case _: BigInt => ScalaBigIntEncoder
      case _: java.math.BigInteger => JavaBigIntEncoder
      case _: CalendarInterval => CalendarIntervalEncoder
      case _: java.time.Duration => DayTimeIntervalEncoder
      case _: java.time.Period => YearMonthIntervalEncoder
      case _: java.sql.Date => STRICT_DATE_ENCODER
      case _: java.time.LocalDate => STRICT_LOCAL_DATE_ENCODER
      case _: java.sql.Timestamp => STRICT_TIMESTAMP_ENCODER
      case _: java.time.Instant => STRICT_INSTANT_ENCODER
      case _: java.time.LocalDateTime => LocalDateTimeEncoder
      case _: java.time.LocalTime => LocalTimeEncoder
      case _: VariantVal => VariantEncoder
      case _: Geography => DEFAULT_GEOGRAPHY_ENCODER
      case _: Geometry => DEFAULT_GEOMETRY_ENCODER
      case _: Row => UnboundRowEncoder

      // Complex encoders
      case _: Option[t] => OptionEncoder(deriveOf[t])

      case _: Array[t] =>
        val element = deriveOf[t]
        ArrayEncoder(element, element.nullable)

      case _: scala.collection.Seq[t] =>
        val element = deriveOf[t]
        IterableEncoder(summonInline[ClassTag[T]], element, element.nullable, false)

      case _: scala.collection.Set[t] =>
        val element = deriveOf[t]
        IterableEncoder(summonInline[ClassTag[T]], element, element.nullable, false)

      case _: scala.collection.Map[k, v] =>
        val value = deriveOf[v]
        MapEncoder(summonInline[ClassTag[T]], deriveOf[k], value, value.nullable)

      // UDTs and Products, which cannot be told apart by their static type.
      case _ => deriveUdtOrProduct[T]
    }
  }

  inline def deriveUdtOrProduct[T]: AgnosticEncoder[?] = {
    val cls = summonInline[ClassTag[T]].runtimeClass
    summonFrom {
      case mirror: Mirror.ProductOf[T] =>
        udtEncoderFor(cls).getOrElse(productEncoder[T](mirror))
      case _ =>
        udtEncoderFor(cls).getOrElse {
          throw ExecutionErrors.cannotFindEncoderForTypeError(cls.getName)
        }
    }
  }

  inline def productEncoder[T](mirror: Mirror.ProductOf[T]): AgnosticEncoder[?] = {
    val names = constValueTuple[mirror.MirroredElemLabels].toList.asInstanceOf[List[String]]
    val encoders = fieldEncoders[mirror.MirroredElemTypes]
    val clsTag = summonInline[ClassTag[T]]
    val path = WalkedTypePath().recordRoot(clsTag.runtimeClass.getName)
    val fields = names.zip(encoders).map { case (name, encoder) =>
      checkFieldName(name, path.recordField(encoder.clsTag.runtimeClass.getName, name))
      EncoderField(name, encoder, encoder.nullable, Metadata.empty)
    }
    ProductEncoder(
      clsTag,
      fields,
      Option(OuterScopes.getOuterScope(clsTag.runtimeClass)))
  }

  inline def fieldEncoders[Ts <: Tuple]: List[AgnosticEncoder[?]] = {
    inline erasedValue[Ts] match {
      case _: EmptyTuple => Nil
      case _: (t *: ts) => deriveOf[t] :: fieldEncoders[ts]
    }
  }

  def checkFieldName(fieldName: String, path: WalkedTypePath): Unit = {
    if (SourceVersion.isKeyword(fieldName) ||
        !SourceVersion.isIdentifier(NameTransformer.encode(fieldName))) {
      throw ExecutionErrors.cannotUseInvalidJavaIdentifierAsFieldNameError(fieldName, path)
    }
  }

  def udtEncoderFor(cls: Class[?]): Option[AgnosticEncoder[?]] = {
    val annotation = cls.getAnnotation(classOf[SQLUserDefinedType])
    if (annotation != null) {
      val udt = annotation.udt().getConstructor().newInstance().asInstanceOf[UserDefinedType[Any]]
      val udtClass = udt.userClass.getAnnotation(classOf[SQLUserDefinedType]).udt()
      Some(UDTEncoder(udt, udtClass))
    } else if (UDTRegistration.exists(cls.getName)) {
      val udt = UDTRegistration
        .getUDTFor(cls.getName)
        .get
        .getConstructor()
        .newInstance()
        .asInstanceOf[UserDefinedType[Any]]
      Some(UDTEncoder(udt, udt.getClass))
    } else {
      None
    }
  }
}
