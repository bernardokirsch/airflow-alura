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

package org.apache.spark.examples.extensions

import org.apache.spark.sql.catalyst.expressions.{CurrentDate, Expression, RuntimeReplaceable, SubtractDates}

/**
 * How old are you in days?
 */
case class AgeExample(birthday: Expression, child: Expression) extends RuntimeReplaceable {

  def this(birthday: Expression) = this(birthday, SubtractDates(CurrentDate(), birthday))
  override def exprsReplaced: Seq[Expression] = Seq(birthday)

  override protected def withNewChildInternal(newChild: Expression): Expression = copy(newChild)
}
