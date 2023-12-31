/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.flink.api.scala.extensions.impl.acceptPartialFunctions

import org.apache.flink.annotation.PublicEvolving
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.scala.{HalfUnfinishedKeyPairOperation, UnfinishedKeyPairOperation}

/**
 * Wraps an unfinished key pair operation, allowing to use anonymous partial functions to perform
 * extraction of items in a tuple, case class instance or collection
 *
 * @param ds
 *   The wrapped unfinished key pair operation data set
 * @tparam L
 *   The type of the left data set items
 * @tparam R
 *   The type of the right data set items
 * @tparam O
 *   The type of the output data set items
 * @deprecated
 *   All Flink Scala APIs are deprecated and will be removed in a future Flink major version. You
 *   can still build your application in Scala, but you should move to the Java version of either
 *   the DataStream and/or Table API.
 * @see
 *   <a href="https://s.apache.org/flip-265">FLIP-265 Deprecate and remove Scala API support</a>
 */
@deprecated(org.apache.flink.api.scala.FLIP_265_WARNING, since = "1.18.0")
class OnUnfinishedKeyPairOperation[L, R, O](ds: UnfinishedKeyPairOperation[L, R, O]) {

  /**
   * Initiates a join or co-group operation, defining the first half of the where clause with the
   * items of the left data set that will be checked for equality with the ones provided by the
   * second half.
   *
   * @param fun
   *   The function that defines the comparing item of the where clause
   * @tparam K
   *   The type of the key, for which type information must be known
   * @return
   *   A data set of Os
   */
  @PublicEvolving
  def whereClause[K: TypeInformation](fun: (L) => K): HalfUnfinishedKeyPairOperation[L, R, O] =
    ds.where(fun)

}
