/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.intel.ssg.bdt.spark.sql.catalyst.expressions;

import org.apache.spark.unsafe.array.ByteArrayMethods;
import org.apache.spark.unsafe.types.UTF8String;

import static org.apache.spark.unsafe.Platform.*;

public class UTF8StringHelper {
  public static UTF8String trim(UTF8String word, UTF8String trimWord) {
    int len = trimWord.numBytes();
    if (len == 0) return word;
    int s = 0;
    int e = word.numBytes() - len;
    while (s + len <= word.numBytes() && matchAt(word, trimWord, s)) s+=len;
    while (e >= 0 && matchAt(word, trimWord, e)) e-=len;
    if (s > e) {
      // empty string
      return UTF8String.fromBytes(new byte[0]);
    } else {
      return copyUTF8String(word, s, e + len - 1);
    }
  }

  public static UTF8String trimLeft(UTF8String word, UTF8String trimWord) {
    int len = trimWord.numBytes();
    if (len == 0) return word;
    int s = 0;
    while (s + len <= word.numBytes() && matchAt(word, trimWord, s)) s+=len;
    if (s == word.numBytes()) {
      // empty string
      return UTF8String.fromBytes(new byte[0]);
    } else {
      return copyUTF8String(word, s, word.numBytes() - 1);
    }
  }

  public static UTF8String trimRight(UTF8String word, UTF8String trimWord) {
    int len = trimWord.numBytes();
    if (len == 0) return word;
    int e = word.numBytes() - len;
    while (e >= 0 && matchAt(word, trimWord, e)) e-=len;
    if (e < 0) {
      // empty string
      return UTF8String.fromBytes(new byte[0]);
    } else {
      return copyUTF8String(word, 0, e + len - 1);
    }
  }

  private static boolean matchAt(final UTF8String word, final UTF8String trimWord, int pos) {
    if (trimWord.numBytes() + pos > word.numBytes() || pos < 0) {
      return false;
    }
    return ByteArrayMethods.arrayEquals(
      word.getBaseObject(),
      word.getBaseOffset() + pos,
      trimWord.getBaseObject(),
      trimWord.getBaseOffset(),
      trimWord.numBytes());
  }

  /**
   * Copy the bytes from the current UTF8String, and make a new UTF8String.
   * @param start the start position of the current UTF8String in bytes.
   * @param end the end position of the current UTF8String in bytes.
   * @return a new UTF8String in the position of [start, end] of current UTF8String bytes.
   */
  private static UTF8String copyUTF8String(UTF8String s, int start, int end) {
    int len = end - start + 1;
    byte[] newBytes = new byte[len];
    copyMemory(s.getBaseObject(), s.getBaseOffset() + start, newBytes, BYTE_ARRAY_OFFSET, len);
    return UTF8String.fromBytes(newBytes);
  }
}
