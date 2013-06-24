/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package core.data;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ArgumentChecker;

/**
 * 
 */
public class Condition {
  
  private ByteSequence cf;
  private ByteSequence cq;
  private ByteSequence cv;
  private ByteSequence val;
  private Long ts;
  private IteratorSetting iterators[] = new IteratorSetting[0];
  private static final ByteSequence EMPTY = new ArrayByteSequence(new byte[0]);
  

  public Condition(CharSequence cf, CharSequence cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = new ArrayByteSequence(cf.toString().getBytes(Constants.UTF8));
    this.cq = new ArrayByteSequence(cq.toString().getBytes(Constants.UTF8));
    this.cv = EMPTY;
  }
  
  public Condition(byte[] cf, byte[] cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = new ArrayByteSequence(cf);
    this.cq = new ArrayByteSequence(cq);
    this.cv = EMPTY;
  }

  public Condition(ByteSequence cf, ByteSequence cq) {
    ArgumentChecker.notNull(cf, cq);
    this.cf = cf;
    this.cq = cq;
    this.cv = EMPTY;
  }

  public ByteSequence getFamily() {
    return cf;
  }
  
  public ByteSequence getQualifier() {
    return cq;
  }

  public Condition setTimestamp(long ts) {
    this.ts = ts;
    return this;
  }
  
  public Long getTimestamp() {
    return ts;
  }

  public Condition setValue(CharSequence value) {
    ArgumentChecker.notNull(value);
    this.val = new ArrayByteSequence(value.toString().getBytes(Constants.UTF8));
    return this;
  }

  public Condition setValue(byte[] value) {
    ArgumentChecker.notNull(value);
    this.val = new ArrayByteSequence(value);
    return this;
  }

  public ByteSequence getValue() {
    return val;
  }

  public Condition setVisibility(ColumnVisibility cv) {
    ArgumentChecker.notNull(cv);
    this.cv = new ArrayByteSequence(cv.getExpression());
    return this;
  }

  public ByteSequence getVisibility() {
    return cv;
  }

  public Condition setIterators(IteratorSetting... iterators) {
    ArgumentChecker.notNull(iterators);
    this.iterators = iterators;
    return this;
  }

  public IteratorSetting[] getIterators() {
    return iterators;
  }

}
