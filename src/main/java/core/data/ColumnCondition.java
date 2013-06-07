package core.data;

import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
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


/**
 * 
 */
public class ColumnCondition {
  
  private ByteSequence cf;
  private ByteSequence cq;
  private ByteSequence cv;
  private ByteSequence val;
  private long ts;
  private boolean hasTs;
  
  public ColumnCondition(byte[] cf, byte[] cq, byte[] cv, byte[] val) {
    this.cf = new ArrayByteSequence(cf);
    this.cq = new ArrayByteSequence(cq);
    this.cv = new ArrayByteSequence(cv);
    this.val = val == null ? null : new ArrayByteSequence(val);
    this.ts = Long.MAX_VALUE;
    this.hasTs = false;
  }
  
  public ColumnCondition(byte[] cf, byte[] cq, byte[] cv, long ts, byte[] val) {
    this.cf = new ArrayByteSequence(cf);
    this.cq = new ArrayByteSequence(cq);
    this.cv = new ArrayByteSequence(cv);
    this.val = val == null ? null : new ArrayByteSequence(val);
    this.ts = ts;
    this.hasTs = true;
  }

  public int hashCode() {
    return cf.hashCode() + cq.hashCode() + cv.hashCode();
  }
  
  public boolean equals(Object o) {
    if (o instanceof ColumnCondition) {
      ColumnCondition occ = (ColumnCondition) o;
      return occ.cf.equals(cf) && occ.cq.equals(cq) && occ.cv.equals(cv) && (!hasTs || !occ.hasTs || ts == occ.ts);
    }
    return false;
  }
  
  public String toString() {
    return cf + " " + cq + " " + cv + " " + val + " " + hasTs;
  }
  
  public ByteSequence getColumnFamily() {
    return cf;
  }
  
  public ByteSequence getColumnQualifier() {
    return cq;
  }
  
  public ByteSequence getColumnVisibility() {
    return cv;
  }

  public ByteSequence getValue() {
    return val;
  }

  public long getTimestamp() {
    return ts;
  }

}
