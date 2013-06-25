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

package core.client.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.MutationsRejectedException;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.ArrayByteSequence;
import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.security.VisibilityEvaluator;
import org.apache.accumulo.core.security.VisibilityParseException;
import org.apache.accumulo.core.util.BadArgumentException;
import org.apache.commons.collections.map.LRUMap;
import org.apache.hadoop.io.Text;

import core.client.ConditionalWriter;
import core.data.Condition;
import core.data.ConditionalMutation;

/**
 * 
 */
public class ConditionalWriterImpl implements ConditionalWriter {
  
  private Connector conn;
  private String table;
  private Authorizations auths;
  private VisibilityEvaluator ve;
  private LRUMap cache;
  
  public ConditionalWriterImpl(String table, Connector conn, Authorizations auths) {
    this.conn = conn;
    this.table = table;
    this.auths = auths;
    
    this.ve = new VisibilityEvaluator(auths);
    this.cache = new LRUMap(1000);
  }
  
  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    
    try {
      Scanner scanner = conn.createScanner(table, auths);
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      
      ArrayList<Result> results = new ArrayList<Result>();
      
      mloop: while (mutations.hasNext()) {
        ConditionalMutation cm = mutations.next();
        
        byte[] row = cm.getRow();
        
        List<Condition> conditions = cm.getConditions();

        for (Condition cc : conditions) {
          
          if (!isVisible(cc.getVisibility())) {
            results.add(new Result(Status.INVISIBLE_VISIBILITY, cm));
            continue mloop;
          }

          scanner.clearColumns();
          
          if (cc.getTimestamp() == null) {
            scanner.setRange(Range.exact(new Text(row), new Text(cc.getFamily().toArray()), new Text(cc.getQualifier().toArray()), new Text(cc.getVisibility()
                .toArray())));
          } else {
            scanner.setRange(Range.exact(new Text(row), new Text(cc.getFamily().toArray()), new Text(cc.getQualifier().toArray()), new Text(cc.getVisibility()
                .toArray()), cc.getTimestamp()));
          }

          scanner.clearScanIterators();
          for (IteratorSetting is : cc.getIterators()) {
            scanner.addScanIterator(is);
          }

          Value val = null;
          
          for (Entry<Key,Value> entry : scanner) {
            val = entry.getValue();
            break;
          }
          
          // TODO check key == condition columns

          if ((val == null ^ cc.getValue() == null) || (val != null && !cc.getValue().equals(new ArrayByteSequence(val.get())))) {
            results.add(new Result(Status.REJECTED, cm));
            continue mloop;
          }
        }

        try {
          bw.addMutation(cm);
          bw.close();
          results.add(new Result(Status.ACCEPTED, cm));
        } catch (MutationsRejectedException mre) {
          results.add(new Result(Status.VIOLATED, cm));
          continue mloop;
        } finally {
          bw.close();
          bw = conn.createBatchWriter(table, new BatchWriterConfig());
        }
      }

      bw.close();
      
      return results.iterator();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
  
  private boolean isVisible(ByteSequence cv) {
    Text testVis = new Text(cv.toArray());
    if (testVis.getLength() == 0)
      return true;
    
    Boolean b = (Boolean) cache.get(testVis);
    if (b != null)
      return b;
    
    try {
      Boolean bb = ve.evaluate(new ColumnVisibility(testVis));
      cache.put(new Text(testVis), bb);
      return bb;
    } catch (VisibilityParseException e) {
      return false;
    } catch (BadArgumentException e) {
      return false;
    }
  }

  public Result write(ConditionalMutation mutation) {
    return write(Collections.singleton(mutation).iterator()).next();
  }
  
  public void setTimeout(long timeOut, TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }
  
  public long getTimeout(TimeUnit timeUnit) {
    throw new UnsupportedOperationException();
  }
  
}
