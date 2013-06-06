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

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.impl.ScannerOptions;
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

/**
 * 
 */
public class ConditionalWriterImpl implements ConditionalWriter {
  
  private static class ScanOpts extends ScannerOptions {
    public void configure(Scanner scanner) {
      setOptions((ScannerOptions) scanner, this);
    }
  }

  private Connector conn;
  private String table;
  private Authorizations auths;
  private VisibilityEvaluator ve;
  private LRUMap cache;
  private ScanOpts scanOpts = new ScanOpts() {};
  
  public ConditionalWriterImpl(String table, Connector conn, Authorizations auths) {
    this.conn = conn;
    this.table = table;
    this.auths = auths;
    
    this.ve = new VisibilityEvaluator(auths);
    this.cache = new LRUMap(1000);
  }
  
  public Iterator<Result> write(Iterator<ConditionalMutation> mutations) {
    
    ByteSequence absentVal = new ArrayByteSequence(new byte[0]);

    try {
      Scanner scanner = conn.createScanner(table, auths);
      BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
      
      ArrayList<Result> results = new ArrayList<Result>();
      
      mloop: while (mutations.hasNext()) {
        ConditionalMutation cm = mutations.next();
        
        byte[] row = cm.getRow();
        
        scanOpts.configure(scanner);

        scanner.setRange(new Range(new Text(row)));
        
        HashMap<ColumnCondition,ByteSequence> condtionMap = new HashMap<ColumnCondition,ByteSequence>();
        
        List<ColumnCondition> conditions = cm.getConditions();
        scanner.clearColumns();
        for (ColumnCondition cc : conditions) {
          
          if (!isVisible(cc.cv)) {
            results.add(new Result(Status.INVISIBLE_VISIBILITY, cm));
            continue mloop;
          }

          scanner.fetchColumn(new Text(cc.cf.toArray()), new Text(cc.cq.toArray()));
          condtionMap.put(cc, cc.val == null ? absentVal : cc.val);
        }

        for (Entry<Key,Value> entry : scanner) {
          Key k = entry.getKey();
          
          ColumnCondition cc = new ColumnCondition(k.getColumnFamilyData().toArray(), k.getColumnQualifierData().toArray(), k.getColumnVisibilityData()
              .toArray(), k.getTimestamp(), new byte[0]);
          
          ByteSequence val = condtionMap.remove(cc);
          
          if (val != null) {
            if (val == absentVal || !Arrays.equals(entry.getValue().get(), val.toArray())) {
              results.add(new Result(Status.REJECTED, cm));
              continue mloop;
            }
          }
        }

        int ac = 0;
        
        for (ByteSequence bs : condtionMap.values()) {
          if (bs == absentVal) {
            ac++;
          }
        }
        
        if (condtionMap.size() != ac) {
          results.add(new Result(Status.REJECTED, cm));
        } else {
          results.add(new Result(Status.ACCEPTED, cm));
          bw.addMutation(cm);
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
  
  public void addScanIterator(IteratorSetting cfg) {
    scanOpts.addScanIterator(cfg);
  }
  
  public void removeScanIterator(String iteratorName) {
    scanOpts.removeScanIterator(iteratorName);
  }
  
  public void updateScanIteratorOption(String iteratorName, String key, String value) {
    scanOpts.updateScanIteratorOption(iteratorName, key, value);
  }
  
  public void clearScanIterators() {
    scanOpts.clearScanIterators();
  }
  
  public void setTimeout(long timeOut, TimeUnit timeUnit) {
    scanOpts.setTimeout(timeOut, timeUnit);
  }
  
  public long getTimeout(TimeUnit timeUnit) {
    return scanOpts.getTimeout(timeUnit);
  }
  
}
