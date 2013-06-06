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
import java.util.List;

import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.core.util.ArgumentChecker;
import org.apache.hadoop.io.Text;

/**
 * since 1.6.0
 */
public class ConditionalMutation extends Mutation {
  
  private ArrayList<ColumnCondition> conditions = new ArrayList<ColumnCondition>();

  public ConditionalMutation(byte[] row) {
    this(row, 0, row.length);
  }
  
  public ConditionalMutation(byte[] row, int start, int length) {
    super(row, start, length);
  }
  
  public ConditionalMutation(Text row) {
    this(row.getBytes(), 0, row.getLength());
  }
  
  public ConditionalMutation(CharSequence row) {
    this(new Text(row.toString()));
  }
  
  public void putCondition(CharSequence cf, CharSequence cq, ColumnVisibility cv, CharSequence val) {
    ArgumentChecker.notNull(cf, cq, cv, val);
    conditions.add(new ColumnCondition(cf.toString().getBytes(), cq.toString().getBytes(), cv.getExpression(), val.toString().getBytes()));
  }
  
  public void putCondition(CharSequence cf, CharSequence cq, ColumnVisibility cv, long ts, CharSequence val) {
    ArgumentChecker.notNull(cf, cq, cv, val);
    conditions.add(new ColumnCondition(cf.toString().getBytes(), cq.toString().getBytes(), cv.getExpression(), ts, val.toString().getBytes()));
  }

  public void putConditionAbsent(CharSequence cf, CharSequence cq, ColumnVisibility cv) {
    ArgumentChecker.notNull(cf, cq, cv);
    conditions.add(new ColumnCondition(cf.toString().getBytes(), cq.toString().getBytes(), cv.getExpression(), null));
  }
  
  public void putConditionAbsent(CharSequence cf, CharSequence cq, ColumnVisibility cv, long ts) {
    ArgumentChecker.notNull(cf, cq, cv);
    conditions.add(new ColumnCondition(cf.toString().getBytes(), cq.toString().getBytes(), cv.getExpression(), ts, null));
  }

  public List<ColumnCondition> getConditions() {
    return conditions;
  }

}
