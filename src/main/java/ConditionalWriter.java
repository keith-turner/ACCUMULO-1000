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

import java.util.Iterator;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;

/**
 * 
 */
public interface ConditionalWriter {
  public static class Result {

    Status status;
    ConditionalMutation mutation;
    
    Result(Status s, ConditionalMutation m) {
      this.status = s;
      this.mutation = m;
    }
  }
  
  public static enum Status {
    // conditions were met and mutation was written
    ACCEPTED,
    // conditions were not met and mutation was not written
    REJECTED,
    // mutation violated a constraint
    VIOLATED,
    // error occurred after mutation was sent to server, its unknown if the
    // mutation was written
    UNKNOWN,
    // A condition contained a column visibility that could never be seen
    INVALID_VISIBILITY,
    // nothing was done with this mutation, this is caused by previous
    // mutations failing in some way like timing out
    IGNORED
  }
  
  public abstract Iterator<Result> write(Iterator<ConditionalMutation> mutations);
  
  public abstract Result write(ConditionalMutation mutation);
  
  /**
   * Add a server-side scan iterator.
   * 
   * @param cfg
   *          fully specified scan-time iterator, including all options for the iterator. Any changes to the iterator setting after this call are not propagated
   *          to the stored iterator.
   * @throws IllegalArgumentException
   *           if the setting conflicts with existing iterators
   */
  public void addScanIterator(IteratorSetting cfg);
  
  /**
   * Remove an iterator from the list of iterators.
   * 
   * @param iteratorName
   *          nickname used for the iterator
   */
  public void removeScanIterator(String iteratorName);
  
  /**
   * Update the options for an iterator. Note that this does <b>not</b> change the iterator options during a scan, it just replaces the given option on a
   * configured iterator before a scan is started.
   * 
   * @param iteratorName
   *          the name of the iterator to change
   * @param key
   *          the name of the option
   * @param value
   *          the new value for the named option
   */
  public void updateScanIteratorOption(String iteratorName, String key, String value);
  
  /**
   * Clears scan iterators prior to returning a scanner to the pool.
   */
  public void clearScanIterators();
  
  /**
   * This setting determines how long a scanner will automatically retry when a failure occurs. By default a scanner will retry forever.
   * 
   * Setting to zero or Long.MAX_VALUE and TimeUnit.MILLISECONDS means to retry forever.
   * 
   * @param timeOut
   * @param timeUnit
   *          determines how timeout is interpreted
   */
  public void setTimeout(long timeOut, TimeUnit timeUnit);
  
  /**
   * Returns the setting for how long a scanner will automatically retry when a failure occurs.
   * 
   * @return the timeout configured for this scanner
   */
  public long getTimeout(TimeUnit timeUnit);

}
