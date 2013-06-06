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
}
