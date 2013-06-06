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

import java.util.Collections;
import java.util.Map.Entry;

import org.apache.accumulo.core.Constants;
import org.apache.accumulo.core.client.BatchWriter;
import org.apache.accumulo.core.client.BatchWriterConfig;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Mutation;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.LongCombiner.Type;
import org.apache.accumulo.core.iterators.user.SummingCombiner;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.core.security.ColumnVisibility;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

/**
 * 
 */
public class ConditionalWriterTest {
  
  private static String secret = "superSecret";
  public static TemporaryFolder folder = new TemporaryFolder();
  public static MiniAccumuloCluster cluster;
  
  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    folder.create();
    MiniAccumuloConfig cfg = new MiniAccumuloConfig(folder.newFolder("miniAccumulo"), secret);
    cluster = new MiniAccumuloCluster(cfg);
    cluster.start();
  }

  @Test
  public void testBasic() throws Exception {
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create("foo");

    ConditionalWriter cw = new ConditionalWriterImpl("foo", conn, Constants.NO_AUTHS);
    
    // mutation conditional on column tx:seq not exiting
    ConditionalMutation cm0 = new ConditionalMutation("99006");
    cm0.putConditionAbsent("tx", "seq", new ColumnVisibility());
    cm0.put("name", "last", "doe");
    cm0.put("name", "first", "john");
    cm0.put("tx", "seq", "1");
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm0).status);
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm0).status);

    // mutation conditional on column tx:seq being 1
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.putCondition("tx", "seq", new ColumnVisibility(), "1");
    cm1.put("name", "last", "Doe");
    cm1.put("tx", "seq", "2");
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm1).status);

    // test condition where value differs
    ConditionalMutation cm2 = new ConditionalMutation("99006");
    cm2.putCondition("tx", "seq", new ColumnVisibility(), "1");
    cm2.put("name", "last", "DOE");
    cm2.put("tx", "seq", "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm2).status);
    
    // test condition where column does not exists
    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.putCondition("txtypo", "seq", new ColumnVisibility(), "1"); // does not exists
    cm3.put("name", "last", "deo");
    cm3.put("tx", "seq", "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm3).status);
    
    // test two conditions, where one should fail
    ConditionalMutation cm4 = new ConditionalMutation("99006");
    cm4.putCondition("tx", "seq", new ColumnVisibility(), "2");
    cm4.putCondition("name", "last", new ColumnVisibility(), "doe");
    cm4.put("name", "last", "deo");
    cm4.put("tx", "seq", "3");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm4).status);

    // test two conditions, where one should fail
    ConditionalMutation cm5 = new ConditionalMutation("99006");
    cm5.putCondition("tx", "seq", new ColumnVisibility(), "1");
    cm5.putCondition("name", "last", new ColumnVisibility(), "Doe");
    cm5.put("name", "last", "deo");
    cm5.put("tx", "seq", "3");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm5).status);

    // ensure rejected mutations did not write
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    scanner.fetchColumn(new Text("name"), new Text("last"));
    scanner.setRange(new Range("99006"));
    Assert.assertEquals("Doe", scanner.iterator().next().getValue().toString());

    // test w/ two conditions that are met
    ConditionalMutation cm6 = new ConditionalMutation("99006");
    cm6.putCondition("tx", "seq", new ColumnVisibility(), "2");
    cm6.putCondition("name", "last", new ColumnVisibility(), "Doe");
    cm6.put("name", "last", "DOE");
    cm6.put("tx", "seq", "3");
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm6).status);
    
    Assert.assertEquals("DOE", scanner.iterator().next().getValue().toString());
    
    // test a conditional mutation that deletes
    ConditionalMutation cm7 = new ConditionalMutation("99006");
    cm7.putCondition("tx", "seq", new ColumnVisibility(), "3");
    cm7.putDelete("name", "last");
    cm7.putDelete("name", "first");
    cm7.putDelete("tx", "seq");
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm7).status);
    
    Assert.assertFalse(scanner.iterator().hasNext());

    // add the row back
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm0).status);
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm0).status);
    
    Assert.assertEquals("doe", scanner.iterator().next().getValue().toString());
  }
  
  @Test
  public void testFields() throws Exception {
    String table = "foo2";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    Authorizations auths = new Authorizations("A", "B");
    
    conn.securityOperations().changeUserAuthorizations("root", auths);
    
    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, auths);
    
    ColumnVisibility cva = new ColumnVisibility("A");
    ColumnVisibility cvb = new ColumnVisibility("B");
    
    ConditionalMutation cm0 = new ConditionalMutation("99006");
    cm0.putConditionAbsent("tx", "seq", cva);
    cm0.put("name", "last", cva, "doe");
    cm0.put("name", "first", cva, "john");
    cm0.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm0).status);
    
    Scanner scanner = conn.createScanner(table, auths);
    scanner.setRange(new Range("99006"));
    // TODO verify all columns
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    Entry<Key,Value> entry = scanner.iterator().next();
    Assert.assertEquals("1", entry.getValue().toString());
    long ts = entry.getKey().getTimestamp();
    
    // test wrong colf
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.putCondition("txA", "seq", cva, "1");
    cm1.put("name", "last", cva, "Doe");
    cm1.put("name", "first", cva, "John");
    cm1.put("tx", "seq", cva, "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm1).status);
    
    // test wrong colq
    ConditionalMutation cm2 = new ConditionalMutation("99006");
    cm2.putCondition("tx", "seqA", cva, "1");
    cm2.put("name", "last", cva, "Doe");
    cm2.put("name", "first", cva, "John");
    cm2.put("tx", "seq", cva, "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm2).status);
    
    // test wrong colv
    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.putCondition("tx", "seq", cvb, "1");
    cm3.put("name", "last", cva, "Doe");
    cm3.put("name", "first", cva, "John");
    cm3.put("tx", "seq", cva, "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm3).status);

    // test wrong timestamp
    ConditionalMutation cm4 = new ConditionalMutation("99006");
    cm4.putCondition("tx", "seq", cva, ts + 1, "1");
    cm4.put("name", "last", cva, "Doe");
    cm4.put("name", "first", cva, "John");
    cm4.put("tx", "seq", cva, "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm4).status);
    
    // test wrong timestamp
    ConditionalMutation cm5 = new ConditionalMutation("99006");
    cm5.putCondition("tx", "seq", cva, ts - 1, "1");
    cm5.put("name", "last", cva, "Doe");
    cm5.put("name", "first", cva, "John");
    cm5.put("tx", "seq", cva, "2");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm5).status);

    // ensure no updates were made
    entry = scanner.iterator().next();
    Assert.assertEquals("1", entry.getValue().toString());

    // set all columns correctly
    ConditionalMutation cm6 = new ConditionalMutation("99006");
    cm6.putCondition("tx", "seq", cva, ts, "1");
    cm6.put("name", "last", cva, "Doe");
    cm6.put("name", "first", cva, "John");
    cm6.put("tx", "seq", cva, "2");
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm6).status);

    entry = scanner.iterator().next();
    Assert.assertEquals("2", entry.getValue().toString());
    
    // TODO test each field w/ absence

  }

  @Test
  public void testBadColVis() throws Exception {
    // test when a user sets a col vis in a condition that can never be seen
    String table = "foo3";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    Authorizations auths = new Authorizations("A", "B");
    
    conn.securityOperations().changeUserAuthorizations("root", auths);

    Authorizations filteredAuths = new Authorizations("A");
    
    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, filteredAuths);
    
    ColumnVisibility cva = new ColumnVisibility("A");
    ColumnVisibility cvb = new ColumnVisibility("B");
    ColumnVisibility cvc = new ColumnVisibility("C");
    
    // User has authorization, but didn't include it in the writer
    ConditionalMutation cm0 = new ConditionalMutation("99006");
    cm0.putConditionAbsent("tx", "seq", cvb);
    cm0.put("name", "last", cva, "doe");
    cm0.put("name", "first", cva, "john");
    cm0.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm0).status);
    
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.putCondition("tx", "seq", cvb, "1");
    cm1.put("name", "last", cva, "doe");
    cm1.put("name", "first", cva, "john");
    cm1.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm1).status);

    // User does not have the authorization
    ConditionalMutation cm2 = new ConditionalMutation("99006");
    cm2.putConditionAbsent("tx", "seq", cvc);
    cm2.put("name", "last", cva, "doe");
    cm2.put("name", "first", cva, "john");
    cm2.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm2).status);
    
    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.putCondition("tx", "seq", cvc, "1");
    cm3.put("name", "last", cva, "doe");
    cm3.put("name", "first", cva, "john");
    cm3.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm3).status);

    // if any visibility is bad, good visibilities don't override
    ConditionalMutation cm4 = new ConditionalMutation("99006");
    cm4.putConditionAbsent("tx", "seq", cvb);
    cm4.putConditionAbsent("tx", "seq", cva);
    cm4.put("name", "last", cva, "doe");
    cm4.put("name", "first", cva, "john");
    cm4.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm4).status);
    
    ConditionalMutation cm5 = new ConditionalMutation("99006");
    cm5.putCondition("tx", "seq", cvb, "1");
    cm5.putCondition("tx", "seq", cva, "1");
    cm5.put("name", "last", cva, "doe");
    cm5.put("name", "first", cva, "john");
    cm5.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm5).status);

    ConditionalMutation cm6 = new ConditionalMutation("99006");
    cm6.putCondition("tx", "seq", cvb, "1");
    cm6.putConditionAbsent("tx", "seq", cva);
    cm6.put("name", "last", cva, "doe");
    cm6.put("name", "first", cva, "john");
    cm6.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm6).status);

    ConditionalMutation cm7 = new ConditionalMutation("99006");
    cm7.putConditionAbsent("tx", "seq", cvb);
    cm7.putCondition("tx", "seq", cva, "1");
    cm7.put("name", "last", cva, "doe");
    cm7.put("name", "first", cva, "john");
    cm7.put("tx", "seq", cva, "1");
    Assert.assertEquals(ConditionalWriter.Status.INVISIBLE_VISIBILITY, cw.write(cm7).status);
  }
  
  @Test
  public void testConstraints() {
    // ensure constraint violations are properly reported
  }

  @Test
  public void testIterators() throws Exception {
    String table = "foo4";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table, false);
    
    BatchWriter bw = conn.createBatchWriter(table, new BatchWriterConfig());
    
    Mutation m = new Mutation("ACCUMULO-1000");
    m.put("count", "comments", "1");
    bw.addMutation(m);
    bw.addMutation(m);
    bw.addMutation(m);
    bw.close();
    
    IteratorSetting iterConfig = new IteratorSetting(10, SummingCombiner.class);
    SummingCombiner.setEncodingType(iterConfig, Type.STRING);
    SummingCombiner.setColumns(iterConfig, Collections.singletonList(new IteratorSetting.Column("count")));
    
    Scanner scanner = conn.createScanner(table, new Authorizations());
    scanner.addScanIterator(iterConfig);
    scanner.setRange(new Range("ACCUMULO-1000"));
    scanner.fetchColumn(new Text("count"), new Text("comments"));
    
    Assert.assertEquals("3", scanner.iterator().next().getValue().toString());

    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations());
    
    ConditionalMutation cm0 = new ConditionalMutation("ACCUMULO-1000");
    cm0.putCondition("count", "comments", new ColumnVisibility(), "3");
    cm0.put("count", "comments", "1");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm0).status);
    Assert.assertEquals("3", scanner.iterator().next().getValue().toString());
    
    cw.addScanIterator(iterConfig);
    
    Assert.assertEquals(ConditionalWriter.Status.ACCEPTED, cw.write(cm0).status);
    Assert.assertEquals("4", scanner.iterator().next().getValue().toString());
    
    if (System.currentTimeMillis() % 2 == 0)
      cw.removeScanIterator(iterConfig.getName());
    else
      cw.clearScanIterators();

    ConditionalMutation cm1 = new ConditionalMutation("ACCUMULO-1000");
    cm1.putCondition("count", "comments", new ColumnVisibility(), "4");
    cm1.put("count", "comments", "1");
    Assert.assertEquals(ConditionalWriter.Status.REJECTED, cw.write(cm1).status);
    Assert.assertEquals("4", scanner.iterator().next().getValue().toString());
  }

  @Test
  public void testSecurity() {
    // test against table user does not have read and/or write permissions for
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
}
