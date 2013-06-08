package core.client;
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
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map.Entry;
import java.util.TreeSet;

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
import org.apache.accumulo.examples.simple.constraints.AlphaNumKeyConstraint;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.accumulo.minicluster.MiniAccumuloConfig;
import org.apache.hadoop.io.Text;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import core.client.ConditionalWriter.Result;
import core.client.ConditionalWriter.Status;
import core.client.impl.ConditionalWriterImpl;
import core.data.Condition;
import core.data.ConditionalMutation;

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
    cm0.addCondition(new Condition("tx", "seq"));
    cm0.put("name", "last", "doe");
    cm0.put("name", "first", "john");
    cm0.put("tx", "seq", "1");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
    Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());

    // mutation conditional on column tx:seq being 1
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.addCondition(new Condition("tx", "seq").setValue("1"));
    cm1.put("name", "last", "Doe");
    cm1.put("tx", "seq", "2");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());

    // test condition where value differs
    ConditionalMutation cm2 = new ConditionalMutation("99006");
    cm2.addCondition(new Condition("tx", "seq").setValue("1"));
    cm2.put("name", "last", "DOE");
    cm2.put("tx", "seq", "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm2).getStatus());
    
    // test condition where column does not exists
    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.addCondition(new Condition("txtypo", "seq").setValue("1")); // does not exists
    cm3.put("name", "last", "deo");
    cm3.put("tx", "seq", "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm3).getStatus());
    
    // test two conditions, where one should fail
    ConditionalMutation cm4 = new ConditionalMutation("99006");
    cm4.addCondition(new Condition("tx", "seq").setValue("2"));
    cm4.addCondition(new Condition("name", "last").setValue("doe"));
    cm4.put("name", "last", "deo");
    cm4.put("tx", "seq", "3");
    Assert.assertEquals(Status.REJECTED, cw.write(cm4).getStatus());

    // test two conditions, where one should fail
    ConditionalMutation cm5 = new ConditionalMutation("99006");
    cm5.addCondition(new Condition("tx", "seq").setValue("1"));
    cm5.addCondition(new Condition("name", "last").setValue("Doe"));
    cm5.put("name", "last", "deo");
    cm5.put("tx", "seq", "3");
    Assert.assertEquals(Status.REJECTED, cw.write(cm5).getStatus());

    // ensure rejected mutations did not write
    Scanner scanner = conn.createScanner("foo", Constants.NO_AUTHS);
    scanner.fetchColumn(new Text("name"), new Text("last"));
    scanner.setRange(new Range("99006"));
    Assert.assertEquals("Doe", scanner.iterator().next().getValue().toString());

    // test w/ two conditions that are met
    ConditionalMutation cm6 = new ConditionalMutation("99006");
    cm6.addCondition(new Condition("tx", "seq").setValue("2"));
    cm6.addCondition(new Condition("name", "last").setValue("Doe"));
    cm6.put("name", "last", "DOE");
    cm6.put("tx", "seq", "3");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());
    
    Assert.assertEquals("DOE", scanner.iterator().next().getValue().toString());
    
    // test a conditional mutation that deletes
    ConditionalMutation cm7 = new ConditionalMutation("99006");
    cm7.addCondition(new Condition("tx", "seq").setValue("3"));
    cm7.putDelete("name", "last");
    cm7.putDelete("name", "first");
    cm7.putDelete("tx", "seq");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm7).getStatus());
    
    Assert.assertFalse(scanner.iterator().hasNext());

    // add the row back
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
    Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
    
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
    cm0.addCondition(new Condition("tx", "seq").setVisibility(cva));
    cm0.put("name", "last", cva, "doe");
    cm0.put("name", "first", cva, "john");
    cm0.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm0).getStatus());
    
    Scanner scanner = conn.createScanner(table, auths);
    scanner.setRange(new Range("99006"));
    // TODO verify all columns
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    Entry<Key,Value> entry = scanner.iterator().next();
    Assert.assertEquals("1", entry.getValue().toString());
    long ts = entry.getKey().getTimestamp();
    
    // test wrong colf
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.addCondition(new Condition("txA", "seq").setVisibility(cva).setValue("1"));
    cm1.put("name", "last", cva, "Doe");
    cm1.put("name", "first", cva, "John");
    cm1.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
    
    // test wrong colq
    ConditionalMutation cm2 = new ConditionalMutation("99006");
    cm2.addCondition(new Condition("tx", "seqA").setVisibility(cva).setValue("1"));
    cm2.put("name", "last", cva, "Doe");
    cm2.put("name", "first", cva, "John");
    cm2.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm2).getStatus());
    
    // test wrong colv
    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.addCondition(new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
    cm3.put("name", "last", cva, "Doe");
    cm3.put("name", "first", cva, "John");
    cm3.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm3).getStatus());

    // test wrong timestamp
    ConditionalMutation cm4 = new ConditionalMutation("99006");
    cm4.addCondition(new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts + 1).setValue("1"));
    cm4.put("name", "last", cva, "Doe");
    cm4.put("name", "first", cva, "John");
    cm4.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm4).getStatus());
    
    // test wrong timestamp
    ConditionalMutation cm5 = new ConditionalMutation("99006");
    cm5.addCondition(new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts - 1).setValue("1"));
    cm5.put("name", "last", cva, "Doe");
    cm5.put("name", "first", cva, "John");
    cm5.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.REJECTED, cw.write(cm5).getStatus());

    // ensure no updates were made
    entry = scanner.iterator().next();
    Assert.assertEquals("1", entry.getValue().toString());

    // set all columns correctly
    ConditionalMutation cm6 = new ConditionalMutation("99006");
    cm6.addCondition(new Condition("tx", "seq").setVisibility(cva).setTimestamp(ts).setValue("1"));
    cm6.put("name", "last", cva, "Doe");
    cm6.put("name", "first", cva, "John");
    cm6.put("tx", "seq", cva, "2");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm6).getStatus());

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
    cm0.addCondition(new Condition("tx", "seq").setVisibility(cvb));
    cm0.put("name", "last", cva, "doe");
    cm0.put("name", "first", cva, "john");
    cm0.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm0).getStatus());
    
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.addCondition(new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
    cm1.put("name", "last", cva, "doe");
    cm1.put("name", "first", cva, "john");
    cm1.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm1).getStatus());

    // User does not have the authorization
    ConditionalMutation cm2 = new ConditionalMutation("99006");
    cm2.addCondition(new Condition("tx", "seq").setVisibility(cvc));
    cm2.put("name", "last", cva, "doe");
    cm2.put("name", "first", cva, "john");
    cm2.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm2).getStatus());
    
    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.addCondition(new Condition("tx", "seq").setVisibility(cvc).setValue("1"));
    cm3.put("name", "last", cva, "doe");
    cm3.put("name", "first", cva, "john");
    cm3.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm3).getStatus());

    // if any visibility is bad, good visibilities don't override
    ConditionalMutation cm4 = new ConditionalMutation("99006");
    cm4.addCondition(new Condition("tx", "seq").setVisibility(cvb));
    cm4.addCondition(new Condition("tx", "seq").setVisibility(cva));
    cm4.put("name", "last", cva, "doe");
    cm4.put("name", "first", cva, "john");
    cm4.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm4).getStatus());
    
    ConditionalMutation cm5 = new ConditionalMutation("99006");
    cm5.addCondition(new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
    cm5.addCondition(new Condition("tx", "seq").setVisibility(cva).setValue("1"));
    cm5.put("name", "last", cva, "doe");
    cm5.put("name", "first", cva, "john");
    cm5.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm5).getStatus());

    ConditionalMutation cm6 = new ConditionalMutation("99006");
    cm6.addCondition(new Condition("tx", "seq").setVisibility(cvb).setValue("1"));
    cm6.addCondition(new Condition("tx", "seq").setVisibility(cva));
    cm6.put("name", "last", cva, "doe");
    cm6.put("name", "first", cva, "john");
    cm6.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm6).getStatus());

    ConditionalMutation cm7 = new ConditionalMutation("99006");
    cm7.addCondition(new Condition("tx", "seq").setVisibility(cvb));
    cm7.addCondition(new Condition("tx", "seq").setVisibility(cva).setValue("1"));
    cm7.put("name", "last", cva, "doe");
    cm7.put("name", "first", cva, "john");
    cm7.put("tx", "seq", cva, "1");
    Assert.assertEquals(Status.INVISIBLE_VISIBILITY, cw.write(cm7).getStatus());
  }
  
  @Test
  public void testConstraints() throws Exception {
    // ensure constraint violations are properly reported
    String table = "foo5";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    conn.tableOperations().addConstraint(table, AlphaNumKeyConstraint.class.getName());
    conn.tableOperations().clone(table, table + "_clone", true, new HashMap<String,String>(), new HashSet<String>());
    
    Scanner scanner = conn.createScanner(table + "_clone", new Authorizations());

    ConditionalWriter cw = new ConditionalWriterImpl(table + "_clone", conn, new Authorizations());

    ConditionalMutation cm0 = new ConditionalMutation("99006+");
    cm0.addCondition(new Condition("tx", "seq"));
    cm0.put("tx", "seq", "1");
    
    Assert.assertEquals(Status.VIOLATED, cw.write(cm0).getStatus());
    Assert.assertFalse(scanner.iterator().hasNext());
    
    ConditionalMutation cm1 = new ConditionalMutation("99006");
    cm1.addCondition(new Condition("tx", "seq"));
    cm1.put("tx", "seq", "1");
    
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
    Assert.assertTrue(scanner.iterator().hasNext());

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
    cm0.addCondition(new Condition("count", "comments").setValue("3"));
    cm0.put("count", "comments", "1");
    Assert.assertEquals(Status.REJECTED, cw.write(cm0).getStatus());
    Assert.assertEquals("3", scanner.iterator().next().getValue().toString());
    
    ConditionalMutation cm1 = new ConditionalMutation("ACCUMULO-1000");
    cm1.addCondition(new Condition("count", "comments").setIterators(iterConfig).setValue("3"));
    cm1.put("count", "comments", "1");
    Assert.assertEquals(Status.ACCEPTED, cw.write(cm1).getStatus());
    Assert.assertEquals("4", scanner.iterator().next().getValue().toString());
    
    ConditionalMutation cm2 = new ConditionalMutation("ACCUMULO-1000");
    cm2.addCondition(new Condition("count", "comments").setValue("4"));
    cm2.put("count", "comments", "1");
    Assert.assertEquals(Status.REJECTED, cw.write(cm1).getStatus());
    Assert.assertEquals("4", scanner.iterator().next().getValue().toString());
    
    // TODO test conditions with different iterators
  }

  @Test
  public void testBatch() throws Exception {
    String table = "foo6";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    
    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B"));
    
    ColumnVisibility cvab = new ColumnVisibility("A|B");
    
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    ConditionalMutation cm0 = new ConditionalMutation("99006");
    cm0.addCondition(new Condition("tx", "seq").setVisibility(cvab));
    cm0.put("name", "last", cvab, "doe");
    cm0.put("name", "first", cvab, "john");
    cm0.put("tx", "seq", cvab, "1");
    mutations.add(cm0);
    
    ConditionalMutation cm1 = new ConditionalMutation("59056");
    cm1.addCondition(new Condition("tx", "seq").setVisibility(cvab));
    cm1.put("name", "last", cvab, "doe");
    cm1.put("name", "first", cvab, "jane");
    cm1.put("tx", "seq", cvab, "1");
    mutations.add(cm1);
    
    ConditionalMutation cm2 = new ConditionalMutation("19059");
    cm2.addCondition(new Condition("tx", "seq").setVisibility(cvab));
    cm2.put("name", "last", cvab, "doe");
    cm2.put("name", "first", cvab, "jack");
    cm2.put("tx", "seq", cvab, "1");
    mutations.add(cm2);
    
    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations("A"));
    Iterator<Result> results = cw.write(mutations.iterator());
    int count = 0;
    while (results.hasNext()) {
      Result result = results.next();
      Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      count++;
    }
    
    Assert.assertEquals(3, count);

    Scanner scanner = conn.createScanner(table, new Authorizations("A"));
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    
    for (String row : new String[] {"99006", "59056", "19059"}) {
      scanner.setRange(new Range(row));
      Assert.assertEquals("1", scanner.iterator().next().getValue().toString());
    }

    TreeSet<Text> splits = new TreeSet<Text>();
    splits.add(new Text("7"));
    splits.add(new Text("3"));
    conn.tableOperations().addSplits(table, splits);

    mutations.clear();

    ConditionalMutation cm3 = new ConditionalMutation("99006");
    cm3.addCondition(new Condition("tx", "seq").setVisibility(cvab).setValue("1"));
    cm3.put("name", "last", cvab, "Doe");
    cm3.put("tx", "seq", cvab, "2");
    mutations.add(cm3);
    
    ConditionalMutation cm4 = new ConditionalMutation("59056");
    cm4.addCondition(new Condition("tx", "seq").setVisibility(cvab));
    cm4.put("name", "last", cvab, "Doe");
    cm4.put("tx", "seq", cvab, "1");
    mutations.add(cm4);
    
    ConditionalMutation cm5 = new ConditionalMutation("19059");
    cm5.addCondition(new Condition("tx", "seq").setVisibility(cvab).setValue("2"));
    cm5.put("name", "last", cvab, "Doe");
    cm5.put("tx", "seq", cvab, "3");
    mutations.add(cm5);

    results = cw.write(mutations.iterator());
    int accepted = 0;
    int rejected = 0;
    while (results.hasNext()) {
      Result result = results.next();
      if (new String(result.getMutation().getRow()).equals("99006")) {
        Assert.assertEquals(Status.ACCEPTED, result.getStatus());
        accepted++;
      } else {
        Assert.assertEquals(Status.REJECTED, result.getStatus());
        rejected++;
      }
    }
    
    Assert.assertEquals(1, accepted);
    Assert.assertEquals(2, rejected);

    for (String row : new String[] {"59056", "19059"}) {
      scanner.setRange(new Range(row));
      Assert.assertEquals("1", scanner.iterator().next().getValue().toString());
    }
    
    scanner.setRange(new Range("99006"));
    Assert.assertEquals("2", scanner.iterator().next().getValue().toString());

    scanner.clearColumns();
    scanner.fetchColumn(new Text("name"), new Text("last"));
    Assert.assertEquals("Doe", scanner.iterator().next().getValue().toString());
  }
  
  @Test
  public void testBatchErrors() throws Exception {
    
    String table = "foo7";
    
    ZooKeeperInstance zki = new ZooKeeperInstance(cluster.getInstanceName(), cluster.getZooKeepers());
    Connector conn = zki.getConnector("root", new PasswordToken(secret));
    
    conn.tableOperations().create(table);
    conn.tableOperations().addConstraint(table, AlphaNumKeyConstraint.class.getName());
    conn.tableOperations().clone(table, table + "_clone", true, new HashMap<String,String>(), new HashSet<String>());

    conn.securityOperations().changeUserAuthorizations("root", new Authorizations("A", "B"));
    
    ColumnVisibility cvaob = new ColumnVisibility("A|B");
    ColumnVisibility cvaab = new ColumnVisibility("A&B");
    
    ArrayList<ConditionalMutation> mutations = new ArrayList<ConditionalMutation>();
    
    ConditionalMutation cm0 = new ConditionalMutation("99006");
    cm0.addCondition(new Condition("tx", "seq").setVisibility(cvaob));
    cm0.put("name+", "last", cvaob, "doe");
    cm0.put("name", "first", cvaob, "john");
    cm0.put("tx", "seq", cvaob, "1");
    mutations.add(cm0);
    
    ConditionalMutation cm1 = new ConditionalMutation("59056");
    cm1.addCondition(new Condition("tx", "seq").setVisibility(cvaab));
    cm1.put("name", "last", cvaab, "doe");
    cm1.put("name", "first", cvaab, "jane");
    cm1.put("tx", "seq", cvaab, "1");
    mutations.add(cm1);
    
    ConditionalMutation cm2 = new ConditionalMutation("19059");
    cm2.addCondition(new Condition("tx", "seq").setVisibility(cvaob));
    cm2.put("name", "last", cvaob, "doe");
    cm2.put("name", "first", cvaob, "jack");
    cm2.put("tx", "seq", cvaob, "1");
    mutations.add(cm2);
    
    ConditionalMutation cm3 = new ConditionalMutation("90909");
    cm3.addCondition(new Condition("tx", "seq").setVisibility(cvaob).setValue("1"));
    cm3.put("name", "last", cvaob, "doe");
    cm3.put("name", "first", cvaob, "john");
    cm3.put("tx", "seq", cvaob, "2");
    mutations.add(cm3);

    ConditionalWriter cw = new ConditionalWriterImpl(table, conn, new Authorizations("A"));
    Iterator<Result> results = cw.write(mutations.iterator());
    HashSet<String> rows = new HashSet<String>();
    while (results.hasNext()) {
      Result result = results.next();
      String row = new String(result.getMutation().getRow());
      if (row.equals("19059")) {
        Assert.assertEquals(Status.ACCEPTED, result.getStatus());
      } else if (row.equals("59056")) {
        Assert.assertEquals(Status.INVISIBLE_VISIBILITY, result.getStatus());
      } else if (row.equals("99006")) {
        Assert.assertEquals(Status.VIOLATED, result.getStatus());
      } else if (row.equals("90909")) {
        Assert.assertEquals(Status.REJECTED, result.getStatus());
      }
      rows.add(row);
    }
    
    Assert.assertEquals(4, rows.size());

    Scanner scanner = conn.createScanner(table, new Authorizations("A"));
    scanner.fetchColumn(new Text("tx"), new Text("seq"));
    
    Iterator<Entry<Key,Value>> iter = scanner.iterator();
    Assert.assertEquals("1", iter.next().getValue().toString());
    Assert.assertFalse(iter.hasNext());

  }
  
  @Test
  public void testSecurity() {
    // test against table user does not have read and/or write permissions for
  }

  @Test
  public void testTimeout() {
    
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    cluster.stop();
    folder.delete();
  }
}
