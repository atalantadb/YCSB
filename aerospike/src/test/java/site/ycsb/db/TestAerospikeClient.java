/**
 * Copyright (c) 2026 YCSB contributors. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.db;

import com.aerospike.client.BatchRecord;
import com.aerospike.client.BatchWrite;
import com.aerospike.client.Bin;
import com.aerospike.client.IAerospikeClient;
import com.aerospike.client.Key;
import com.aerospike.client.Operation;
import com.aerospike.client.Record;
import com.aerospike.client.policy.Policy;
import com.aerospike.client.policy.RecordExistsAction;
import org.testng.annotations.AfterMethod;
import org.testng.annotations.BeforeMethod;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertNull;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

/**
 * Regression tests for key compatibility, read results, and batch configuration.
 */
public class TestAerospikeClient {
  private AerospikeClient binding;
  private CapturingClient backend;

  @BeforeMethod
  public void setUp() throws Exception {
    binding = new AerospikeClient();
    backend = new CapturingClient();
    IAerospikeClient fake = (IAerospikeClient) Proxy.newProxyInstance(
        IAerospikeClient.class.getClassLoader(), new Class<?>[]{IAerospikeClient.class}, backend);
    field("sharedClient").set(null, fake);
    field("clientUsers").setInt(null, 0);
  }

  @AfterMethod(alwaysRun = true)
  public void tearDown() throws Exception {
    binding.cleanup();
    field("sharedClient").set(null, null);
    field("clientUsers").setInt(null, 0);
  }

  private static Field field(String name) throws Exception {
    Field reflected = AerospikeClient.class.getDeclaredField(name);
    reflected.setAccessible(true);
    return reflected;
  }

  private void init(String... options) throws DBException {
    Properties props = new Properties();
    for (int i = 0; i < options.length; i += 2) {
      props.setProperty(options[i], options[i + 1]);
    }
    binding.setProperties(props);
    binding.init();
  }

  private static Map<String, ByteIterator> values() {
    Map<String, ByteIterator> values = new HashMap<String, ByteIterator>();
    values.put("field0", new ByteArrayByteIterator(new byte[]{0, 1, (byte) 255}));
    return values;
  }

  private static Record record(byte... bytes) {
    Map<String, Object> bins = new HashMap<String, Object>();
    bins.put("field0", bytes);
    return new Record(bins, 1, 0);
  }

  private static void assertKey(Key key, String namespace, String table, String userKey) {
    assertEquals(key.namespace, namespace);
    assertEquals(key.setName, table);
    assertEquals(key.userKey.getObject(), userKey);
  }

  @Test
  public void singleRecordCrudUsesOriginalKeyAndOperationTable() throws Exception {
    init("table", "configured-set", "as.set", "legacy-set");
    assertEquals(binding.insert("insert-set", "existing-key", values()), Status.OK);
    assertKey((Key) backend.arguments[1], "ycsb", "insert-set", "existing-key");
    assertEquals(((Bin[]) backend.arguments[2])[0].value.getObject(), new byte[]{0, 1, (byte) 255});

    backend.readResult = record((byte) 0, (byte) 1, (byte) 255);
    Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    assertEquals(binding.read("read-set", "existing-key", null, result), Status.OK);
    assertKey((Key) backend.arguments[1], "ycsb", "read-set", "existing-key");
    assertEquals(result.get("field0").toArray(), new byte[]{0, 1, (byte) 255});

    assertEquals(binding.update("update-set", "existing-key", values()), Status.OK);
    assertKey((Key) backend.arguments[1], "ycsb", "update-set", "existing-key");
    assertEquals(binding.delete("delete-set", "existing-key"), Status.OK);
    assertKey((Key) backend.arguments[1], "ycsb", "delete-set", "existing-key");
  }

  @Test
  public void batchWritesUseDistinctKeysAndIdenticalPayloads() throws Exception {
    init("batchsize", "3");
    assertEquals(binding.insert("batch-set", "user42", values()), Status.OK);
    assertBatchWrite(RecordExistsAction.CREATE_ONLY, Operation.Type.WRITE);
    assertEquals(binding.update("batch-set", "user42", values()), Status.OK);
    assertBatchWrite(RecordExistsAction.UPDATE_ONLY, Operation.Type.WRITE);
    assertEquals(binding.delete("batch-set", "user42"), Status.OK);
    assertBatchWrite(RecordExistsAction.UPDATE, Operation.Type.DELETE);
  }

  @SuppressWarnings("unchecked")
  private void assertBatchWrite(RecordExistsAction action, Operation.Type type) {
    assertEquals(backend.methodName, "operate");
    List<BatchRecord> records = (List<BatchRecord>) backend.arguments[1];
    assertEquals(records.size(), 3);
    for (int i = 0; i < records.size(); i++) {
      BatchWrite write = (BatchWrite) records.get(i);
      assertKey(write.key, "ycsb", "batch-set", "user42_" + i);
      assertEquals(write.policy.recordExistsAction, action);
      assertEquals(write.ops.length, 1);
      assertEquals(write.ops[0].type, type);
      if (type == Operation.Type.WRITE) {
        assertEquals(write.ops[0].value.getObject(), new byte[]{0, 1, (byte) 255});
      }
    }
  }

  @Test
  public void batchReadReturnsFirstRecordAndPassesFieldProjection() throws Exception {
    init("batchsize", "2");
    backend.batchReadResult = new Record[]{record((byte) 7), record((byte) 8)};
    Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    assertEquals(binding.read("read-set", "user42", Collections.singleton("field0"), result), Status.OK);
    Key[] keys = (Key[]) backend.arguments[1];
    assertKey(keys[0], "ycsb", "read-set", "user42_0");
    assertKey(keys[1], "ycsb", "read-set", "user42_1");
    assertEquals(backend.arguments[2], new String[]{"field0"});
    assertEquals(result.size(), 1);
    assertEquals(result.get("field0").toArray(), new byte[]{7});
  }

  @Test
  public void emptyProjectionCanReturnHeaderWithoutBins() throws Exception {
    init();
    backend.readResult = new Record(null, 1, 0);
    Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    assertEquals(binding.read("read-set", "user42", Collections.<String>emptySet(), result), Status.OK);
    assertEquals(backend.arguments[2], new String[0]);
    assertTrue(result.isEmpty());
  }

  @Test
  public void missingRecordReturnsErrorWithoutPopulatingResult() throws Exception {
    init("batchsize", "2");
    backend.batchReadResult = new Record[]{record((byte) 7), null};
    Map<String, ByteIterator> result = new HashMap<String, ByteIterator>();
    assertEquals(binding.read("read-set", "user42", null, result), Status.ERROR);
    assertTrue(result.isEmpty());
  }

  @Test
  public void partialBatchWriteReturnsError() throws Exception {
    init("batchsize", "2");
    backend.writeSuccess = false;
    assertEquals(binding.insert("batch-set", "user42", values()), Status.ERROR);
    assertEquals(binding.update("batch-set", "user42", values()), Status.ERROR);
    assertEquals(binding.delete("batch-set", "user42"), Status.ERROR);
  }

  @DataProvider(name = "namespaces")
  public Object[][] namespaces() {
    return new Object[][]{
        {new String[0], "ycsb"},
        {new String[]{"namespace", "alias-ns"}, "alias-ns"},
        {new String[]{"as.namespace", "legacy-ns"}, "legacy-ns"},
        {new String[]{"as.namespace", "legacy-ns", "namespace", "alias-ns"}, "legacy-ns"}
    };
  }

  @Test(dataProvider = "namespaces")
  public void namespacePrecedence(String[] options, String expected) throws Exception {
    init(options);
    binding.insert("table", "key", values());
    assertKey((Key) backend.arguments[1], expected, "table", "key");
  }

  @DataProvider(name = "timeouts")
  public Object[][] timeouts() {
    return new Object[][]{
        {new String[0], 10000, 10000, 10000, 10000},
        {new String[]{"as.timeout", "5000"}, 5000, 5000, 5000, 5000},
        {new String[]{"as.timeout", "5000", "as.batch.totalTimeout", "20000"}, 5000, 5000, 5000, 20000},
        {new String[]{"as.timeout", "5000", "as.single.socketTimeout", "1000", "as.single.totalTimeout", "6000",
            "as.batch.socketTimeout", "2000", "as.batch.totalTimeout", "7000"}, 1000, 6000, 2000, 7000}
    };
  }

  @Test(dataProvider = "timeouts")
  public void timeoutPrecedence(String[] options, int singleSocket, int singleTotal,
                                int batchSocket, int batchTotal) throws Exception {
    init(options);
    for (String name : new String[]{"readPolicy", "insertPolicy", "updatePolicy", "deletePolicy"}) {
      Policy policy = (Policy) field(name).get(binding);
      assertEquals(policy.socketTimeout, singleSocket);
      assertEquals(policy.totalTimeout, singleTotal);
    }
    for (String name : new String[]{"batchReadPolicy", "batchWritePolicy"}) {
      Policy policy = (Policy) field(name).get(binding);
      assertEquals(policy.socketTimeout, batchSocket);
      assertEquals(policy.totalTimeout, batchTotal);
    }
  }

  @DataProvider(name = "invalidBatchSizes")
  public Object[][] invalidBatchSizes() {
    return new Object[][]{{"0"}, {"-1"}, {"invalid"}, {"2147483648"}};
  }

  @Test(dataProvider = "invalidBatchSizes")
  public void invalidBatchSizeDoesNotAcquireClient(String size) throws Exception {
    try {
      init("batchsize", size);
      fail("Invalid batchsize should fail initialization");
    } catch (DBException expected) {
      assertTrue(expected.getMessage().contains("batchsize"));
    }
    assertEquals(field("clientUsers").getInt(null), 0);
    assertNull(field("client").get(binding));
    assertNull(backend.methodName);
  }

  @Test
  public void sharedClientClosesAfterLastUserAndCleanupIsIdempotent() throws Exception {
    init();
    AerospikeClient second = new AerospikeClient();
    second.setProperties(new Properties());
    second.init();
    try {
      binding.cleanup();
      binding.cleanup();
      assertFalse(backend.closed);
      assertEquals(field("clientUsers").getInt(null), 1);
    } finally {
      second.cleanup();
    }
    assertTrue(backend.closed);
    assertEquals(field("clientUsers").getInt(null), 0);
    assertNull(field("sharedClient").get(null));
  }

  private static class CapturingClient implements InvocationHandler {
    private String methodName;
    private Object[] arguments;
    private Record readResult;
    private Record[] batchReadResult;
    private boolean writeSuccess = true;
    private boolean closed;

    @Override
    public Object invoke(Object proxy, Method method, Object[] args) {
      methodName = method.getName();
      arguments = args;
      switch (methodName) {
      case "get":
        return args[1] instanceof Key[] ? batchReadResult : readResult;
      case "put":
        return null;
      case "operate":
      case "delete":
        return writeSuccess;
      case "close":
        closed = true;
        return null;
      default:
        throw new AssertionError("Unexpected client call: " + methodName);
      }
    }
  }
}
