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
package site.ycsb.db.atalanta;

import com.light.atalanta.DbMutation;
import com.light.atalanta.Delete;
import com.light.atalanta.Get;
import com.light.atalanta.Put;
import com.light.atalanta.Result;
import com.light.atalanta.Table;
import com.light.atalanta.transport.messages.ResultOperation;
import com.light.atalanta.transport.values.KeyValues;
import com.light.atalanta.transport.values.QualifierValue;
import com.light.atalanta.util.Bytes;
import com.light.atalanta.util.XxHash32KeySalter;
import org.junit.Test;
import site.ycsb.ByteArrayByteIterator;
import site.ycsb.ByteIterator;
import site.ycsb.DBException;
import site.ycsb.Status;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * Offline checks for full-record reads, batch validation, and SDK error handling.
 */
public class AtalantaClientOperationsTest {
  private static final byte[] FAMILY = Bytes.toBytes("cf");

  private static void setField(AtalantaClient binding, String name, Object value) throws Exception {
    Field reflected = AtalantaClient.class.getDeclaredField(name);
    reflected.setAccessible(true);
    reflected.set(binding, value);
  }

  private static AtalantaClient client(FakeTable table, int batchSize, boolean buffered) throws Exception {
    AtalantaClient binding = new AtalantaClient();
    binding.setProperties(new Properties());
    setField(binding, "currentTable", table);
    setField(binding, "tableName", "test");
    setField(binding, "batchSize", batchSize);
    setField(binding, "columnFamilyBytes", FAMILY);
    setField(binding, "clientSideBuffering", buffered);
    if (buffered) {
      setField(binding, "bufferedMutator", table);
    }
    return binding;
  }

  private static Result record(byte firstValue) {
    byte[] row = Bytes.toBytes("row");
    KeyValues main = new KeyValues(row, FAMILY, new QualifierValue[]{
        new QualifierValue(Bytes.toBytes("field0"), new byte[]{firstValue, 0, (byte) 255}),
        new QualifierValue(Bytes.toBytes("field1"), new byte[]{42})
    });
    KeyValues other = new KeyValues(row, Bytes.toBytes("other-cf"),
        new QualifierValue(Bytes.toBytes("field0"), new byte[]{99}));
    return new Result(new ResultOperation(new KeyValues[]{main, other}));
  }

  private static Map<String, ByteIterator> values() {
    Map<String, ByteIterator> values = new HashMap<>();
    values.put("field0", new ByteArrayByteIterator(new byte[]{7, 0, (byte) 255}));
    return values;
  }

  private static void assertFullGet(Get get, String key) {
    assertArrayEquals(XxHash32KeySalter.saltKey(Bytes.toBytes(key)), get.getRow());
    assertTrue(get.getFamilyMap() == null || get.getFamilyMap().isEmpty());
  }

  @Test
  public void singleReadReturnsBinaryFieldsFromConfiguredFamily() throws Exception {
    FakeTable table = new FakeTable();
    table.singleResult = record((byte) 7);
    Map<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, client(table, 1, false).read("test", "key", null, result));
    assertFullGet(table.lastGet, "key");
    assertEquals(2, result.size());
    assertArrayEquals(new byte[]{7, 0, (byte) 255}, result.get("field0").toArray());
    assertArrayEquals(new byte[]{42}, result.get("field1").toArray());
  }

  @Test
  public void requestedFieldsAreFilteredAfterFetchingWholeRecord() throws Exception {
    for (int size : new int[]{1, 2}) {
      FakeTable table = new FakeTable();
      table.singleResult = record((byte) 7);
      table.batchResults = new Result[]{record((byte) 7), record((byte) 8)};
      Map<String, ByteIterator> result = new HashMap<>();
      assertEquals(Status.OK, client(table, size, false)
          .read("test", "key", Collections.singleton("field1"), result));
      assertEquals(Collections.singleton("field1"), result.keySet());
      assertArrayEquals(new byte[]{42}, result.get("field1").toArray());
      if (size == 1) {
        assertFullGet(table.lastGet, "key");
      } else {
        assertFullGet(table.lastGets.get(0), "key_0");
        assertFullGet(table.lastGets.get(1), "key_1");
      }
    }
  }

  @Test
  public void emptyOrUnknownProjectionStillChecksRecordExistence() throws Exception {
    FakeTable table = new FakeTable();
    table.singleResult = record((byte) 7);
    AtalantaClient binding = client(table, 1, false);
    Map<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, binding.read("test", "key", Collections.<String>emptySet(), result));
    assertTrue(result.isEmpty());
    assertEquals(Status.OK, binding.read("test", "key", Collections.singleton("absent-field"), result));
    assertTrue(result.isEmpty());
    table.singleResult = Result.EMPTY_RESULT;
    assertEquals(Status.ERROR, binding.read("test", "key", Collections.<String>emptySet(), result));
  }

  @Test
  public void batchReadReturnsFirstRecordWithoutComparingOtherValues() throws Exception {
    FakeTable table = new FakeTable();
    table.batchResults = new Result[]{record((byte) 7), record((byte) 8)};
    Map<String, ByteIterator> result = new HashMap<>();
    assertEquals(Status.OK, client(table, 2, false).read("test", "key", null, result));
    assertEquals(2, table.lastGets.size());
    assertFullGet(table.lastGets.get(0), "key_0");
    assertFullGet(table.lastGets.get(1), "key_1");
    assertArrayEquals(new byte[]{7, 0, (byte) 255}, result.get("field0").toArray());
  }

  @Test
  public void invalidBatchResultsReturnErrorAndDoNotModifyResult() throws Exception {
    FakeTable table = new FakeTable();
    AtalantaClient binding = client(table, 2, false);
    for (Result[] response : new Result[][]{
        null, new Result[0], new Result[]{record((byte) 7)},
        new Result[]{record((byte) 7), null}, new Result[]{record((byte) 7), Result.EMPTY_RESULT},
        new Result[]{record((byte) 7), record((byte) 8), record((byte) 9)}}) {
      table.batchResults = response;
      Map<String, ByteIterator> result = values();
      assertEquals(Status.ERROR, binding.read("test", "key", null, result));
      assertEquals(Collections.singleton("field0"), result.keySet());
      assertArrayEquals(new byte[]{7, 0, (byte) 255}, result.get("field0").toArray());
    }
  }

  @Test
  public void missingSingleRecordReturnsError() throws Exception {
    FakeTable table = new FakeTable();
    AtalantaClient binding = client(table, 1, false);
    for (Result response : new Result[]{null, Result.EMPTY_RESULT}) {
      table.singleResult = response;
      Map<String, ByteIterator> result = new HashMap<>();
      assertEquals(Status.ERROR, binding.read("test", "key", null, result));
      assertTrue(result.isEmpty());
    }
  }

  @Test
  public void synchronousWritesAndDeletesKeepKeyLayoutsAndBatchPayloads() throws Exception {
    for (int size : new int[]{1, 3}) {
      FakeTable table = new FakeTable();
      AtalantaClient binding = client(table, size, false);
      assertEquals(Status.OK, binding.insert("test", "key", values()));
      assertEquals(size, table.lastPuts.size());
      for (int i = 0; i < size; i++) {
        Put put = table.lastPuts.get(i);
        String key = size == 1 ? "key" : "key_" + i;
        assertArrayEquals(XxHash32KeySalter.saltKey(Bytes.toBytes(key)), put.getRow());
        assertArrayEquals(new byte[]{7, 0, (byte) 255},
            put.getFamilyCellMap().get(FAMILY).get(0).cloneValue());
      }
      assertEquals(Status.OK, binding.update("test", "key", values()));
      assertEquals(Status.OK, binding.delete("test", "key"));
      assertEquals(size, table.lastDeletes.size());
      for (int i = 0; i < size; i++) {
        String key = size == 1 ? "key" : "key_" + i;
        assertArrayEquals(XxHash32KeySalter.saltKey(Bytes.toBytes(key)), table.lastDeletes.get(i).getRow());
      }
    }
  }

  @Test
  public void checkedAndRuntimeSdkErrorsReturnErrorForAllOperations() throws Exception {
    for (int size : new int[]{1, 2}) {
      for (Exception error : new Exception[]{new IOException("SDK error"),
          new RuntimeException(new IOException("Async SDK error"))}) {
        FakeTable table = new FakeTable();
        table.error = error;
        AtalantaClient binding = client(table, size, false);
        Map<String, ByteIterator> result = new HashMap<>();
        assertEquals(Status.ERROR, binding.read("test", "key", null, result));
        assertTrue(result.isEmpty());
        assertEquals(Status.ERROR, binding.insert("test", "key", values()));
        assertEquals(Status.ERROR, binding.update("test", "key", values()));
        assertEquals(Status.ERROR, binding.delete("test", "key"));
      }
    }
  }

  @Test
  public void bufferedWritesReportAcceptanceAndSurfaceImmediateErrors() throws Exception {
    for (int size : new int[]{1, 2}) {
      FakeTable table = new FakeTable();
      AtalantaClient binding = client(table, size, true);
      assertEquals(Status.BATCHED_OK, binding.insert("test", "key", values()));
      assertEquals(size, table.lastMutations.size());
      assertFalse(table.putCalled);
      assertEquals(Status.BATCHED_OK, binding.update("test", "key", values()));
      assertTrue(Status.BATCHED_OK.isOk());
      table.error = new RuntimeException("Buffer error");
      assertEquals(Status.ERROR, binding.update("test", "key", values()));
      table.error = new IOException("Buffer IO error");
      assertEquals(Status.ERROR, binding.insert("test", "key", values()));
    }
  }

  @Test
  public void invalidBatchSizeIsRejectedBeforeOpeningConnection() throws Exception {
    for (String size : new String[]{"0", "-1", "invalid", "2147483648"}) {
      Properties props = new Properties();
      props.setProperty("columnfamily", "cf");
      props.setProperty("batchsize", size);
      AtalantaClient binding = new AtalantaClient();
      binding.setProperties(props);
      try {
        binding.init();
        fail("Expected invalid batchsize to fail initialization");
      } catch (DBException expected) {
        assertTrue(expected.getMessage().contains("batchsize"));
      }
    }
  }

  private static class FakeTable extends Table {
    private Result singleResult;
    private Result[] batchResults;
    private Get lastGet;
    private List<Get> lastGets;
    private List<Put> lastPuts;
    private List<Delete> lastDeletes;
    private List<DbMutation> lastMutations;
    private boolean putCalled;
    private Exception error;

    private void checkError() throws IOException {
      if (error instanceof IOException) {
        throw (IOException) error;
      }
      if (error instanceof RuntimeException) {
        throw (RuntimeException) error;
      }
    }

    @Override
    public Result get(Get get) throws IOException {
      checkError();
      lastGet = get;
      return singleResult;
    }

    @Override
    public Result[] get(List<Get> gets) throws IOException {
      checkError();
      lastGets = gets;
      return batchResults;
    }

    @Override
    public void put(Put put) throws IOException {
      put(Collections.singletonList(put));
    }

    @Override
    public void put(List<Put> puts) throws IOException {
      checkError();
      putCalled = true;
      lastPuts = puts;
    }

    @Override
    public void delete(Delete delete) throws IOException {
      delete(Collections.singletonList(delete));
    }

    @Override
    public void delete(List<Delete> deletes) throws IOException {
      checkError();
      lastDeletes = deletes;
    }

    @Override
    public void mutate(DbMutation mutation) throws IOException {
      mutate(Collections.singletonList(mutation));
    }

    @Override
    public void mutate(List<DbMutation> mutations) throws IOException {
      checkError();
      lastMutations = mutations;
    }
  }
}
