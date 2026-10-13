package site.ycsb.db.atalanta;

import com.light.atalanta.Admin;
import com.light.atalanta.DbColumnDescriptor;
import com.light.atalanta.TableName;
import com.light.atalanta.api.DbTableDescriptor;
import com.light.atalanta.model.RecordCompressionType;
import org.junit.Test;
import site.ycsb.DBException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.*;

/** Offline table preparation checks: no database or ZooKeeper connection is opened. */
public class AtalantaClientTableOptionsTest {
  private static final TableName TABLE = TableName.valueOf("default", "test");

  private AtalantaClient client(String... options) throws DBException {
    Properties props = new Properties();
    props.setProperty("columnfamily", "cf");
    props.setProperty("regioncount", "4");
    props.setProperty("dotransactions", "false");
    props.setProperty("tableRecreate", "true");
    for (int i = 0; i < options.length; i += 2) {
      props.setProperty(options[i], options[i + 1]);
    }
    AtalantaClient client = new AtalantaClient();
    client.setProperties(props);
    client.readTableOptions();
    return client;
  }

  @Test
  public void recreatesEnabledTableWithAllOptions() throws Exception {
    FakeAdmin admin = new FakeAdmin(true, false);
    client("adb.tbl.compress", "none", "adb.tbl.dict", "true", "adb.reg.rec.count", "123456")
        .prepareInitialTable(admin, TABLE);
    assertEquals(Arrays.asList("disable", "delete", "createConfigured"), admin.actions);
    assertEquals(TABLE, admin.descriptor.getTableName());
    DbColumnDescriptor column = admin.descriptor.getFamilies().iterator().next();
    assertEquals("cf", column.getNameAsString());
    assertEquals(RecordCompressionType.none, column.getRecordCompressionType());
    assertEquals(Boolean.TRUE, column.getDictionary());
    // The per-region count must not be divided by the number of regions.
    assertEquals(Long.valueOf(123456), column.getRecordTargetCount());
    assertEquals(3, admin.splits.length);
  }

  @Test
  public void createsMissingTableWithLzAndDisabledDictionary() throws Exception {
    FakeAdmin admin = new FakeAdmin(false, false);
    client("adb.tbl.compress", "lz", "adb.tbl.dict", "false")
        .prepareInitialTable(admin, TABLE);
    assertEquals(Arrays.asList("createConfigured"), admin.actions);
    DbColumnDescriptor column = admin.descriptor.getFamilies().iterator().next();
    assertEquals(RecordCompressionType.lz, column.getRecordCompressionType());
    assertEquals(Boolean.FALSE, column.getDictionary());
    assertNull(column.getRecordTargetCount());
  }

  @Test
  public void acceptsEachOptionIndependently() throws Exception {
    for (String[] option : new String[][] {
        {"adb.tbl.compress", "none"}, {"adb.tbl.dict", "false"}, {"adb.reg.rec.count", "0"}}) {
      FakeAdmin admin = new FakeAdmin(true, true);
      client(option).prepareInitialTable(admin, TABLE);
      assertEquals(Arrays.asList("delete", "createConfigured"), admin.actions);
    }
  }

  @Test
  public void preservesExistingTableDuringTransactionsIncludingInserts() throws Exception {
    FakeAdmin admin = new FakeAdmin(true, false);
    client("dotransactions", "true", "adb.tbl.dict", "true").prepareInitialTable(admin, TABLE);
    assertTrue(admin.actions.isEmpty());
  }

  @Test
  public void preservesExistingTableWhenRecreationIsDisabled() throws Exception {
    FakeAdmin admin = new FakeAdmin(true, false);
    client("tableRecreate", "false", "adb.tbl.dict", "true").prepareInitialTable(admin, TABLE);
    assertTrue(admin.actions.isEmpty());
  }

  @Test
  public void doesNotRecreateWithoutNewOptions() throws Exception {
    FakeAdmin admin = new FakeAdmin(true, false);
    client().prepareInitialTable(admin, TABLE);
    assertTrue(admin.actions.isEmpty());
  }

  @Test
  public void retainsLegacyCreationWithoutNewOptions() throws Exception {
    FakeAdmin admin = new FakeAdmin(false, false);
    client().prepareInitialTable(admin, TABLE);
    assertEquals(Arrays.asList("createLegacy"), admin.actions);
  }

  @Test
  public void supportsLowercaseRecreatePropertyAndOneRegion() throws Exception {
    AtalantaClient client = client("adb.tbl.dict", "true", "regioncount", "1");
    client.getProperties().remove("tableRecreate");
    client.getProperties().setProperty("tablerecreate", "true");
    client.readTableOptions();
    FakeAdmin admin = new FakeAdmin(true, true);
    client.prepareInitialTable(admin, TABLE);
    assertEquals(Arrays.asList("delete", "createConfigured"), admin.actions);
    assertEquals(0, admin.splits.length);
  }

  @Test
  public void rejectsInvalidOptionsBeforeDatabaseAccess() throws Exception {
    for (String[] option : new String[][] {
        {"adb.tbl.compress", "snappy"}, {"adb.tbl.dict", "yes"},
        {"adb.reg.rec.count", "-1"}, {"adb.reg.rec.count", "abc"},
        {"adb.reg.rec.count", "9223372036854775808"}, {"regioncount", "0"}}) {
      try {
        client(option);
        fail("Expected invalid option: " + Arrays.toString(option));
      } catch (DBException expected) {
        assertTrue(expected.getMessage().startsWith("Invalid table options:"));
      }
    }
  }

  @Test
  public void doesNotCreateAfterFailedDeletion() throws Exception {
    FakeAdmin admin = new FakeAdmin(true, true) {
      @Override
      public void deleteTable(TableName table) throws IOException {
        throw new IOException("Deletion failed");
      }
    };
    try {
      client("adb.tbl.dict", "true").prepareInitialTable(admin, TABLE);
      fail("Expected deletion failure");
    } catch (IOException expected) {
      assertEquals("Deletion failed", expected.getMessage());
      assertTrue(admin.actions.isEmpty());
    }
  }

  private static class FakeAdmin extends Admin {
    private final boolean exists;
    private final boolean disabled;
    final List<String> actions = new ArrayList<>();
    DbTableDescriptor descriptor;
    byte[][] splits;

    FakeAdmin(boolean exists, boolean disabled) {
      this.exists = exists;
      this.disabled = disabled;
    }

    @Override
    public boolean tableExists(TableName table) {
      return exists;
    }

    @Override
    public boolean isTableDisabled(TableName table) {
      return disabled;
    }

    @Override
    public void disableTable(TableName table) {
      actions.add("disable");
    }

    @Override
    public void deleteTable(TableName table) throws IOException {
      actions.add("delete");
    }

    @Override
    public void createTable(DbTableDescriptor table, byte[][] splitKeys) {
      actions.add("createConfigured");
      descriptor = table;
      splits = splitKeys;
    }

    @Override
    public void createTable(TableName table, byte[][] splitKeys) {
      actions.add("createLegacy");
      splits = splitKeys;
    }
  }
}
