package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.apache.bookkeeper.utils.TestBookieImpl;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.PrimitiveIterator.OfLong;


import static org.apache.bookkeeper.utils.Utility.*;


@RunWith(value = Enclosed.class)
public class BookieImplEntryTest {

    final static Logger LOGGER = Logger.getLogger(BookieImplEntryTest.class.getName());

    @RunWith(value = Parameterized.class)
    public static class AddEntryTest {
        private final String VALID_MASTER_KEY = "valid_key";
        private final long EXISTING_LEDGER_ID = 0;
        private final long NOT_EXISTING_LEDGER_ID = 1;
        private final String PAYLOAD = "valid_payload";
        private final String JOURNAL_DIR = "temp_journal";
        private final String LEDGER_DIR = "temp_ledger";
        private final long ENTRY_ID = 0;
        private ByteBuf entry;
        protected boolean ackBeforeSync;
        private Object ctx;
        private byte[] masterKey;
        private long ledgerIdToRead;
        private boolean expectedException;
        private BookkeeperInternalCallbacks.WriteCallback mockedCallback;
        private File journalTempDir;
        private File ledgerTempDir;
        private Bookie bookieUnderTest;


        @Parameterized.Parameters
        public static Collection<Object[]> testParameters() {
            return Arrays.asList(new Object[][]{
                    //   entry                      ackBeforeSync   ctx           masterKey                       entry to read                       expectedException
                    {EntryStatus.VALID_ENTRY,       true,           null,         MasterKeyStatus.VALID_KEY,      ReadEntryStatus.EXISTING_ENTRY,     false }, // expected true is false
                    {EntryStatus.VALID_ENTRY,       true,           new Object(), MasterKeyStatus.VALID_KEY,      ReadEntryStatus.EXISTING_ENTRY,     false },
                    {EntryStatus.VALID_ENTRY,       false,          new Object(), MasterKeyStatus.VALID_KEY,      ReadEntryStatus.EXISTING_ENTRY,     false },
                    {EntryStatus.VALID_ENTRY,       false,          new Object(), MasterKeyStatus.VALID_KEY,      ReadEntryStatus.NOT_EXISTING_ENTRY, true  },
                    {EntryStatus.VALID_ENTRY,       false,          new Object(), MasterKeyStatus.NULL_KEY,       ReadEntryStatus.EXISTING_ENTRY,     true  },
                    {EntryStatus.VALID_ENTRY,       false,          new Object(), MasterKeyStatus.NULL_KEY,       ReadEntryStatus.NOT_EXISTING_ENTRY, true  },
                    {EntryStatus.NOT_VALID_ENTRY,   false,          new Object(), MasterKeyStatus.VALID_KEY,      ReadEntryStatus.EXISTING_ENTRY,     true  },
                    {EntryStatus.NOT_VALID_ENTRY,   false,          new Object(), MasterKeyStatus.VALID_KEY,      ReadEntryStatus.NOT_EXISTING_ENTRY, true  },
                    {EntryStatus.NOT_VALID_ENTRY,   false,          new Object(), MasterKeyStatus.NULL_KEY,       ReadEntryStatus.EXISTING_ENTRY,     true  },
                    {EntryStatus.NOT_VALID_ENTRY,   false,          new Object(), MasterKeyStatus.NULL_KEY,       ReadEntryStatus.NOT_EXISTING_ENTRY, true  },
                    {EntryStatus.NULL_ENTRY,        false,          new Object(), MasterKeyStatus.VALID_KEY,      ReadEntryStatus.EXISTING_ENTRY,     true  }
            });
        }

        public AddEntryTest(EntryStatus entryStatus, boolean ackBeforeSync, Object ctx, MasterKeyStatus masterKeyStatus, ReadEntryStatus readEntryStatus, boolean expectedException) {
            // setup all test parameters
            this.ackBeforeSync = ackBeforeSync;
            this.ctx = ctx;
            this.expectedException = expectedException;
            this.setEntry(entryStatus);
            this.setMasterKey(masterKeyStatus);
            this.setReadLedger(readEntryStatus);
        }


        private void setEntry(EntryStatus entryStatus) {
            switch (entryStatus) {
                case VALID_ENTRY:
                    this.entry = Unpooled.buffer(2 * Long.BYTES + PAYLOAD.length());
                    this.entry.writeLong(EXISTING_LEDGER_ID);
                    this.entry.writeLong(ENTRY_ID);
                    this.entry.writeBytes(PAYLOAD.getBytes(StandardCharsets.UTF_8));
                    break;
                case NOT_VALID_ENTRY:
                    // add only a payload
                    this.entry = Unpooled.buffer(PAYLOAD.length());
                    this.entry.writeBytes(PAYLOAD.getBytes(StandardCharsets.UTF_8));
                    break;
                case NULL_ENTRY:
                    this.entry = null;
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Entry status not valid setting to null");
                    this.entry = null;
            }
        }

        private void setMasterKey(MasterKeyStatus masterKeyStatus) {
            switch (masterKeyStatus) {
                case VALID_KEY:
                    this.masterKey = VALID_MASTER_KEY.getBytes(StandardCharsets.UTF_8);
                    break;
                case NULL_KEY:
                    this.masterKey = null;
                    break;
                default:
                    LOGGER.log(Level.WARNING, "MasterKey status not valid setting to null");
                    this.masterKey = null;
            }
        }

        private void setReadLedger(ReadEntryStatus readEntryStatus) {
            switch (readEntryStatus) {
                case EXISTING_ENTRY:
                    this.ledgerIdToRead = EXISTING_LEDGER_ID;
                    break;
                case NOT_EXISTING_ENTRY:
                    this.ledgerIdToRead = NOT_EXISTING_LEDGER_ID;
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Read entry status not valid setting to not valid");
                    this.ledgerIdToRead = NOT_EXISTING_LEDGER_ID;
            }
        }

        @Before
        public void setupBookie() {
            // mock the callback
            this.mockedCallback = Mockito.mock(BookkeeperInternalCallbacks.WriteCallback.class);
            // configure the behaviour
            // do nothing
            Mockito.doNothing().when(this.mockedCallback).writeComplete(Mockito.isA(Integer.class), Mockito.isA(Long.class), Mockito.isA(Long.class), Mockito.isA(BookieId.class), Mockito.isA(Object.class));

            // setup the bookie for the test
            try {
                // create dir for journal and ledger here to reset on next test
                this.journalTempDir = IOUtils.createTempDir(JOURNAL_DIR, ".tmp");
                this.ledgerTempDir = IOUtils.createTempDir(LEDGER_DIR, ".tmp");
                ServerConfiguration serverConf = TestBKConfiguration.newServerConfiguration();
                serverConf.setJournalDirName(this.journalTempDir.toString());
                String[] dirs = new String[]{this.ledgerTempDir.getAbsolutePath()};
                serverConf.setLedgerDirNames(dirs);
                this.bookieUnderTest = new TestBookieImpl(serverConf);
            } catch (Exception e) {
                Assert.fail();
            }
            // start the bookie
            this.bookieUnderTest.start();
        }

        @After
        public void shutdownBookie() {
            // shutdown the bookie
            this.bookieUnderTest.shutdown();
            try {
                // delete dirs
                FileUtils.deleteDirectory(this.journalTempDir);
                FileUtils.deleteDirectory(this.ledgerTempDir);
            } catch (Exception e) {
                Assert.fail();
            }
        }

        @Test
        public void testAddEntry() {
            //set boolean value if the exception is thrown
            boolean isExceptionThrown = false;
            try {
                // add the entry
                this.bookieUnderTest.addEntry(this.entry, this.ackBeforeSync, this.mockedCallback, this.ctx, this.masterKey);

                // read the entry added
                ByteBuf actualEntry = this.bookieUnderTest.readEntry(this.ledgerIdToRead, ENTRY_ID);

                // Assert expected ledgerID, actual ledgerID
                long actualLedgerID = actualEntry.readLong();
                Assert.assertEquals(ledgerIdToRead, actualLedgerID);

                // Assert expected entryID, actual entryID
                long actualEntryID = actualEntry.readLong();
                Assert.assertEquals(ENTRY_ID, actualEntryID);

                // Assert payload byte for byte
                byte[] expectedPayload = PAYLOAD.getBytes(StandardCharsets.UTF_8);
                byte expectedByte;
                byte actualByte;

                for (byte b : expectedPayload) {
                    expectedByte = b;
                    actualByte = actualEntry.readByte();
                    // expected byte, actual byte
                    Assert.assertEquals(expectedByte, actualByte);
                }

            } catch (Exception e) {
                isExceptionThrown = true;
            }

            // assert the exception
            Assert.assertEquals(this.expectedException, isExceptionThrown);
        }
    }

    @RunWith(value = Parameterized.class)
    public static class MultiAddEntryTest {
        private final String MASTER_KEY = "valid_key";
        private final long LEDGER_ID = 0;
        private final String PAYLOAD = "valid_payload";
        private final String JOURNAL_DIR = "temp_journal";
        private final String LEDGER_DIR = "temp_ledger";
        private final int MAX_REPETITIONS = 5;
        protected boolean ackBeforeSync;
        private Object ctx;
        private byte[] masterKey;
        private List<ByteBuf> entryList;
        private boolean expectedException;
        private BookkeeperInternalCallbacks.WriteCallback mockedCallback;
        private File journalTempDir;
        private File ledgerTempDir;
        private Bookie bookieUnderTest;
        private int numberOfInsert;

        @Parameterized.Parameters
        public static Collection<Object[]> testParameters() {
            return Arrays.asList(new Object[][]{
                    //ackBeforeSync     ctx             masterKey                     entry list to read                expectedException
                    {true,              null,           MasterKeyStatus.VALID_KEY,    EntryListStatus.ALL_VALID,        false},
                    {false,             new Object(),   MasterKeyStatus.VALID_KEY,    EntryListStatus.ALL_VALID,        false},
                    {false,             new Object(),   MasterKeyStatus.VALID_KEY,    EntryListStatus.ALL_VALID,        false},
                    {false,             new Object(),   MasterKeyStatus.VALID_KEY,    EntryListStatus.ONE_NOT_VALID,    true },
                    {false,             new Object(),   MasterKeyStatus.VALID_KEY,    EntryListStatus.ONE_NULL,         true },
                    {false,             new Object(),   MasterKeyStatus.NULL_KEY,     EntryListStatus.ALL_VALID,        true },
                    {false,             new Object(),   MasterKeyStatus.NULL_KEY,     EntryListStatus.ONE_NOT_VALID,    true },
                    {false,             new Object(),   MasterKeyStatus.NULL_KEY,     EntryListStatus.ONE_NULL,         true }
            });
        }

        public MultiAddEntryTest(boolean ackBeforeSync, Object ctx, MasterKeyStatus masterKeyStatus,EntryListStatus entryListStatus, boolean expectedException) {
            // setup all test parameters
            this.ackBeforeSync = ackBeforeSync;
            this.ctx = ctx;
            this.expectedException = expectedException;
            this.setMasterKey(masterKeyStatus);
            Random random = new Random();
            // MIN 1 repetition
            this.numberOfInsert = random.nextInt(MAX_REPETITIONS-1)+1;
            this.generateListEntry(entryListStatus);
        }

        private void generateListEntry(EntryListStatus status) {
            this.entryList = new ArrayList<>();
            ByteBuf entry;
            long i;
            switch(status){
                default:
                    LOGGER.log(Level.WARNING, "Entry status not valid setting to all valid");
                case ALL_VALID:
                    for(i=0; i<this.numberOfInsert; i++){
                        entry = this.generateEntry(EntryStatus.VALID_ENTRY, i);
                        this.entryList.add(entry);
                    }
                    break;
                case ONE_NOT_VALID:
                    i=0;
                    entry = this.generateEntry(EntryStatus.NOT_VALID_ENTRY, i);
                    this.entryList.add(entry);
                    for(i=1; i<this.numberOfInsert; i++){
                        entry = this.generateEntry(EntryStatus.VALID_ENTRY, i);
                        this.entryList.add(entry);
                    }
                    break;
                case ONE_NULL:
                    i=0;
                    entry = this.generateEntry(EntryStatus.NULL_ENTRY, i);
                    this.entryList.add(entry);
                    for(i=1; i<this.numberOfInsert; i++){
                        entry = this.generateEntry(EntryStatus.VALID_ENTRY, i);
                        this.entryList.add(entry);
                    }
                    break;
            }
        }

        private ByteBuf generateEntry(EntryStatus entryStatus, long entryID) {
            ByteBuf returnValue;
            switch (entryStatus) {
                case VALID_ENTRY:
                    returnValue = Unpooled.buffer(2 * Long.BYTES + PAYLOAD.length());
                    returnValue.writeLong(LEDGER_ID);
                    returnValue.writeLong(entryID);
                    returnValue.writeBytes(PAYLOAD.getBytes(StandardCharsets.UTF_8));
                    break;
                case NOT_VALID_ENTRY:
                    // add only a payload
                    returnValue = Unpooled.buffer(PAYLOAD.length());
                    returnValue.writeBytes(PAYLOAD.getBytes(StandardCharsets.UTF_8));
                    break;
                case NULL_ENTRY:
                    returnValue = null;
                    break;
                default:
                    LOGGER.log(Level.WARNING, "Entry status not valid setting to null");
                    returnValue = null;
            }
            return returnValue;
        }

        private void setMasterKey(MasterKeyStatus masterKeyStatus) {
            switch (masterKeyStatus) {
                case VALID_KEY:
                    this.masterKey = MASTER_KEY.getBytes(StandardCharsets.UTF_8);
                    break;
                case NULL_KEY:
                    this.masterKey = null;
                    break;
                default:
                    LOGGER.log(Level.WARNING, "MasterKey status not valid setting to null");
                    this.masterKey = null;
            }
        }

        @Before
        public void setupBookie() {
            // mock the callback
            this.mockedCallback = Mockito.mock(BookkeeperInternalCallbacks.WriteCallback.class);
            // configure the behaviour
            // do nothing
            Mockito.doNothing().when(this.mockedCallback).writeComplete(Mockito.isA(Integer.class), Mockito.isA(Long.class), Mockito.isA(Long.class), Mockito.isA(BookieId.class), Mockito.isA(Object.class));

            // setup the bookie for the test
            try {
                // create dir for journal and ledger here to reset on next test
                this.journalTempDir = IOUtils.createTempDir(JOURNAL_DIR, ".tmp");
                this.ledgerTempDir = IOUtils.createTempDir(LEDGER_DIR, ".tmp");
                ServerConfiguration serverConf = TestBKConfiguration.newServerConfiguration();
                serverConf.setJournalDirName(this.journalTempDir.toString());
                String[] dirs = new String[]{this.ledgerTempDir.getAbsolutePath()};
                serverConf.setLedgerDirNames(dirs);
                this.bookieUnderTest = new TestBookieImpl(serverConf);
            } catch (Exception e) {
                Assert.fail();
            }
            // start the bookie
            this.bookieUnderTest.start();
        }

        @After
        public void shutdownBookie() {
            // shutdown the bookie
            this.bookieUnderTest.shutdown();
            try {
                // delete dirs
                FileUtils.deleteDirectory(this.journalTempDir);
                FileUtils.deleteDirectory(this.ledgerTempDir);
            } catch (Exception e) {
                Assert.fail();
            }
        }

        @Test
        public void testMultiAddEntry() {
            boolean isExceptionThrown = false;
            try {
                // add the entry
                for(ByteBuf entry: this.entryList ) {
                    this.bookieUnderTest.addEntry(entry, this.ackBeforeSync, this.mockedCallback, this.ctx, this.masterKey);
                }

                // used also as counter
                int actualSize = 0;
                long actualId;
                long expectedId;

                OfLong entryIdIterator = this.bookieUnderTest.getListOfEntriesOfLedger(LEDGER_ID);

                while(entryIdIterator.hasNext()){

                    actualId = entryIdIterator.nextLong();

                    // entry IDs are a sequence
                    expectedId = actualSize;

                    // Assert ID expected ID, actual ID
                    Assert.assertEquals(expectedId, actualId);

                    actualSize++;
                }

                int expectedSize = this.numberOfInsert;
                // Assert final size expected size, actual size
                Assert.assertEquals(expectedSize, actualSize);
            } catch (Exception e) {
                isExceptionThrown = true;
            }

            // assert the exception
            Assert.assertEquals(this.expectedException, isExceptionThrown);
        }


    }



}
