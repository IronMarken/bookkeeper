package org.apache.bookkeeper.bookie;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.apache.bookkeeper.utils.Utility;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.bookkeeper.utils.Utility.EntryListStatus.*;
import static org.apache.bookkeeper.utils.Utility.MasterKeyStatus.NULL_KEY;
import static org.apache.bookkeeper.utils.Utility.MasterKeyStatus.VALID_KEY;
import static org.apache.bookkeeper.utils.Utility.LedgerStatus.EXISTING_LEDGER;
import static org.apache.bookkeeper.utils.Utility.LedgerStatus.NOT_EXISTING_LEDGER;

@RunWith(value = Enclosed.class)
public class SortedLedgerStorageTest {

    final static Logger LOGGER = Logger.getLogger(SortedLedgerStorageTest.class.getName());

    @RunWith(value = Parameterized.class)
    public static class LedgerGenerationTest {
        private final String VALID_MASTER_KEY = "valid_key";
        private final String TEMP_DIR_NAME = "testTempDir";
        private final long EXISTING_LEDGER = 0;
        private final long NOT_EXISTING_LEDGER = 1;
        private SortedLedgerStorage storageUnderTest;
        private byte[] masterKey;
        private File tempDir;
        private long checkedLedgerID;
        private boolean expectedLedgerExists;
        private boolean expectedException;


        @Parameterized.Parameters
        public static Collection<Object[]> testParameters() {
            return Arrays.asList(new Object[][]{
                    // masterKeyStatus          // checkExistingLedger          // expectedException
                    {VALID_KEY,                 true,                           false},
                    {VALID_KEY,                 false,                          true },
                    {NULL_KEY,                  true,                           true },
                    {NULL_KEY,                  false,                          true }
            });}

        public LedgerGenerationTest(Utility.MasterKeyStatus masterKeyStatus, boolean checkExistingLedger, boolean expectedException){
            //set masterKey
            this.setMasterKey(masterKeyStatus);

            // set ledger to check
            if(checkExistingLedger)
                this.checkedLedgerID = EXISTING_LEDGER;
            else
                this.checkedLedgerID = NOT_EXISTING_LEDGER;
            this.expectedLedgerExists = checkExistingLedger;

            // set expected Exception value
            this.expectedException = expectedException;
        }

        private void setMasterKey(Utility.MasterKeyStatus masterKeyStatus) {
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

        @Before
        public void setupStorage() {
            // create the storage
            this.storageUnderTest = new SortedLedgerStorage();
           try {
               this.tempDir = IOUtils.createTempDir(TEMP_DIR_NAME, ".tmp");
               ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
               serverConfiguration.setLedgerDirNames(new String[] { tempDir.toString() });

               LedgerDirsManager ledgerDirsManager = new LedgerDirsManager(serverConfiguration, serverConfiguration.getLedgerDirs(),
                       new DiskChecker(serverConfiguration.getDiskUsageThreshold(), serverConfiguration.getDiskUsageWarnThreshold()));
               this.storageUnderTest.initialize(serverConfiguration, null, ledgerDirsManager, ledgerDirsManager,
                       NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);
           } catch(Exception e){
                Assert.fail();
           }
        }

        @After
        public void shutdownSUT() {
            try {
                this.storageUnderTest.shutdown();
                FileUtils.deleteDirectory(this.tempDir);
            } catch(Exception e){
               Assert.fail();
            }
        }

        @Test
        public void ledgerTest() {

            boolean actualException = false;

            try {
                // generate Ledger
                this.storageUnderTest.setMasterKey(EXISTING_LEDGER, masterKey);

                // check if Ledger to check exists
                boolean actualLedgerExists = this.storageUnderTest.ledgerExists(this.checkedLedgerID);
                // Assert expected, actual
                Assert.assertEquals(expectedLedgerExists, actualLedgerExists);

                // check masterKey
                byte[] actualMasterKey = this.storageUnderTest.readMasterKey(this.checkedLedgerID);
                int expectedLength = this.masterKey.length;
                int actualLength = actualMasterKey.length;

                // check lengths
                Assert.assertEquals(expectedLength, actualLength);

                // check all bytes
                byte actualByte;
                byte expectedByte;

                for(int i=0; i < actualLength; i++) {
                    actualByte = actualMasterKey[i];
                    expectedByte = this.masterKey[i];

                    // Assert expected, actual
                    Assert.assertEquals(expectedByte, actualByte);
                }
            } catch (Exception e){
                actualException = true;
            }
            Assert.assertEquals(expectedException, actualException);
        }

    }

    @RunWith(value = Parameterized.class)
    public static class EntryGenerationTest {

        private final String TEMP_DIR_NAME = "testTempDir";
        private final String PAYLOAD = "valid_payload";
        private final String VALID_MASTER_KEY = "valid_key";
        private final int MAX_REPETITIONS = 1000;
        private final long EXISTING_LEDGER_ID = 0;
        private final long NOT_EXISTING_LEDGER_ID = 1;
        private SortedLedgerStorage storageUnderTest;
        private File tempDir;
        private int entryListSize;
        private List<ByteBuf> entryList;
        private CheckpointSource dummyCheckpointSource;
        private Checkpointer dummyCheckpointer;
        private byte[] masterKey;

        private long ledgerToRead;

        private boolean writeLedgerExists;
        private boolean isFenced;

        private boolean expectedException;
        private int expectedSize;

        @Parameterized.Parameters
        public static Collection<Object[]> testParameters() {
            return Arrays.asList(new Object[][]{
                    // entryListStatus          // masterKeyStatus         // read ledger status     // writeLedger status      // isFenced         // expectedException
                    {ALL_VALID,                 VALID_KEY,                 EXISTING_LEDGER,             NOT_EXISTING_LEDGER,    false,              true },
                    {ALL_VALID,                 VALID_KEY,                 EXISTING_LEDGER,             EXISTING_LEDGER,        false,              false},
                    {ALL_VALID,                 VALID_KEY,                 EXISTING_LEDGER,             EXISTING_LEDGER,        true,               false},
                    {ONE_NOT_VALID,             VALID_KEY,                 EXISTING_LEDGER,             EXISTING_LEDGER,        false,              true },
                    {ONE_NULL,                  VALID_KEY,                 EXISTING_LEDGER,             EXISTING_LEDGER,        false,              true },
                    {ALL_VALID,                 NULL_KEY,                  EXISTING_LEDGER,             EXISTING_LEDGER,        false,              true },
                    {ONE_NOT_VALID,             NULL_KEY,                  EXISTING_LEDGER,             EXISTING_LEDGER,        false,              true },
                    {ONE_NULL,                  NULL_KEY,                  EXISTING_LEDGER,             EXISTING_LEDGER,        false,              true },
                    {ALL_VALID,                 VALID_KEY,                 NOT_EXISTING_LEDGER,         EXISTING_LEDGER,        false,              true },
                    {ONE_NOT_VALID,             VALID_KEY,                 NOT_EXISTING_LEDGER,         EXISTING_LEDGER,        false,              true },
                    {ONE_NULL,                  VALID_KEY,                 NOT_EXISTING_LEDGER,         EXISTING_LEDGER,        false,              true },
                    {ALL_VALID,                 NULL_KEY,                  NOT_EXISTING_LEDGER,         EXISTING_LEDGER,        false,              true },
                    {ONE_NOT_VALID,             NULL_KEY,                  NOT_EXISTING_LEDGER,         EXISTING_LEDGER,        false,              true },
                    {ONE_NULL,                  NULL_KEY,                  NOT_EXISTING_LEDGER,         EXISTING_LEDGER,        false,              true }
            });}

        public EntryGenerationTest(Utility.EntryListStatus entryListStatus, Utility.MasterKeyStatus masterKeyStatus, Utility.LedgerStatus readLedgerStatus,  Utility.LedgerStatus writeLedgerStatus, boolean isFenced, boolean expectedException ){
            Random random = new Random();
            // MIN 1 repetition
            this.entryListSize = random.nextInt(MAX_REPETITIONS-1)+1;
            this.generateListEntry(entryListStatus);

            // set masterKey
            this.setMasterKey(masterKeyStatus);

            // set expected exception
            this.expectedException = expectedException;

            // dummy checkpoint source
            this.dummyCheckpointSource = new CheckpointSource() {
                @Override
                public Checkpoint newCheckpoint() {
                    // MAX value
                    return Checkpoint.MAX;
                }

                @Override
                public void checkpointComplete(Checkpoint checkpoint, boolean compact) {
                    // dummy with no op
                }
            };


            if( readLedgerStatus == EXISTING_LEDGER) {
                this.ledgerToRead = EXISTING_LEDGER_ID;
                this.expectedSize = this.entryListSize;
            }else {
                this.ledgerToRead = NOT_EXISTING_LEDGER_ID;
                this.expectedSize = 0;
            }

            this.dummyCheckpointer = new Checkpointer() {
                @Override
                public void startCheckpoint(CheckpointSource.Checkpoint checkpoint) {
                    // dummy with no op
                }

                @Override
                public void start() {
                    // dummy with no op
                }
            };


            this.writeLedgerExists = writeLedgerStatus == EXISTING_LEDGER;

            this.isFenced = isFenced;

        }
        private void setMasterKey(Utility.MasterKeyStatus masterKeyStatus) {
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

        private void generateListEntry(Utility.EntryListStatus status) {
            this.entryList = new ArrayList<>();
            ByteBuf entry;
            long i;
            switch(status){
                default:
                    LOGGER.log(Level.WARNING, "Entry status not valid setting to all valid");
                case ALL_VALID:
                    for(i=0; i<this.entryListSize; i++){
                        entry = this.generateEntry(Utility.EntryStatus.VALID_ENTRY, i);
                        this.entryList.add(entry);
                    }
                    break;
                case ONE_NOT_VALID:
                    i=0;
                    entry = this.generateEntry(Utility.EntryStatus.NOT_VALID_ENTRY, i);
                    this.entryList.add(entry);
                    for(i=1; i<this.entryListSize; i++){
                        entry = this.generateEntry(Utility.EntryStatus.VALID_ENTRY, i);
                        this.entryList.add(entry);
                    }
                    break;
                case ONE_NULL:
                    i=0;
                    entry = this.generateEntry(Utility.EntryStatus.NULL_ENTRY, i);
                    this.entryList.add(entry);
                    for(i=1; i<this.entryListSize; i++){
                        entry = this.generateEntry(Utility.EntryStatus.VALID_ENTRY, i);
                        this.entryList.add(entry);
                    }
                    break;
            }
        }

        private ByteBuf generateEntry(Utility.EntryStatus entryStatus, long entryID) {
            ByteBuf returnValue;
            switch (entryStatus) {
                case VALID_ENTRY:
                    returnValue = Unpooled.buffer(3 * Long.BYTES + PAYLOAD.length());
                    returnValue.writeLong(EXISTING_LEDGER_ID);
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


        @Before
        public void setupStorage() {
            // create the storage
            this.storageUnderTest = new SortedLedgerStorage();
            try {
                this.tempDir = IOUtils.createTempDir(TEMP_DIR_NAME, ".tmp");
                ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
                String[] dirs = new String[]{this.tempDir.getAbsolutePath()};
                serverConfiguration.setLedgerDirNames(dirs);

                LedgerManager mockedLedgerManager = Mockito.mock(LedgerManager.class);

                DiskChecker diskChecker = BookieResources.createDiskChecker(serverConfiguration);
                LedgerDirsManager ledgerDirsManager = BookieResources.createLedgerDirsManager(serverConfiguration, diskChecker, NullStatsLogger.INSTANCE);

                this.storageUnderTest.initialize(serverConfiguration, mockedLedgerManager, ledgerDirsManager, ledgerDirsManager,
                        NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

                this.storageUnderTest.setCheckpointSource(this.dummyCheckpointSource);
                this.storageUnderTest.setCheckpointer(this.dummyCheckpointer);
            } catch(Exception e){
                Assert.fail();
            }
        }


        @After
        public void shutdownSUT() {
            try {
                this.storageUnderTest.shutdown();
                FileUtils.deleteDirectory(this.tempDir);
            } catch(Exception e){
                Assert.fail();
            }
        }

        @Test
        public void entryTest() {
            boolean actualException = false;
            try {
                this.storageUnderTest.setMasterKey(EXISTING_LEDGER_ID, this.masterKey);
                if(this.isFenced)
                    this.storageUnderTest.setFenced(EXISTING_LEDGER_ID);

                // Assert if is fenced
                Assert.assertEquals(this.isFenced, this.storageUnderTest.isFenced(EXISTING_LEDGER_ID));

                if(!this.writeLedgerExists)
                    this.storageUnderTest.deleteLedger(EXISTING_LEDGER_ID);

                // add all
                for (ByteBuf entry : this.entryList) {
                    this.storageUnderTest.addEntry(entry);
                }

                PrimitiveIterator.OfLong entryIdIterator = this.storageUnderTest.getListOfEntriesOfLedger(this.ledgerToRead);

                int actualSize = 0;
                ByteBuf actualEntry;
                ByteBuf expectedEntry;

                long actualLedgerId;
                long expectedLedgerId;

                long actualEntryId;
                long expectedEntryId;

                byte[] expectedPayload = PAYLOAD.getBytes(StandardCharsets.UTF_8);

                byte actualPayloadByte;

                while(entryIdIterator.hasNext()) {
                    long entryId = entryIdIterator.nextLong();

                    // check order in SortedLedgerStorage
                    Assert.assertEquals( actualSize, entryId);
                    actualSize ++;
                }

                // assert size is equal
                Assert.assertEquals(this.expectedSize, actualSize);

                // check entry one by one
                for(int i=0; i<this.entryListSize; i++){
                    actualEntry = this.storageUnderTest.getEntry(this.ledgerToRead, i);
                    expectedEntry = this.entryList.get(i);

                    // Ledger id
                    actualLedgerId = actualEntry.readLong();
                    expectedLedgerId = expectedEntry.readLong();

                    // assert ledgerID
                    Assert.assertEquals(expectedLedgerId, actualLedgerId);

                    // Entry id
                    actualEntryId = actualEntry.readLong();
                    expectedEntryId = expectedEntry.readLong();

                    // assert entryID
                    Assert.assertEquals(expectedEntryId, actualEntryId);

                    // Payload
                    for (byte expectedPayloadByte : expectedPayload){
                        actualPayloadByte = actualEntry.readByte();

                        // assert on byte
                        Assert.assertEquals(expectedPayloadByte, actualPayloadByte);
                    }
                }


            } catch(Exception e){
                actualException = true;
            }

            // expectedException, actualException
            Assert.assertEquals( this.expectedException, actualException);
        }

    }
}
