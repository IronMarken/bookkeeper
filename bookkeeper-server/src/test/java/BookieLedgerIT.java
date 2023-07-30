import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import org.apache.bookkeeper.bookie.*;
import org.apache.bookkeeper.conf.ServerConfiguration;
import org.apache.bookkeeper.discover.RegistrationManager;
import org.apache.bookkeeper.meta.LedgerManager;
import org.apache.bookkeeper.meta.LedgerManagerFactory;
import org.apache.bookkeeper.meta.MetadataBookieDriver;
import org.apache.bookkeeper.meta.NullMetadataBookieDriver;
import org.apache.bookkeeper.net.BookieId;
import org.apache.bookkeeper.proto.BookkeeperInternalCallbacks;
import org.apache.bookkeeper.proto.SimpleBookieServiceInfoProvider;
import org.apache.bookkeeper.stats.NullStatsLogger;
import org.apache.bookkeeper.util.DiskChecker;
import org.apache.bookkeeper.util.IOUtils;
import org.apache.bookkeeper.utils.TestBKConfiguration;
import org.apache.bookkeeper.utils.Utility;
import org.apache.commons.io.FileUtils;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.mockito.Mockito;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

import static org.apache.bookkeeper.utils.Utility.EntryListStatus.*;
import static org.apache.bookkeeper.utils.Utility.LedgerStatus.EXISTING_LEDGER;
import static org.apache.bookkeeper.utils.Utility.LedgerStatus.NOT_EXISTING_LEDGER;
import static org.apache.bookkeeper.utils.Utility.MasterKeyStatus.NULL_KEY;
import static org.apache.bookkeeper.utils.Utility.MasterKeyStatus.VALID_KEY;


@RunWith(value = Parameterized.class)
public class BookieLedgerIT {

    final static Logger LOGGER = Logger.getLogger(BookieLedgerIT.class.getName());

    private final long EXISTING_LEDGER_ID = 0;
    private final long NOT_EXISTING_LEDGER_ID = 1;
    private final int MAX_REPETITIONS = 1000;
    private final String PAYLOAD = "valid_payload";
    private final String JOURNAL_DIR = "temp_journal";
    private final String LEDGER_DIR = "temp_ledger";
    private final String VALID_MASTER_KEY = "valid_key";
    private BookieImpl bookie;
    private LedgerStorage storage;
    private BookkeeperInternalCallbacks.WriteCallback mockedCallback;
    private File journalTempDir;
    private File ledgerTempDir;
    protected boolean ackBeforeSync;
    private Object ctx;
    private byte[] masterKey;
    private long ledgerIdToRead;
    private List<ByteBuf> entryList;
    private int entryListSize;
    private boolean expectedException;

    @Parameterized.Parameters
    public static Collection<Object[]> testParameters() {
        return Arrays.asList(new Object[][]{
                // list of entry        ack before sync     ctx             masterKey           readLedgerStatus        expected exception
                {ALL_VALID,             false,              null,           NULL_KEY,           EXISTING_LEDGER,        true },
                {ALL_VALID,             false,              new Object(),   VALID_KEY,          EXISTING_LEDGER,        false},
                {ALL_VALID,             true,               new Object(),   VALID_KEY,          EXISTING_LEDGER,        false},
                {ALL_VALID,             false,              new Object(),   VALID_KEY,          NOT_EXISTING_LEDGER,    true },
                {ONE_NOT_VALID,         false,              new Object(),   VALID_KEY,          EXISTING_LEDGER,        true },
                {ONE_NULL,              false,              new Object(),   VALID_KEY,          EXISTING_LEDGER,        true }
        });
    }


    public BookieLedgerIT(Utility.EntryListStatus entryStatus, boolean ackBeforeSync, Object ctx, Utility.MasterKeyStatus masterKeyStatus, Utility.LedgerStatus readLedgerStatus, boolean expectedException) {
        this.ackBeforeSync = ackBeforeSync;
        this.ctx = ctx;
        this.expectedException = expectedException;

        // setup masterKey
        this.setMasterKey(masterKeyStatus);

        // setup entry list
        Random random = new Random();
        this.entryListSize = random.nextInt(MAX_REPETITIONS - 1) + 1;
        this.generateListEntry(entryStatus);

        if (readLedgerStatus == EXISTING_LEDGER) {
            this.ledgerIdToRead = EXISTING_LEDGER_ID;
        } else {
            this.ledgerIdToRead = NOT_EXISTING_LEDGER_ID;
        }
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
        switch (status) {
            default:
                LOGGER.log(Level.WARNING, "Entry status not valid setting to all valid");
            case ALL_VALID:
                for (i = 0; i < this.entryListSize; i++) {
                    entry = this.generateEntry(Utility.EntryStatus.VALID_ENTRY, i);
                    this.entryList.add(entry);
                }
                break;
            case ONE_NOT_VALID:
                i = 0;
                entry = this.generateEntry(Utility.EntryStatus.NOT_VALID_ENTRY, i);
                this.entryList.add(entry);
                for (i = 1; i < this.entryListSize; i++) {
                    entry = this.generateEntry(Utility.EntryStatus.VALID_ENTRY, i);
                    this.entryList.add(entry);
                }
                break;
            case ONE_NULL:
                i = 0;
                entry = this.generateEntry(Utility.EntryStatus.NULL_ENTRY, i);
                this.entryList.add(entry);
                for (i = 1; i < this.entryListSize; i++) {
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
    public void setupSUT() {
        this.mockedCallback = Mockito.mock(BookkeeperInternalCallbacks.WriteCallback.class);
        Mockito.doNothing().when(this.mockedCallback).writeComplete(Mockito.isA(Integer.class), Mockito.isA(Long.class), Mockito.isA(Long.class), Mockito.isA(BookieId.class), Mockito.isA(Object.class));
        ServerConfiguration serverConfiguration = TestBKConfiguration.newServerConfiguration();
        LedgerDirsManager ledgerDirsManager;
        LedgerDirsManager indexDirsManager;
        DiskChecker diskChecker;
        MetadataBookieDriver metadataBookieDriver;
        RegistrationManager registrationManager;

        // setup the storage and the bookie
        try {
            this.ledgerTempDir = IOUtils.createTempDir(LEDGER_DIR, ".tmp");
            String[] dirs = new String[]{this.ledgerTempDir.getAbsolutePath()};
            serverConfiguration.setLedgerDirNames(dirs);

            this.journalTempDir = IOUtils.createTempDir(JOURNAL_DIR, ".tmp");
            serverConfiguration.setLedgerDirNames(dirs);

            metadataBookieDriver = new NullMetadataBookieDriver();

            LedgerManagerFactory ledgerManagerFactory = metadataBookieDriver.getLedgerManagerFactory();
            LedgerManager ledgerManager = ledgerManagerFactory.newLedgerManager();

            diskChecker = BookieResources.createDiskChecker(serverConfiguration);

            ledgerDirsManager = BookieResources.createLedgerDirsManager(
                    serverConfiguration, diskChecker, NullStatsLogger.INSTANCE);
            indexDirsManager = BookieResources.createIndexDirsManager(
                    serverConfiguration, diskChecker, NullStatsLogger.INSTANCE, ledgerDirsManager);

            registrationManager = metadataBookieDriver.createRegistrationManager();

            // setup the storage
            this.storage = new SortedLedgerStorage();
            this.storage.initialize(serverConfiguration, ledgerManager, ledgerDirsManager, indexDirsManager, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT);

            this.bookie = new BookieImpl(serverConfiguration, registrationManager, this.storage, diskChecker, ledgerDirsManager, indexDirsManager, NullStatsLogger.INSTANCE, UnpooledByteBufAllocator.DEFAULT, new SimpleBookieServiceInfoProvider(serverConfiguration));
        } catch (Exception e) {
            Assert.fail();
        }
        this.bookie.start();
    }

    @Test
    public void ITWriteLedgerReadBookie() {
        boolean actualException = false;
        try {
            this.storage.setMasterKey(EXISTING_LEDGER_ID, this.masterKey);
            int i;
            // insert with ledger
            for (i = 0; i < this.entryListSize; i++) {
                this.storage.addEntry(this.entryList.get(i));
            }

            // read entry from bookie
            for (int actualEntryId = 0; actualEntryId < this.entryListSize; actualEntryId++) {
                ByteBuf entryRead = this.bookie.readEntry(this.ledgerIdToRead, actualEntryId);

                // check ledger id
                long ledgerIdRead = entryRead.readLong();

                Assert.assertEquals(ledgerIdToRead, ledgerIdRead);

                // check entry id
                long entryIdRead = entryRead.readLong();

                Assert.assertEquals(actualEntryId, entryIdRead);

                // check payload bytes
                byte[] expectedPayload = PAYLOAD.getBytes(StandardCharsets.UTF_8);

                for (byte expectedByte : expectedPayload) {
                    byte actualByte = entryRead.readByte();

                    Assert.assertEquals(expectedByte, actualByte);
                }
            }
        } catch (Exception e) {
            actualException = true;
        }

        // Assert expectedException, actualException
        Assert.assertEquals(this.expectedException, actualException);
    }

    @Test
    public void ITWriteBookieReadLedger() {
        boolean actualException = false;
        try {
            int i;
            // insert with bookie
            for (i = 0; i < this.entryListSize; i++) {
                this.bookie.addEntry(this.entryList.get(i), this.ackBeforeSync, this.mockedCallback, this.ctx, this.masterKey);
            }

            // read entry from ledger
            for (int actualEntryId = 0; actualEntryId < this.entryListSize; actualEntryId++) {
                ByteBuf entryRead = this.storage.getEntry(this.ledgerIdToRead, actualEntryId);

                // check ledger id
                long ledgerIdRead = entryRead.readLong();

                Assert.assertEquals(ledgerIdToRead, ledgerIdRead);

                // check entry id
                long entryIdRead = entryRead.readLong();

                Assert.assertEquals(actualEntryId, entryIdRead);

                // check payload bytes
                byte[] expectedPayload = PAYLOAD.getBytes(StandardCharsets.UTF_8);

                for (byte expectedByte : expectedPayload) {
                    byte actualByte = entryRead.readByte();

                    Assert.assertEquals(expectedByte, actualByte);
                }
            }
        } catch (Exception e) {
            actualException = true;
        }

        // Assert expectedException, actualException
        Assert.assertEquals(this.expectedException, actualException);
    }

    @After
    public void shutdownSUT() {
        try {
            // shutdown the bookie
            this.bookie.shutdown();
            // delete dirs
            FileUtils.deleteDirectory(this.journalTempDir);
            FileUtils.deleteDirectory(this.ledgerTempDir);

        } catch (Exception e) {
            Assert.fail();
        }
    }
}


