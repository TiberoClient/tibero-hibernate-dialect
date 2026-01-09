import org.hibernate.LockOptions;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TiberoDialectLockTest
 *
 * 1) Dialect lock SQL fragment contract test
 * 2) 실제 Tibero DB에서 NOWAIT / SKIP LOCKED 기능이 동작하는지 integration test
 */
public class LockTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] {};
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
    }

    // =========================================================================
    // 1) Contract Tests (SQL Fragment)
    // =========================================================================

    @Test
    public void testForUpdateStrings() {
        String aliases = "alias";

        assertEquals(" for update nowait", dialect.getForUpdateNowaitString());
        assertEquals(" for update of " + aliases + " nowait", dialect.getForUpdateNowaitString(aliases));

        assertEquals(" for update of " + aliases, dialect.getForUpdateString(aliases));

        assertEquals(" for update skip locked", dialect.getForUpdateSkipLockedString());
        assertEquals(" for update of " + aliases + " skip locked", dialect.getForUpdateSkipLockedString(aliases));
    }

    @Test
    public void testWriteLockString() {
        assertEquals(" for update nowait", dialect.getWriteLockString(LockOptions.NO_WAIT));
        assertEquals(" for update skip locked", dialect.getWriteLockString(LockOptions.SKIP_LOCKED));
        assertEquals(" for update", dialect.getWriteLockString(LockOptions.WAIT_FOREVER));

        String aliases = "alias";
        assertEquals(" for update of alias nowait", dialect.getWriteLockString(aliases, LockOptions.NO_WAIT));
        assertEquals(" for update of alias skip locked", dialect.getWriteLockString(aliases, LockOptions.SKIP_LOCKED));
        assertEquals(" for update of alias", dialect.getWriteLockString(aliases, LockOptions.WAIT_FOREVER));

        // default timeout
        String lock = dialect.getWriteLockString(10);
        assertTrue(lock.startsWith(" for update"));
        assertTrue(lock.contains(" wait "));
    }

    @Test
    public void testReadLockStringDelegatesToWriteLockString() {
        String aliases = "alias";

        assertEquals(dialect.getWriteLockString(LockOptions.NO_WAIT), dialect.getReadLockString(LockOptions.NO_WAIT));
        assertEquals(dialect.getWriteLockString(aliases, LockOptions.NO_WAIT), dialect.getReadLockString(aliases, LockOptions.NO_WAIT));

        assertEquals(dialect.getWriteLockString(LockOptions.SKIP_LOCKED), dialect.getReadLockString(LockOptions.SKIP_LOCKED));
        assertEquals(dialect.getWriteLockString(aliases, LockOptions.SKIP_LOCKED), dialect.getReadLockString(aliases, LockOptions.SKIP_LOCKED));

        assertEquals(dialect.getWriteLockString(LockOptions.WAIT_FOREVER), dialect.getReadLockString(LockOptions.WAIT_FOREVER));
        assertEquals(dialect.getWriteLockString(aliases, LockOptions.WAIT_FOREVER), dialect.getReadLockString(aliases, LockOptions.WAIT_FOREVER));
    }

    // =========================================================================
    // 2) Capability/Integration Tests (Real Tibero DB)
    // =========================================================================

    /**
     * supportsNoWait()가 true이고,
     * 실제 Tibero DB에서도 FOR UPDATE NOWAIT이
     * 락 걸린 상황에서 즉시 실패하는지 확인
     */
    @Test
    public void testSupportsNoWaitAndNowaitBehavior() {
        assertTrue(dialect.supportsNoWait());

        final String table = uniqueObjectName("LOCK_NOWAIT_TEST");

        try {
            // 준비: 테이블 생성 및 1 row insert
            inTransaction(session -> {
                session.createNativeMutationQuery("create table " + table + " (id number primary key)").executeUpdate();
                session.createNativeMutationQuery("insert into " + table + " values (1)").executeUpdate();
            });

            // Session1: row lock 잡기
            inSession(session1 -> {
                session1.beginTransaction();

                session1.createNativeQuery(
                        "select id from " + table + " where id=1 for update",
                        Long.class
                ).getSingleResult();

                // Session2: NOWAIT 로 lock 요청 -> 즉시 실패해야 함
                inSession(session2 -> {
                    session2.beginTransaction();
                    try {
                        session2.createNativeQuery(
                                "select id from " + table + " where id=1 for update nowait",
                                Long.class
                        ).getSingleResult();

                        fail("Expected NOWAIT lock acquisition to fail, but it succeeded.");
                    } catch (Exception e) {
                        // expected
                        System.out.println("[Expected failure] NOWAIT lock acquisition failed: " + e.getMessage());
                    } finally {
                        safeRollback(session2);
                    }
                });

                safeRollback(session1);
            });
        } finally {
            dropTableWithRetry(table);
        }
    }

    /**
     * supportsSkipLocked()가 true이고,
     * 실제 Tibero DB에서도 FOR UPDATE SKIP LOCKED가
     * 락 잡힌 row를 skip하고 다른 row를 반환하는지 확인
     */
    @Test
    public void testSupportsSkipLockedAndSkipLockedBehavior() {
        assertTrue(dialect.supportsSkipLocked());

        final String table = uniqueObjectName("LOCK_SKIP_TEST");

        try {
            // 준비: 테이블 생성 및 2 rows insert
            inTransaction(session -> {
                session.createNativeMutationQuery("create table " + table + " (id number primary key)").executeUpdate();
                session.createNativeMutationQuery("insert into " + table + " values (1)").executeUpdate();
                session.createNativeMutationQuery("insert into " + table + " values (2)").executeUpdate();
            });

            // Session1: id=1 lock
            inSession(session1 -> {
                session1.beginTransaction();

                session1.createNativeQuery(
                        "select id from " + table + " where id=1 for update",
                        Long.class
                ).getSingleResult();

                // Session2: SKIP LOCKED -> id=1 skip되고 id=2가 선택되어야 함
                inSession(session2 -> {
                    session2.beginTransaction();
                    try {
                        Long selected = session2.createNativeQuery(
                                        "select id from " + table + " for update skip locked",
                                        Long.class
                                )
                                .getSingleResult();

                        assertEquals("Locked row should be skipped, so id=2 should be selected",
                                Long.valueOf(2), selected);
                    } finally {
                        safeRollback(session2);
                    }
                });

                safeRollback(session1);
            });
        } finally {
            dropTableWithRetry(table);
        }
    }
}
