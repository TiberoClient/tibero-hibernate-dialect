import org.hibernate.QueryTimeoutException;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.ConstraintViolationException;
import org.hibernate.exception.LockAcquisitionException;
import org.hibernate.exception.LockTimeoutException;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * SQLExceptionConversionTest
 *
 * 목적:
 *  - buildSQLExceptionConversionDelegate() 가 Tibero errorCode 기반으로 Hibernate 표준 예외로 변환하는지 검증
 */
public class SQLExceptionConversionTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] {};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
    }

    // ------------------------------------------------------------------------
    // 1) NOWAIT lock acquisition failed (-12033) -> LockTimeoutException
    // ------------------------------------------------------------------------
    @Test
    public void testDelegate_convertsNowaitToLockTimeoutException() {
        final String table = uniqueObjectName("LOCK_NOWAIT");

        try {
            // 준비
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (id number primary key)"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "insert into " + table + " values (1)"
                ).executeUpdate();
            });

            // session1: lock 잡기
            inSession(session1 -> {
                session1.beginTransaction();
                session1.createNativeQuery(
                        "select id from " + table + " where id=1 for update",
                        Long.class
                ).getSingleResult();

                // session2: nowait로 lock 시도 -> 즉시 실패해야 하며 Hibernate가 LockTimeoutException으로 변환
                inSession(session2 -> {
                    session2.beginTransaction();
                    try {
                        session2.createNativeQuery(
                                "select id from " + table + " where id=1 for update nowait",
                                Long.class
                        ).getSingleResult();
                        fail("Expected NOWAIT lock acquisition to fail, but it succeeded.");
                    } catch (Exception e) {
                        LockTimeoutException lte = findCause(e, LockTimeoutException.class);
                        assertNotNull("Expected LockTimeoutException for NOWAIT error", lte);

                        System.out.println("[Expected] NOWAIT converted to LockTimeoutException: " + lte.getMessage());
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

    // ------------------------------------------------------------------------
    // 2) WAIT timeout (-12034) -> LockTimeoutException
    // ------------------------------------------------------------------------
    @Test
    public void testDelegate_convertsWaitTimeoutToLockTimeoutException() {
        final String table = uniqueObjectName("LOCK_WAIT");

        try {
            // 준비
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (id number primary key)"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "insert into " + table + " values (1)"
                ).executeUpdate();
            });

            // session1: lock 잡기
            inSession(session1 -> {
                session1.beginTransaction();
                session1.createNativeQuery(
                        "select id from " + table + " where id=1 for update",
                        Long.class
                ).getSingleResult();

                // session2: wait 1초로 lock 요청 -> timeout 발생해야 함
                inSession(session2 -> {
                    session2.beginTransaction();
                    try {
                        session2.createNativeQuery(
                                "select id from " + table + " where id=1 for update wait 1",
                                Long.class
                        ).getSingleResult();
                        fail("Expected WAIT lock acquisition to timeout, but it succeeded.");
                    } catch (Exception e) {
                        LockTimeoutException lte = findCause(e, LockTimeoutException.class);
                        assertNotNull("Expected LockTimeoutException for WAIT timeout", lte);

                        System.out.println("[Expected] WAIT timeout converted to LockTimeoutException: " + lte.getMessage());
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

    // ------------------------------------------------------------------------
    // 3) Deadlock (-12032) -> LockAcquisitionException
    //    - 두 트랜잭션이 서로 상대 row를 lock하려고 하면 deadlock 발생
    // ------------------------------------------------------------------------
    @Test
    public void testDelegate_convertsDeadlockToLockAcquisitionException() {
        final String table = uniqueObjectName("DEADLOCK");

        try {
            // 준비: 2 row
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (id number primary key, val number)"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "insert into " + table + " values (1, 10)"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "insert into " + table + " values (2, 20)"
                ).executeUpdate();
            });

            // 두 세션을 동시에 진행해야 하므로 Thread 사용
            final Exception[] threadEx = new Exception[2];

            Thread t1 = new Thread(() -> {
                inSession(s1 -> {
                    s1.beginTransaction();
                    try {
                        // row1 lock
                        s1.createNativeMutationQuery("update " + table + " set val=val+1 where id=1")
                                .executeUpdate();

                        sleep(300);

                        // row2 접근 -> deadlock 유발
                        s1.createNativeMutationQuery("update " + table + " set val=val+1 where id=2")
                                .executeUpdate();

                        s1.getTransaction().commit();
                    } catch (Exception e) {
                        threadEx[0] = e;
                        safeRollback(s1);
                    }
                });
            });

            Thread t2 = new Thread(() -> {
                inSession(s2 -> {
                    s2.beginTransaction();
                    try {
                        // row2 lock
                        s2.createNativeMutationQuery("update " + table + " set val=val+1 where id=2")
                                .executeUpdate();

                        sleep(300);

                        // row1 접근 -> deadlock 유발
                        s2.createNativeMutationQuery("update " + table + " set val=val+1 where id=1")
                                .executeUpdate();

                        s2.getTransaction().commit();
                    } catch (Exception e) {
                        threadEx[1] = e;
                        safeRollback(s2);
                    }
                });
            });

            t1.start();
            t2.start();
            t1.join();
            t2.join();

            // deadlock은 어느 한 쪽에서 발생하면 됨
            Exception deadlockEx = threadEx[0] != null ? threadEx[0] : threadEx[1];
            assertNotNull("Expected at least one thread to hit deadlock", deadlockEx);

            LockAcquisitionException lae = findCause(deadlockEx, LockAcquisitionException.class);
            assertNotNull("Expected deadlock to be converted to LockAcquisitionException", lae);

            System.out.println("[Expected] Deadlock converted to LockAcquisitionException: " + lae.getMessage());

        } catch (InterruptedException ie) {
            throw new RuntimeException(ie);
        } finally {
            dropTableWithRetry(table);
        }
    }

    // ------------------------------------------------------------------------
    // 4) Statement cancelled (-12040) -> QueryTimeoutException
    //    - 가장 쉬운 방법: Hibernate query timeout 설정 후 sleep 함수 실행
    // ------------------------------------------------------------------------
    @Test
    public void testDelegate_convertsCancelledStatementToQueryTimeoutException() {
        final String plsql =
                "BEGIN " +
                "  DBMS_LOCK.SLEEP(5); " +
                "END;";

        try {
            inTransaction(session -> {
                        // query timeout 1초 설정
                        session.createNativeMutationQuery(plsql)
                                .setTimeout(1)  // seconds
                                .executeUpdate();
            });

            fail("Expected QueryTimeoutException but PL/SQL sleep succeeded.");
        } catch (Exception e) {
            QueryTimeoutException qte = findCause(e, QueryTimeoutException.class);
            assertNotNull("Expected QueryTimeoutException for statement cancel/timeout", qte);

            System.out.println("[Expected] Statement canceled/timeout converted to QueryTimeoutException: " + qte.getMessage());
        }
    }

    @Test
    public void testSQLExceptionConversionDelegate_uniqueViolationToConstraintViolationException() {
        final String table = uniqueObjectName("UQ_TEST2");
        final String uqName = uniqueObjectName("UQ_UK2");

        try {
            // create table + unique constraint
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (" +
                                "id number primary key, " +
                                "uk varchar2(100), " +
                                "constraint " + uqName + " unique (uk)" +
                                ")"
                ).executeUpdate();
            });

            // insert first row
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "insert into " + table + " (id, uk) values (1, 'A')"
                ).executeUpdate();
            });

            // second insert -> UNIQUE violation 발생 -> Hibernate 변환 결과 확인
            try {
                inTransaction(session -> {
                    session.createNativeMutationQuery(
                            "insert into " + table + " (id, uk) values (2, 'A')"
                    ).executeUpdate();
                });
                fail("Expected UNIQUE constraint violation, but insert succeeded.");
            } catch (Exception e) {
                // Hibernate가 변환해서 던지는 예외가 ConstraintViolationException인지 확인
                ConstraintViolationException cve = findCause(e, ConstraintViolationException.class);
                assertNotNull("Expected Hibernate ConstraintViolationException", cve);

                assertEquals("ConstraintKind should be UNIQUE",
                        ConstraintViolationException.ConstraintKind.UNIQUE,
                        cve.getKind());

                assertNotNull("Constraint name should be extracted", cve.getConstraintName());
                assertEquals(uqName.toUpperCase(), normalizeConstraintName(cve.getConstraintName()).toUpperCase());
            }

        } finally {
            dropTableWithRetry(table);
        }
    }

    // ------------------------------------------------------------------------
    // Helper
    // ------------------------------------------------------------------------

    /**
     * 예외 체인에서 특정 타입의 cause를 찾음
     */
    private <T extends Throwable> T findCause(Throwable t, Class<T> type) {
        while (t != null) {
            if (type.isInstance(t)) {
                return type.cast(t);
            }
            t = t.getCause();
        }
        return null;
    }

    /**
     * 'SCHEMA.CONSTRAINT_NAME' -> CONSTRAINT_NAME conversion helper
     */
    private String normalizeConstraintName(String extracted) {
        if (extracted == null) return null;

        // remove quotes
        String s = extracted.replace("'", "").replace("\"", "");

        // schema-qualified 형태 처리
        // 1) TIBERO.UQ_NAME
        // 2) TIBERO.UQ_NAME (with quotes removed)
        // 3) TIBERO'.'UQ_NAME 형태도 '.' 기준으로 분리 가능
        int dot = s.lastIndexOf('.');
        if (dot >= 0) {
            s = s.substring(dot + 1);
        }
        return s;
    }
}
