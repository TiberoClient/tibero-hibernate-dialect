import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.exception.spi.ViolatedConstraintNameExtractor;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;

/**
 * ConstraintNameExtractorTest
 *
 * 목적:
 *  - getViolatedConstraintNameExtractor() 가 Tibero SQLException 메시지에서 constraint 이름을 추출하는지 검증
 */
public class ConstraintNameExtractorTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;
    private ViolatedConstraintNameExtractor extractor;

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

        extractor = dialect.getViolatedConstraintNameExtractor();
        assertNotNull(extractor);
    }

    // ------------------------------------------------------------------------
    // 1) CHECK constraint violation (10006)
    // ------------------------------------------------------------------------
    @Test
    public void testExtractor_checkConstraintViolation() {
        final String table = uniqueObjectName("CHK_T");
        final String chkName = uniqueObjectName("CHK_POS");

        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (" +
                                "id number primary key, " +
                                "score number, " +
                                "constraint " + chkName + " check (score > 0)" +
                                ")"
                ).executeUpdate();
            });

            try {
                inTransaction(session -> {
                    // score = -1 => CHECK 위반
                    session.createNativeMutationQuery(
                            "insert into " + table + " (id, score) values (1, -1)"
                    ).executeUpdate();
                });
                fail("Expected CHECK constraint violation, but insert succeeded.");
            } catch (Exception e) {
                SQLException sqlEx = findSQLException(e);
                assertNotNull(sqlEx);

                String extracted = extractor.extractConstraintName(sqlEx);
                assertNotNull("CHECK violation should contain constraint name", extracted);
                assertEquals(chkName.toUpperCase(), normalizeConstraintName(extracted).toUpperCase());
            }

        } finally {
            dropTableWithRetry(table);
        }
    }

    // ------------------------------------------------------------------------
    // 2) UNIQUE constraint violation (10007)
    // ------------------------------------------------------------------------
    @Test
    public void testViolatedConstraintNameExtractor_uniqueConstraint() {
        final String table = uniqueObjectName("UQ_TEST");
        final String uqName = uniqueObjectName("UQ_UK");

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

            // second insert -> UNIQUE violation
            try {
                inTransaction(session -> {
                    session.createNativeMutationQuery(
                            "insert into " + table + " (id, uk) values (2, 'A')"
                    ).executeUpdate();
                });
                fail("Expected UNIQUE constraint violation, but insert succeeded.");
            } catch (Exception e) {
                SQLException sqlException = findSQLException(e);
                assertNotNull("SQLException should be present in exception chain", sqlException);

                String extracted = extractor.extractConstraintName(sqlException);

                System.out.println("Extracted constraint name = " + extracted);
                assertNotNull("Extractor should return constraint name for UNIQUE violation", extracted);

                // Tibero가 대문자 normalize 할 수 있으므로 비교는 ignoreCase 권장
                assertEquals(uqName.toUpperCase(), normalizeConstraintName(extracted).toUpperCase());
            }

        } finally {
            dropTableWithRetry(table);
        }
    }

    // ------------------------------------------------------------------------
    // 3) INTEGRITY constraint violation (PK not found) (10008)
    //    - FK insert 시 부모 PK가 없으면 발생
    // ------------------------------------------------------------------------
    @Test
    public void testExtractor_integrityPkNotFound_fkInsert() {
        final String parent = uniqueObjectName("P_T");
        final String child  = uniqueObjectName("C_T");
        final String fkName = uniqueObjectName("FK_PC");

        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + parent + " (" +
                                "id number primary key" +
                                ")"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "create table " + child + " (" +
                                "id number primary key, " +
                                "parent_id number not null, " +
                                "constraint " + fkName + " foreign key(parent_id) references " + parent + "(id)" +
                                ")"
                ).executeUpdate();
            });

            try {
                inTransaction(session -> {
                    // parent_id = 999 : parent row 없음 -> PK not found -> integrity violation
                    session.createNativeMutationQuery(
                            "insert into " + child + " (id, parent_id) values (1, 999)"
                    ).executeUpdate();
                });
                fail("Expected integrity PK-not-found (FK insert) violation, but insert succeeded.");
            } catch (Exception e) {
                SQLException sqlEx = findSQLException(e);
                assertNotNull(sqlEx);

                String extracted = extractor.extractConstraintName(sqlEx);
                assertNotNull("Integrity PK-not-found should contain constraint name", extracted);

                // 보통 FK constraint name이 들어옴
                assertEquals(fkName.toUpperCase(), normalizeConstraintName(extracted).toUpperCase());
            }

        } finally {
            dropTableWithRetry(child);
            dropTableWithRetry(parent);
        }
    }

    // ------------------------------------------------------------------------
    // 4) INTEGRITY constraint violation (FK exists) (10009)
    //    - 부모 삭제 시 자식이 남아있으면 발생
    // ------------------------------------------------------------------------
    @Test
    public void testExtractor_integrityFkExists_parentDelete() {
        final String parent = uniqueObjectName("P2_T");
        final String child  = uniqueObjectName("C2_T");
        final String fkName = uniqueObjectName("FK_P2C2");

        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + parent + " (" +
                                "id number primary key" +
                                ")"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "create table " + child + " (" +
                                "id number primary key, " +
                                "parent_id number not null, " +
                                "constraint " + fkName + " foreign key(parent_id) references " + parent + "(id)" +
                                ")"
                ).executeUpdate();
            });

            // parent row + child row 삽입
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "insert into " + parent + " (id) values (1)"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "insert into " + child + " (id, parent_id) values (10, 1)"
                ).executeUpdate();
            });

            // parent delete -> FK exists 위반
            try {
                inTransaction(session -> {
                    session.createNativeMutationQuery(
                            "delete from " + parent + " where id = 1"
                    ).executeUpdate();
                });
                fail("Expected integrity FK-exists violation (parent delete), but delete succeeded.");
            } catch (Exception e) {
                SQLException sqlEx = findSQLException(e);
                assertNotNull(sqlEx);

                String extracted = extractor.extractConstraintName(sqlEx);
                assertNotNull("Integrity FK-exists should contain constraint name", extracted);
                assertEquals(fkName.toUpperCase(), normalizeConstraintName(extracted).toUpperCase());
            }

        } finally {
            dropTableWithRetry(child);
            dropTableWithRetry(parent);
        }
    }

    // ------------------------------------------------------------------------
    // 5) NOT NULL violation (10005)
    //    - extractor는 null 반환해야 함
    // ------------------------------------------------------------------------
    @Test
    public void testExtractor_notNullViolation_returnsNull() {
        final String table = uniqueObjectName("NN_T");

        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (" +
                                "id number primary key, " +
                                "name varchar2(100) not null" +
                                ")"
                ).executeUpdate();
            });

            try {
                inTransaction(session -> {
                    // name NULL => NOT NULL violation
                    session.createNativeMutationQuery(
                            "insert into " + table + " (id, name) values (1, null)"
                    ).executeUpdate();
                });
                fail("Expected NOT NULL violation, but insert succeeded.");
            } catch (Exception e) {
                SQLException sqlEx = findSQLException(e);
                assertNotNull(sqlEx);

                String extracted = extractor.extractConstraintName(sqlEx);

                // NOT NULL은 extractor에서 null 반환하도록 구현됨
                assertNull("NOT NULL violation should return null constraint name", extracted);
            }

        } finally {
            dropTableWithRetry(table);
        }
    }




    // ------------------------------------------------------------------------
    // Helper
    // ------------------------------------------------------------------------

    /**
     * 예외 체인에서 SQLException을 찾음
     */
    private SQLException findSQLException(Throwable t) {
        while (t != null) {
            if (t instanceof SQLException) {
                return (SQLException) t;
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
