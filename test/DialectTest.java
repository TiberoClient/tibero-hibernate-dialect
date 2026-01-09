import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.temptable.TemporaryTableKind;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class DialectTest extends BaseCoreFunctionalTestCase {

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
    }

    @Test
    public void testGetCurrentSchemaCommandTest() {
        String schemaCommand = dialect.getCurrentSchemaCommand();

        assertNotNull("Dialect에 schema command가 정의되어 있어야 합니다.", schemaCommand);

        inSession(session -> {
            String currentSchema = session.createNativeQuery(schemaCommand, String.class).getSingleResult();
            //System.out.println("Detected Schema From Tibero : " + currentSchema);
            assertNotNull(currentSchema);
            assertEquals("TIBERO", currentSchema.toUpperCase());
        });
    }

    @Test
    public void testGetCurrentTimestampSelectString() {
        // Dialect가 제공하는 현재 Timestamp 조회 SQL을 가져온다.
        String timestampQuery = dialect.getCurrentTimestampSelectString();

        assertNotNull("Dialect에 current timestamp query가 정의되어 있어야 합니다.", timestampQuery);

        inSession(session -> {
            Object result = session.createNativeQuery(timestampQuery, Object.class).getSingleResult();
            //System.out.println("Detected Timestamp From Tibero : " + result);
            assertNotNull("현재 Timestamp 결과는 null이면 안 됩니다.", result);
            assertTrue(
                    "Timestamp 결과는 java.sql.Timestamp 또는 java.util.Date 계열이어야 합니다.",
                    result instanceof java.sql.Timestamp || result instanceof java.util.Date
            );
        });
    }

    /**
     * Tests fragment generation and keyword behaviors specific to the database dialect.
     */
    @Test
    public void testFragmentsAndKeywords() {
        assertEquals("current_timestamp", dialect.currentTimestampWithTimeZone());
        assertEquals("add", dialect.getAddColumnString());
    }

    /**
     * getSupportedTemporaryTableKind(), getTemporaryTableCreateOptions() 검증
     * 1. fragment test
     * 2. global, on commit delete rows 문법이 유효
     * 3. commit 이후 row가 실제로 delete 되는 동작까지 검증
     */
    @Test
    public void testGlobalTemporaryTableOnCommitDeleteRowsBehavior() {
        TemporaryTableKind kind = dialect.getSupportedTemporaryTableKind();
        String options = dialect.getTemporaryTableCreateOptions();

        assertEquals(TemporaryTableKind.GLOBAL, kind);
        assertEquals("on commit delete rows", options);

        // kind에 따라 temp table prefix 결정
        String kindKeyword = (kind == TemporaryTableKind.GLOBAL) ? "global temporary" : "temporary";

        String ddl = "create " + kindKeyword + " table TMP_KIND_OPT_TEST (id number) " + options;
        String drop = "drop table TMP_KIND_OPT_TEST";
        String insert = "insert into TMP_KIND_OPT_TEST values (1)";
        String count = "select count(*) from TMP_KIND_OPT_TEST";

        // 1) create + insert + count before commit -> 1 row
        inTransaction(session -> {
            try {
                session.createNativeMutationQuery(drop).executeUpdate();
            } catch (Exception ignored) {}

            session.createNativeMutationQuery(ddl).executeUpdate();
            session.createNativeMutationQuery(insert).executeUpdate();

            long beforeCommit = session.createNativeQuery(count, Long.class).getSingleResult();
            assertEquals("Row should exist before commit", 1L, beforeCommit);
        });

        // 2) after commit: on commit delete rows -> 0 row
        inTransaction(session -> {
            long afterCommit = session.createNativeQuery(count, Long.class).getSingleResult();
            assertEquals("Rows should be deleted after commit", 0L, afterCommit);

            session.createNativeMutationQuery(drop).executeUpdate();
        });
    }

    @Test
    public void testTiberoDoesNotSupportTupleDistinctCountSyntax() {
        assertFalse(dialect.supportsTupleDistinctCounts());

        inTransaction(session -> {
            try {
                session.createNativeQuery(
                        "select count(distinct (1, 2)) from dual",
                        Long.class
                ).getSingleResult();

                fail("Tibero should not support COUNT(DISTINCT (a,b)) syntax, but query succeeded.");
            } catch (Exception e) {
                // expected: syntax error / invalid expression etc.
                System.out.println("[Expected failure] Tuple distinct count not supported: " + e.getMessage());
            }
        });
    }
}























