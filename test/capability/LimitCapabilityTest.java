package capability;

import support.AbstractTiberoDialectTestBase;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TiberoDialectLimitCapabilityTest
 *
 * 목적:
 *  1) TiberoDialect가 선언한 limit 값들이 contract 상 정확한지 확인
 *  2) 일부 값은 실제 Tibero DB에서 동작(제약)하는지 실제 SQL로 capability 검증
 *
 * 검증 대상:
 *  - getMaxVarcharLength() = 65532  (DB DDL로 검증)
 *  - getMaxVarbinaryLength() = 2000 (DB DDL로 검증, RAW)
 *  - getMaxIdentifierLength() = 128 (DB DDL로 검증)
 *  - getMaxAliasLength() = 118 (contract-only)
 */
public class LimitCapabilityTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] { }; // 엔티티 불필요 (native DDL/DML로 검증)
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
    // 1) Contract-only (빠르고 안정적)
    // ------------------------------------------------------------------------

    @Test
    public void testDialectFeatureFlagsAndLimits_contractOnly() {
        assertEquals(65532, dialect.getMaxVarcharLength());
        assertEquals(2000, dialect.getMaxVarbinaryLength());

        assertEquals(128, dialect.getMaxIdentifierLength());

        assertEquals(118, dialect.getMaxAliasLength());
    }

    // ------------------------------------------------------------------------
    // 2) DB Capability 검증 - max varchar length (65532)
    // ------------------------------------------------------------------------

    @Test
    public void testMaxVarcharLength_supportedByTibero() {
        int max = dialect.getMaxVarcharLength(); // 현재 Dialect 값

        // length == max should succeed
        String tableSucceed = uniqueObjectName("VC_OK");
        // length == max+1 should fail
        String tableFail = uniqueObjectName("VC_FAIL");

        String ddlSucceed = "create table " + tableSucceed + " (" +
                "id number primary key, " +
                "v varchar2(" + max + ")" +
                ")";

        String ddlFail = "create table " + tableFail + " (" +
                "id number primary key, " +
                "v varchar2(" + (max + 1) + ")" +
                ")";

        try {
            // 1) varchar2(max) create should succeed
            inTransaction(session -> session.createNativeMutationQuery(ddlSucceed).executeUpdate());

            // 2) varchar2(max+1) create should fail
            try {
                inTransaction(session -> session.createNativeMutationQuery(ddlFail).executeUpdate());
                fail("Expected varchar2(" + (max + 1) + ") to fail, but it succeeded.");
            } catch (Exception e) {
                // expected
            }

        } finally {
            try { dropTableWithRetry(tableSucceed); } catch (Exception ignored) {}
            try { dropTableWithRetry(tableFail); } catch (Exception ignored) {}
        }
    }

    // ------------------------------------------------------------------------
    // 3) DB Capability 검증 - max varbinary length (2000) (VARBINARY -> RAW 매핑)
    // ------------------------------------------------------------------------

    @Test
    public void testMaxVarbinaryLength_supportedByTibero() {
        int max = dialect.getMaxVarbinaryLength(); // 현재 Dialect 값

        String tableSucceed = uniqueObjectName("VB_OK");
        String tableFail = uniqueObjectName("VB_FAIL");

        String ddlSucceed = "create table " + tableSucceed + " (" +
                "id number primary key, " +
                "v raw(" + max + ")" +
                ")";

        String ddlFail = "create table " + tableFail + " (" +
                "id number primary key, " +
                "v raw(" + (max + 1) + ")" +
                ")";

        try {
            // 1) raw(max) create should succeed
            inTransaction(session -> session.createNativeMutationQuery(ddlSucceed).executeUpdate());

            // 2) raw(max+1) create should fail
            try {
                inTransaction(session -> session.createNativeMutationQuery(ddlFail).executeUpdate());
                fail("Expected raw(" + (max + 1) + ") to fail, but it succeeded.");
            } catch (Exception e) {
                // expected
            }

        } finally {
            try { dropTableWithRetry(tableSucceed); } catch (Exception ignored) {}
            try { dropTableWithRetry(tableFail); } catch (Exception ignored) {}
        }
    }

    // ------------------------------------------------------------------------
    // 4) DB Capability 검증 - max identifier length (128)
    // ------------------------------------------------------------------------

    @Test
    public void testMaxIdentifierLength_supportedByTibero() {
        String tableSucceed = repeat('a', dialect.getMaxIdentifierLength());
        String tableFail = repeat('a', dialect.getMaxIdentifierLength() + 1);

        try {
            String ddlSucceed = "create table " + tableSucceed + " (" +
                    "id number primary key " +
                    ")";

            inTransaction(session ->
                    session.createNativeMutationQuery(ddlSucceed)
                            .executeUpdate()
            );

            String ddlFail = "create table " + tableFail + " (" +
                    "id number primary key " +
                    ")";

            try {
                inTransaction(session ->
                        session.createNativeMutationQuery(ddlFail)
                                .executeUpdate()
                );
                fail("Expected identifier with length 129 to fail, but it succeeded.");
            } catch (Exception e) {
                // expected
            }

        } finally {
            try { dropTableWithRetry(tableSucceed); } catch (Exception ignored) {}
            try { dropTableWithRetry(tableFail); } catch (Exception ignored) {}
        }
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private String repeat(char c, int n) {
        StringBuilder sb = new StringBuilder(n);
        for (int i = 0; i < n; i++) sb.append(c);
        return sb.toString();
    }

    private String buildInList(int start, int end) {
        StringBuilder sb = new StringBuilder();
        for (int i = start; i <= end; i++) {
            if (i > start) sb.append(",");
            sb.append(i);
        }
        return sb.toString();
    }
}
