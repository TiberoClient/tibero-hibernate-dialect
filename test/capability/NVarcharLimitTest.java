package capability;

import support.AbstractTiberoDialectTestBase;
import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * NVARCHAR2 상한 회귀 테스트.
 *
 * 전수조사에서 발견된 결함: Dialect 기본 구현은 getMaxNVarcharLength() 가
 * getMaxVarcharLength() 를 그대로 반환하므로 65532 가 되지만,
 * Tibero 실측 상한은 32766 임 (32767 부터 JDBC-5079 Data type length is out of range).
 */
public class NVarcharLimitTest extends AbstractTiberoDialectTestBase {

    private Dialect dialect;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
    }

    @Before
    public void setUp() {
        SessionFactoryImplementor sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
    }

    // ------------------------------------------------------------------------
    // contract
    // ------------------------------------------------------------------------

    @Test
    public void nvarcharLimits_contract() {
        TiberoDialect d = new TiberoDialect();
        assertEquals(32766, d.getMaxNVarcharLength());
        assertEquals(32766, d.getMaxNVarcharCapacity());

        // varchar 상한과 분리되어 있어야 함 (기본 구현은 동일값을 반환)
        assertEquals(65532, d.getMaxVarcharLength());
        assertNotEquals(d.getMaxVarcharLength(), d.getMaxNVarcharLength());
    }

    // ------------------------------------------------------------------------
    // DB capability
    // ------------------------------------------------------------------------

    @Test
    public void nvarchar2_atDeclaredMax_succeeds() {
        String table = uniqueObjectName("NVMAX");
        try {
            inTransaction(session ->
                    session.createNativeMutationQuery(
                            "create table " + table + " (c nvarchar2(" + dialect.getMaxNVarcharLength() + "))"
                    ).executeUpdate());
        } finally {
            dropTableWithRetry(table);
        }
    }

    @Test
    public void nvarchar2_aboveDeclaredMax_fails() {
        String table = uniqueObjectName("NVOVER");
        try {
            inTransaction(session ->
                    session.createNativeMutationQuery(
                            "create table " + table + " (c nvarchar2(" + (dialect.getMaxNVarcharLength() + 1) + "))"
                    ).executeUpdate());
            fail("Expected nvarchar2(" + (dialect.getMaxNVarcharLength() + 1) + ") to fail on Tibero");
        } catch (Exception e) {
        } finally {
            try {
                dropTableWithRetry(table);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void nvarchar2_atVarcharMax_wouldFail_provingLimitsDiffer() {
        String table = uniqueObjectName("NVVCH");
        try {
            inTransaction(session ->
                    session.createNativeMutationQuery(
                            "create table " + table + " (c nvarchar2(" + dialect.getMaxVarcharLength() + "))"
                    ).executeUpdate());
            fail("nvarchar2(" + dialect.getMaxVarcharLength()
                    + ") should fail — varchar 상한을 nvarchar 에 그대로 쓰면 안 됨");
        } catch (Exception e) {
        } finally {
            try {
                dropTableWithRetry(table);
            } catch (Exception ignored) {
            }
        }
    }
}
