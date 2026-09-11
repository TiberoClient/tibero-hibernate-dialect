package capability;

import support.AbstractTiberoDialectTestBase;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * dual / empty-string Tibero DB 실측
 */
public class DualEmptyStringCapabilityTest extends AbstractTiberoDialectTestBase {

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

    @Test
    public void selectWithoutFrom_fails_butFromDual_succeeds() {
        try {
            inTransaction(session ->
                    session.createNativeQuery("select 1", Integer.class).getSingleResult()
            );
            fail("Expected select without FROM to fail on Tibero");
        } catch (Exception e) {
            // expected
        }

        String sql = "select 1" + dialect.getFromDualForSelectOnly();
        Integer one = inTransactionReturning(session ->
                session.createNativeQuery(sql, Integer.class).getSingleResult()
        );
        assertEquals(Integer.valueOf(1), one);
    }

    @Test
    public void emptyString_isStoredAsNull() {
        String table = uniqueObjectName("EMPTY_STR");
        try {
            inTransaction(session -> session.createNativeMutationQuery(
                    "create table " + table + " (id number primary key, v varchar2(10))"
            ).executeUpdate());

            inTransaction(session -> session.createNativeMutationQuery(
                    "insert into " + table + " (id, v) values (1, '')"
            ).executeUpdate());

            Long nullCnt = inTransactionReturning(session ->
                    session.createNativeQuery(
                            "select count(*) from " + table + " where id=1 and v is null",
                            Long.class
                    ).getSingleResult()
            );
            assertEquals("empty string should be stored as NULL", Long.valueOf(1), nullCnt);

            assertTrue(
                    "Dialect contract must match DB behavior",
                    dialect.isEmptyStringTreatedAsNull()
            );
        } finally {
            dropTableWithRetry(table);
        }
    }
}
