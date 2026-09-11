package capability;

import support.AbstractTiberoDialectTestBase;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleBooleanJdbcType;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.OracleJsonBlobJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * P1 플래그·타입 Tibero DB 실측
 */
public class FeatureFlagsCapabilityTest extends AbstractTiberoDialectTestBase {

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
    public void fetchFirst_and_offsetFetch_work() {
        Integer a = inTransactionReturning(session ->
                session.createNativeQuery("select 1 from dual fetch first 1 row only", Integer.class)
                        .getSingleResult());
        assertEquals(Integer.valueOf(1), a);

        Integer b = inTransactionReturning(session ->
                session.createNativeQuery(
                        "select * from (select 1 as n from dual offset 0 rows fetch next 1 row only) t",
                        Integer.class
                ).getSingleResult());
        assertEquals(Integer.valueOf(1), b);
        assertTrue(dialect.supportsFetchClause(org.hibernate.query.sqm.FetchClauseType.ROWS_ONLY));
        assertTrue(dialect.supportsOffsetInSubquery());
    }

    @Test
    public void windowFunctions_work() {
        Object rank = inTransactionReturning(session ->
                session.createNativeQuery("select rank() over (order by 1) from dual", Object.class)
                        .getSingleResult());
        assertNotNull(rank);
        assertTrue(dialect.supportsWindowFunctions());
    }

    @Test
    public void recursiveCte_withoutRecursiveKeyword_works() {
        Long n = inTransactionReturning(session ->
                session.createNativeQuery(
                        "with t(n) as (select 1 from dual union all select n+1 from t where n < 2) select max(n) from t",
                        Long.class
                ).getSingleResult());
        assertEquals(Long.valueOf(2), n);
        assertTrue(dialect.supportsRecursiveCTE());
    }

    @Test
    public void lateral_notSupported() {
        assertFalse(dialect.supportsLateral());
        try {
            inTransaction(session ->
                    session.createNativeQuery(
                            "select * from dual d, lateral (select 1 as x from dual) l",
                            Object.class
                    ).getResultList());
            fail("Expected LATERAL to fail on Tibero");
        } catch (Exception e) {
        }
    }

    @Test
    public void contributeTypes_registersBooleanAndJson() {
        JdbcTypeRegistry registry = sessionFactory()
                .getTypeConfiguration()
                .getJdbcTypeRegistry();
        assertSame(OracleBooleanJdbcType.INSTANCE, registry.getDescriptor(SqlTypes.BOOLEAN));
        assertNotNull(registry.getDescriptor(SqlTypes.JSON));
        assertTrue(registry.getDescriptor(SqlTypes.JSON) instanceof OracleJsonBlobJdbcType);
    }

    @Test
    public void currentDate_localtimestamp_native() {
        Object d = inTransactionReturning(session ->
                session.createNativeQuery("select " + dialect.currentDate() + " from dual", Object.class)
                        .getSingleResult());
        assertNotNull(d);
        Object lt = inTransactionReturning(session ->
                session.createNativeQuery("select " + dialect.currentLocalTimestamp() + " from dual", Object.class)
                        .getSingleResult());
        assertNotNull(lt);
    }
}
