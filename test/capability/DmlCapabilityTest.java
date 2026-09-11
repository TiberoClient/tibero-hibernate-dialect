package capability;

import support.AbstractTiberoDialectTestBase;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * DML/DDL 훅 Tibero DB 실측 (merge / IF EXISTS / MODIFY / generated / constraint)
 */
public class DmlCapabilityTest extends AbstractTiberoDialectTestBase {

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
    public void merge_statement_works() {
        String t = uniqueObjectName("MER");
        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + t + " (id number primary key, v varchar2(20))"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "insert into " + t + " values (1, 'a')"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "merge into " + t + " d using (select 1 as id, 'b' as v from dual) s "
                                + "on (d.id = s.id) when matched then update set d.v = s.v "
                                + "when not matched then insert (id, v) values (s.id, s.v)"
                ).executeUpdate();
            });
            String v = inTransactionReturning(session ->
                    session.createNativeQuery("select v from " + t + " where id=1", String.class)
                            .getSingleResult());
            assertEquals("b", v);
        } finally {
            dropTableWithRetry(t);
        }
    }

    @Test
    public void dropTableIfExists_and_modifyColumn_work() {
        String t = uniqueObjectName("DDL");
        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + t + " (id number primary key, c varchar2(10))"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        "alter table " + t + " " + dialect.getAlterColumnTypeString("c", "varchar2(20)", null)
                ).executeUpdate();
            });
            assertTrue(dialect.supportsAlterColumnType());
            assertTrue(dialect.supportsIfExistsBeforeTableName());
            inTransaction(session ->
                    session.createNativeMutationQuery("drop table if exists " + t).executeUpdate());
        } finally {
            // already dropped; ignore
            try {
                dropTableWithRetry(t);
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void generatedAlwaysAs_and_constraintToggle_work() {
        String t = uniqueObjectName("GEN");
        String c = "CK_" + t.substring(Math.max(0, t.length() - 20));
        try {
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + t + " (a number, b number"
                                + dialect.generatedAs("a+1") + ", constraint " + c + " check (a > 0))"
                ).executeUpdate();
                session.createNativeMutationQuery(
                        dialect.getDisableConstraintStatement(t, c)
                ).executeUpdate();
                session.createNativeMutationQuery(
                        dialect.getEnableConstraintStatement(t, c)
                ).executeUpdate();
            });
            assertTrue(dialect.canDisableConstraints());
        } finally {
            dropTableWithRetry(t);
        }
    }

    @Test
    public void valuesList_notSupported_flag() {
        assertFalse(dialect.supportsValuesList());
    }

    @Test
    public void hextoraw_literal_works() {
        Object o = inTransactionReturning(session ->
                session.createNativeQuery("select hextoraw('0a1b') from dual", Object.class)
                        .getSingleResult());
        assertNotNull(o);
    }
}
