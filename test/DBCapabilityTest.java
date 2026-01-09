import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * TiberoDialectCapabilityTest
 *
 * 목적:
 * 1) TiberoDialect가 supportsXXX()로 선언한 기능이
 * 2) 실제 Tibero DB에서도 동일하게 동작하는지 검증
 *
 * - 지원해야 하는 기능: SQL 실행 성공/결과 확인
 * - 지원하지 않아야 하는 기능: SQL 실행 시 예외 발생 확인
 *
 * NOTE:
 * Dialect contract 테스트(assertTrue/False)와 함께,
 * DB capability 테스트를 묶어서 확인한다.
 */
public class DBCapabilityTest extends AbstractTiberoDialectTestBase {

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

    // ------------------------------------------------------------------------
    // supportsTupleDistinctCounts()
    // ------------------------------------------------------------------------

    @Test
    public void testTupleDistinctCountNotSupportedByTibero() {
        // Dialect contract
        assertFalse(dialect.supportsTupleDistinctCounts());

        // DB capability check
        inTransaction(session -> {
            try {
                session.createNativeQuery(
                        "select count(distinct (1, 2)) from dual",
                        Long.class
                ).getSingleResult();

                fail("Expected Tibero to NOT support COUNT(DISTINCT (a,b)) syntax, but query succeeded.");
            } catch (Exception e) {
                // expected: parse error like "Missing right parenthesis"
                System.out.println("[Expected failure] Tuple DISTINCT count not supported: " + e.getMessage());
            }
        });
    }

    // ------------------------------------------------------------------------
    // supportsExistsInSelect()
    // ------------------------------------------------------------------------

    @Test
    public void testExistsInSelectNotSupportedByTibero() {
        // Dialect contract
        assertFalse(dialect.supportsExistsInSelect());

        // DB capability check
        inTransaction(session -> {
            try {
                session.createNativeQuery(
                        "select exists(select 1 from dual) from dual",
                        Integer.class
                ).getSingleResult();

                fail("Expected Tibero to NOT support EXISTS(...) directly in SELECT, but query succeeded.");
            } catch (Exception e) {
                System.out.println("[Expected failure] EXISTS in SELECT not supported: " + e.getMessage());
            }
        });
    }

    // ------------------------------------------------------------------------
    // supportsPartitionBy()
    // ------------------------------------------------------------------------

    @Test
    public void testPartitionBySupportedByTibero() {
        // Dialect contract
        assertTrue(dialect.supportsPartitionBy());

        // DB capability check
        inTransaction(session -> {
            // simplest window function with partition by
            Long result = session.createNativeQuery(
                    "select sum(1) over(partition by 1) from dual",
                    Long.class
            ).getSingleResult();

            assertNotNull(result);
            assertEquals(1L, result.longValue());
        });
    }

    // ------------------------------------------------------------------------
    // supportsCurrentTimestampSelection() + getCurrentTimestampSelectString() + isCurrentTimestampSelectStringCallable()
    // ------------------------------------------------------------------------

    @Test
    public void testCurrentTimestampSelectionSupportedByTibero() {
        assertTrue(dialect.supportsCurrentTimestampSelection());
        assertNotNull(dialect.getCurrentTimestampSelectString());
        assertFalse(dialect.isCurrentTimestampSelectStringCallable());

        inTransaction(session -> {
            String sql = dialect.getCurrentTimestampSelectString();
            Object ts = session.createNativeQuery(sql, Object.class).getSingleResult();
            assertNotNull("Current timestamp selection should return a value", ts);
        });
    }

    // ------------------------------------------------------------------------
    // supportsCommentOn()
    // ------------------------------------------------------------------------

    @Test
    public void testSupportsCommentOnAndCommentExecution() {
        assertTrue(dialect.supportsCommentOn());

        final String table = uniqueObjectName("COMMENT_TEST");

        try {
            inTransaction(session -> {
                // setup: create table
                session.createNativeMutationQuery(
                        "create table " + table + " (id number)"
                ).executeUpdate();

                // comment on table / column
                session.createNativeMutationQuery(
                        "comment on table " + table + " is 'my_table_comment'"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "comment on column " + table + ".id is 'my_col_comment'"
                ).executeUpdate();

                // table comment 확인
                String tableComment = session.createNativeQuery(
                        "select comments from user_tab_comments where table_name = '" + table.toUpperCase() + "'",
                        String.class
                ).getSingleResult();

                assertEquals("my_table_comment", tableComment);

                // column comment 확인
                String colComment = session.createNativeQuery(
                        "select comments from user_col_comments where table_name = '" + table.toUpperCase() + "' and column_name = 'ID'",
                        String.class
                ).getSingleResult();

                assertEquals("my_col_comment", colComment);
            });
        } finally {
            // cleanup (항상 실행)
            dropTableWithRetry(table);
        }

    }

    // ------------------------------------------------------------------------
    // canCreateSchema()
    // ------------------------------------------------------------------------

    @Test
    public void testCreateSchemaStatementFailsInTibero_optional() {
        assertFalse(dialect.canCreateSchema());

        inTransaction(session -> {
            try {
                session.createNativeMutationQuery("create schema SOME_SCHEMA").executeUpdate();
                fail("Expected create schema to fail in Tibero (schema=user concept), but it succeeded.");
            } catch (Exception e) {
                System.out.println("[Expected failure] CREATE SCHEMA failed: " + e.getMessage());
            }
        });
    }


}
