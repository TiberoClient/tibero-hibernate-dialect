import com.tmax.tibero.hibernate.dialect.TiberoTypes;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.procedure.internal.StandardCallableStatementSupport;
import org.junit.Before;
import org.junit.Test;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * DialectRefCursorTest
 *
 * 목적:
 *  - TiberoDialect의 REF CURSOR 관련 API 동작 검증
 *  - Unit Test(Mock): CallableStatement 조작 메서드(registerResultSetOutParameter/getResultSet) 검증
 *  - Integration Test(DB): Tibero DB에서 실제 SYS_REFCURSOR procedure 생성 후 호출 검증
 *
 * 포함하는 Dialect API:
 *  - registerResultSetOutParameter(CallableStatement,int)
 *  - registerResultSetOutParameter(CallableStatement,String)
 *  - getResultSet(CallableStatement)
 *  - getResultSet(CallableStatement,int)
 *  - getResultSet(CallableStatement,String)
 *  - getCallableStatementSupport()
 */
public class RefCursorTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
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
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
    }

    // ========================================================================
    // UNIT TESTS (Mock-based)
    // ========================================================================

    @Test
    public void unit_registerResultSetOutParameter_byIndex() throws Exception {
        CallableStatement cs = mock(CallableStatement.class);

        int next = dialect.registerResultSetOutParameter(cs, 1);

        verify(cs).registerOutParameter(1, TiberoTypes.CURSOR);
        assertEquals("Should return next index", 2, next);
    }

    @Test
    public void unit_registerResultSetOutParameter_byName() throws Exception {
        CallableStatement cs = mock(CallableStatement.class);

        int result = dialect.registerResultSetOutParameter(cs, "out_cursor");

        verify(cs).registerOutParameter("out_cursor", TiberoTypes.CURSOR);
        assertEquals("Conventionally returns 1", 1, result);
    }

    @Test
    public void unit_getResultSet_executeAndGetObjectAt1() throws Exception {
        CallableStatement cs = mock(CallableStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(cs.getObject(1)).thenReturn(rs);

        ResultSet actual = dialect.getResultSet(cs);

        verify(cs).execute();
        verify(cs).getObject(1);
        assertSame(rs, actual);
    }

    @Test
    public void unit_getResultSet_byPosition() throws Exception {
        CallableStatement cs = mock(CallableStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(cs.getObject(2)).thenReturn(rs);

        ResultSet actual = dialect.getResultSet(cs, 2);

        verify(cs).getObject(2);
        assertSame(rs, actual);
    }

    @Test
    public void unit_getResultSet_byName() throws Exception {
        CallableStatement cs = mock(CallableStatement.class);
        ResultSet rs = mock(ResultSet.class);

        when(cs.getObject("out_cursor")).thenReturn(rs);

        ResultSet actual = dialect.getResultSet(cs, "out_cursor");

        verify(cs).getObject("out_cursor");
        assertSame(rs, actual);
    }

    @Test
    public void unit_getCallableStatementSupport_returnsRefCursorInstance() {
        assertSame(
                "Dialect should use REF_CURSOR_INSTANCE",
                StandardCallableStatementSupport.REF_CURSOR_INSTANCE,
                dialect.getCallableStatementSupport()
        );
    }

    // ========================================================================
    // INTEGRATION TEST (DB-based)
    // ========================================================================

    /**
     * 실제 Tibero DB에서 SYS_REFCURSOR OUT procedure 생성 후,
     * CallableStatement로 실행하여 ResultSet이 반환되는지 검증
     */
    @Test
    public void integration_refCursorProcedure_returnsResultSet() {
        final String table = uniqueObjectName("RC_T");
        final String proc = uniqueObjectName("RC_P");

        try {
            // 1) table + data
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        "create table " + table + " (id number primary key, name varchar2(20))"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "insert into " + table + " values (1, 'a')"
                ).executeUpdate();

                session.createNativeMutationQuery(
                        "insert into " + table + " values (2, 'b')"
                ).executeUpdate();
            });

            // 2) procedure 생성 (OUT SYS_REFCURSOR)
            inTransaction(session -> {
                try {
                    session.createNativeMutationQuery("drop procedure " + proc).executeUpdate();
                } catch (Exception ignored) {}

                String ddl =
                        "create procedure " + proc + " (p_cursor out sys_refcursor) as " +
                                "begin " +
                                "  open p_cursor for select id, name from " + table + " order by id; " +
                                "end;";

                session.createNativeMutationQuery(ddl).executeUpdate();
            });

            // 3) JDBC callable 실행 + dialect helper로 ResultSet 추출
            /**
             * index 기반 registerOutParameter + dialect.getResultSet(cs)
             */
            inSession(session -> {
                session.doWork((Connection connection) -> {
                    try (CallableStatement cs = connection.prepareCall("{ call " + proc + "(?) }")) {

                        // OUT cursor 등록
                        dialect.registerResultSetOutParameter(cs, 1);

                        // execute + resultset 획득
                        ResultSet rs = dialect.getResultSet(cs);

                        assertNotNull("ResultSet should not be null", rs);

                        assertTrue(rs.next());
                        assertEquals(1L, rs.getLong(1));
                        assertEquals("a", rs.getString(2));

                        assertTrue(rs.next());
                        assertEquals(2L, rs.getLong(1));
                        assertEquals("b", rs.getString(2));

                        assertFalse(rs.next());
                    }
                });
            });

            /**
             * index 기반 registerOutParameter + dialect.getResultSet(cs, position)
             */
            inSession(session -> {
                session.doWork((Connection connection) -> {
                    try (CallableStatement cs = connection.prepareCall("{ call " + proc + "(?) }")) {

                        // OUT cursor 등록
                        dialect.registerResultSetOutParameter(cs, 1);

                        cs.execute();
                        // execute + resultset 획득
                        ResultSet rs = dialect.getResultSet(cs, 1);

                        assertNotNull("ResultSet should not be null", rs);

                        assertTrue(rs.next());
                        assertEquals(1L, rs.getLong(1));
                        assertEquals("a", rs.getString(2));

                        assertTrue(rs.next());
                        assertEquals(2L, rs.getLong(1));
                        assertEquals("b", rs.getString(2));

                        assertFalse(rs.next());
                    }
                });
            });

            /**
             * named parameter 기반 registerOutParameter + dialect.getResultSet(cs, name)
             *
             * ⚠️ 주의:
             *  - Tibero JDBC driver가 named parameter OUT cursor 지원해야 함
             *  - 지원하지 않으면 SQLException 발생 가능
             */
            inSession(session -> {
                session.doWork((Connection connection) -> {
                    try (CallableStatement cs = connection.prepareCall("{ call " + proc + "(?) }")) {

                        // OUT cursor 등록
                        dialect.registerResultSetOutParameter(cs, "P_CURSOR");

                        cs.execute();
                        // execute + resultset 획득
                        ResultSet rs = dialect.getResultSet(cs, "P_CURSOR");

                        assertNotNull("ResultSet should not be null", rs);

                        assertTrue(rs.next());
                        assertEquals(1L, rs.getLong(1));
                        assertEquals("a", rs.getString(2));

                        assertTrue(rs.next());
                        assertEquals(2L, rs.getLong(1));
                        assertEquals("b", rs.getString(2));

                        assertFalse(rs.next());
                    }
                });
            });
        } finally {
            // cleanup
            inTransaction(session -> {
                try {
                    session.createNativeMutationQuery("drop procedure " + proc).executeUpdate();
                } catch (Exception ignored) {}
            });
            dropTableWithRetry(table);
        }
    }
}
