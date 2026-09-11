package capability;

import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.temptable.TemporaryTableKind;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Locale;

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

    /**
     * {@code getCurrentSchemaCommand()} 가 실제로 현재 스키마를 돌려주는지 본다.
     *
     * <p>기준값을 {@code "TIBERO"} 같은 리터럴로 두면 <b>그 계정으로 접속할 때만</b>
     * 통과한다. 계정을 바꿔 돌리면 dialect 가 멀쩡한데도 테스트가 깨진다.
     * 그래서 기준값을 접속한 커넥션 자신에게서 얻는다 —
     * {@code Connection.getSchema()} 는 JDBC 표준이고, tbjdbc 는 여기에
     * {@code SYS_CONTEXT('USERENV','CURRENT_SCHEMA')} 와 같은 값을 돌려준다(실측).
     *
     * @see #currentSchemaCommand_tracksCurrentSchema_notLoginUser
     *      로그인 계정이 아니라 현재 스키마를 보는지까지 가르는 쪽
     */
    @Test
    public void testGetCurrentSchemaCommandTest() {
        String schemaCommand = dialect.getCurrentSchemaCommand();

        assertNotNull("Dialect에 schema command가 정의되어 있어야 합니다.", schemaCommand);

        inSession(session -> {
            String viaDialect = session.createNativeQuery(schemaCommand, String.class).getSingleResult();
            assertNotNull(viaDialect);

            String viaJdbc = session.doReturningWork(Connection::getSchema);
            assertNotNull("tbjdbc 가 현재 스키마를 보고해야 기준값을 잡을 수 있음", viaJdbc);

            assertEquals("dialect 의 schema command 결과가 JDBC 가 보고하는 현재 스키마와 달라짐",
                    viaJdbc.toUpperCase(Locale.ROOT), viaDialect.toUpperCase(Locale.ROOT));
        });
    }

    /**
     * 명령이 <b>로그인 계정</b>이 아니라 <b>현재 스키마</b>를 돌려주는지 가른다.
     *
     * <p>평소에는 둘이 같은 값이라 구분되지 않는다. {@code select user from dual} 로
     * 잘못 구현해도 위 테스트는 그대로 통과한다. 그래서 세션의 현재 스키마를 잠깐
     * 다른 것으로 바꿔 둘을 갈라놓고 본다 — 그 상태에서 명령이 따라오지 않으면
     * 계정을 보고 있는 것이다.
     *
     * <p>커넥션이 풀로 돌아가므로 {@code finally} 에서 반드시 되돌린다.
     */
    @Test
    public void currentSchemaCommand_tracksCurrentSchema_notLoginUser() {
        final String schemaCommand = dialect.getCurrentSchemaCommand();

        inSession(session -> session.doWork(conn -> {
            final String loginSchema = conn.getSchema();
            final String otherSchema = pickOtherSchema(conn, loginSchema);
            if (otherSchema == null) {
                // 전환할 다른 스키마가 보이지 않는 계정이면 이 단언은 세울 수 없다
                return;
            }

            try {
                conn.setSchema(otherSchema);
                assertEquals("현재 스키마를 바꿨으면 명령 결과도 따라와야 함 — 로그인 계정을 돌려주면 안 됨",
                        otherSchema.toUpperCase(Locale.ROOT),
                        queryOne(conn, schemaCommand).toUpperCase(Locale.ROOT));
            } finally {
                conn.setSchema(loginSchema);
            }
        }));
    }

    /** 현재 스키마가 아닌 아무 스키마 하나. 없으면 {@code null}. */
    private static String pickOtherSchema(Connection conn, String current) throws SQLException {
        try (Statement st = conn.createStatement();
             ResultSet rs = st.executeQuery(
                     "select username from all_users where username <> '" + current + "' and rownum = 1")) {
            return rs.next() ? rs.getString(1) : null;
        }
    }

    private static String queryOne(Connection conn, String sql) throws SQLException {
        try (Statement st = conn.createStatement(); ResultSet rs = st.executeQuery(sql)) {
            assertTrue("결과가 한 행은 나와야 함: " + sql, rs.next());
            return rs.getString(1);
        }
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
            }
        });
    }
}

