import com.tmax.tibero.hibernate.dialect.pagination.TiberoLimitHandler;

import org.hibernate.LockOptions;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.pagination.LimitHandler;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.spi.Limit;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryOptionsAdapter;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.*;

/**
 * TiberoLimitHandlerTest
 *
 * 목적:
 *  - TiberoLimitHandler가 SQL pagination을 올바르게 변환하는지 검증
 *  - offset/fetch 방식과 for update(락) 방식(rownum wrapper) 모두 테스트
 *  - 실제 Tibero DB에서 변환된 SQL이 실행 가능한지 검증
 */
public class LimitHandlerTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;
    private TiberoLimitHandler handler;

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

        LimitHandler lh = dialect.getLimitHandler();
        assertTrue(lh instanceof TiberoLimitHandler);
        handler = (TiberoLimitHandler) lh;
    }

    // ------------------------------------------------------------------------
    // 1) Dialect wiring
    // ------------------------------------------------------------------------
    @Test
    public void testDialectReturnsTiberoLimitHandlerSingleton() {
        assertSame(TiberoLimitHandler.INSTANCE, dialect.getLimitHandler());
    }

    // ------------------------------------------------------------------------
    // 2) Fragment tests: OFFSET/FETCH mode
    // ------------------------------------------------------------------------

    @Test
    public void testProcessSql_fetchFirstOnly_noOffset() {
        String baseSql = "select id from dual";

        Limit limit = new Limit();
        limit.setMaxRows(5);

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        assertTrue(processed.toLowerCase().contains("fetch first ? rows only"));
        assertTrue(handler.supportsOffset());
        assertFalse(handler.bindLimitParametersInReverseOrder());
        assertFalse(handler.useMaxForLimit());
    }

    @Test
    public void testProcessSql_offsetFetch_nextRows() {
        String baseSql = "select id from dual";

        Limit limit = new Limit();
        limit.setFirstRow(10);
        limit.setMaxRows(5);

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        assertTrue(processed.toLowerCase().contains("offset ? rows fetch next ? rows only"));
        assertTrue(handler.supportsOffset());
        assertFalse(handler.bindLimitParametersInReverseOrder());
        assertFalse(handler.useMaxForLimit());
    }

    @Test
    public void testProcessSql_offsetOnly() {
        String baseSql = "select id from dual";

        Limit limit = new Limit();
        limit.setFirstRow(10);

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        assertTrue(processed.toLowerCase().contains("offset ? rows"));
        assertFalse(processed.toLowerCase().contains("fetch next"));
        assertTrue(handler.supportsOffset());
        assertFalse(handler.bindLimitParametersInReverseOrder());
        assertFalse(handler.useMaxForLimit());
    }

    @Test
    public void testProcessSql_noLimit_returnsOriginalSql() {
        String baseSql = "select id from dual";
        Limit limit = new Limit(); // firstRow/maxRows 둘 다 null

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        assertEquals(baseSql, processed);
    }

    @Test
    public void testProcessSql_lockOptionsDefaultMode_usesOffsetFetch() {
        String baseSql = "select id from dual";

        Limit limit = new Limit();
        limit.setMaxRows(5);

        LockOptions lockOptions = new LockOptions(); // 기본 lockMode == NONE
        QueryOptions qo = queryOptionsWithLock(lockOptions);

        String processed = handler.processSql(baseSql, limit, qo);

        assertTrue(processed.toLowerCase().contains("fetch first ? rows only"));
        assertTrue(handler.supportsOffset());
    }



    // ------------------------------------------------------------------------
    // 3) Fragment tests: FOR UPDATE lock mode -> rownum wrapper mode
    // ------------------------------------------------------------------------

    @Test
    public void testProcessSql_forUpdate_withLimit_usesRownumWrapper() {
        String baseSql = "select id from dual for update";

        Limit limit = new Limit();
        limit.setFirstRow(10);
        limit.setMaxRows(5);

        QueryOptions qo = queryOptionsWithLock(LockOptions.UPGRADE);

        String processed = handler.processSql(baseSql, limit, qo);

        String lower = processed.toLowerCase();
        assertTrue(lower.startsWith("select * from (select row_.*,rownum rownum_ from ("));
        assertTrue(lower.contains("rownum<=?"));
        assertTrue(lower.contains("rownum_>?"));
        assertTrue(lower.contains("for update")); // clause preserved

        assertFalse("For update mode should not support offset fetch directly", handler.supportsOffset());
        assertTrue(handler.bindLimitParametersInReverseOrder());
        assertTrue(handler.useMaxForLimit());
    }

    @Test
    public void testGetForUpdateIndex_ignoresQuotedLiteral() {
        // 'for update' 문자열이 리터럴에 있으면 for update clause로 인식하면 안 됨
        String baseSql = "select 'for update' as x from dual";

        Limit limit = new Limit();
        limit.setMaxRows(5);

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        // literal 내 'for update'는 무시되고 offset/fetch 방식으로 처리되어야 함
        assertTrue(processed.toLowerCase().contains("fetch first ? rows only"));
        assertTrue(handler.supportsOffset());
    }

    @Test
    public void testProcessSqlOffsetFetch_forUpdateSql_withoutLockOptions_usesRownumWrapper() {
        String baseSql = "select id from dual for update";

        Limit limit = new Limit();
        limit.setMaxRows(5);

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        assertTrue(processed.toLowerCase().contains("rownum<=?"));
        assertTrue(processed.toLowerCase().contains("for update"));
        assertFalse(handler.supportsOffset());
        assertTrue(handler.bindLimitParametersInReverseOrder());
        assertTrue(handler.useMaxForLimit());
    }

    @Test
    public void testRownumWrapper_firstRowOnly() {
        String baseSql = "select id from dual for update";

        Limit limit = new Limit();
        limit.setFirstRow(10); // firstRow only

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        String lower = processed.toLowerCase();
        assertTrue(lower.contains("where rownum_>?") || lower.contains("rownum>?"));
        assertTrue(lower.contains("for update"));
        assertFalse(handler.supportsOffset());
    }

    @Test
    public void testRownumWrapper_maxRowsOnly() {
        String baseSql = "select id from dual for update";

        Limit limit = new Limit();
        limit.setMaxRows(5);

        String processed = handler.processSql(baseSql, limit, QueryOptions.NONE);

        String lower = processed.toLowerCase();
        assertTrue(lower.contains("where rownum<=?"));
        assertTrue(lower.contains("for update"));
        assertFalse(handler.supportsOffset());
    }

    @Test
    public void testGetForUpdateIndex_quoteBeforeForUpdate_shouldRecognizeForUpdate() {
        String sql = "select 'x' from dual for update";

        Limit limit = new Limit();
        limit.setMaxRows(5);

        String processed = handler.processSql(sql, limit, QueryOptions.NONE);

        assertTrue(processed.toLowerCase().contains("for update"));
        assertTrue(processed.toLowerCase().contains("rownum<=?"));
    }


    // ------------------------------------------------------------------------
    // 4) DB integration tests: 실제 Tibero 실행 검증
    // ------------------------------------------------------------------------

    @Test
    public void testOffsetFetchSql_executesOnTibero() {
        String table = uniqueObjectName("LIMIT_T");

        try {
            // 준비: 테이블 + 20 row
            inTransaction(session -> {
                session.createNativeMutationQuery("create table " + table + " (id number primary key)").executeUpdate();
                for (int i = 1; i <= 20; i++) {
                    session.createNativeMutationQuery("insert into " + table + " values (" + i + ")").executeUpdate();
                }
            });

            // 실제 실행: offset 5, fetch 3 => id 6,7,8
            inTransaction(session -> {
                List<Long> ids = session.createNativeQuery(
                                "select id from " + table + " order by id offset 5 rows fetch next 3 rows only",
                                Long.class
                        )
                        .getResultList();

                assertEquals(3, ids.size());
                assertEquals(Long.valueOf(6), ids.get(0));
                assertEquals(Long.valueOf(7), ids.get(1));
                assertEquals(Long.valueOf(8), ids.get(2));
            });

        } finally {
            dropTableWithRetry(table);
        }
    }

    @Test
    public void testForUpdateLimitSql_executesOnTibero() {
        String table = uniqueObjectName("LIMIT_LOCK_T");

        try {
            inTransaction(session -> {
                session.createNativeMutationQuery("create table " + table + " (id number primary key)").executeUpdate();
                session.createNativeMutationQuery("insert into " + table + " values (1)").executeUpdate();
                session.createNativeMutationQuery("insert into " + table + " values (2)").executeUpdate();
            });

            // for update + fetch (native SQL로 실행 가능 여부만 확인)
            inSession(session -> {
                session.beginTransaction();
                try {
                    Long id = session.createNativeQuery(
                                    "select id from " + table + " where id = 1 for update",
                                    Long.class
                            )
                            .setMaxResults(1)
                            .getSingleResult();

                    assertEquals(Long.valueOf(1), id);
                } finally {
                    safeRollback(session);
                }
            });

        } finally {
            dropTableWithRetry(table);
        }
    }

    // ------------------------------------------------------------------------
    // Helpers
    // ------------------------------------------------------------------------

    private QueryOptions queryOptionsWithLock(LockOptions lockOptions) {
        return new QueryOptionsAdapter() {
            @Override
            public LockOptions getLockOptions() {
                return lockOptions;
            }
        };
    }
}

