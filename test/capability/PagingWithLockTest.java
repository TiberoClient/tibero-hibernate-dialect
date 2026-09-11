package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.util.List;
import java.util.Locale;

import static org.junit.Assert.*;

/**
 * 페이징 + 비관적 락 회귀 테스트.
 *
 * <p>Tibero 는 {@code … fetch first ? rows only for update} 를 JDBC-8004 로 거부한다.
 * 그래서 {@code TiberoSqlAstTranslator} 는 페이징을 서브쿼리로 밀고 {@code for update} 를
 * 바깥에 붙이는 <b>locking wrapper</b> 로 한 문장에 처리한다.
 *
 * <pre>
 * select … from T t where t.id in (select id from T order by … fetch first ? rows only) for update
 * </pre>
 *
 * <p>래퍼를 씌울 수 없는 모양(조인·DISTINCT·GROUP BY 등)은 follow-on locking 으로
 * 폴백한다. follow-on 은 락 문장이 <b>행마다 하나씩</b> 나가므로 1+N 왕복이 되고,
 * 락 없이 먼저 읽은 뒤 잠그기 때문에 그 사이에 갱신 갭이 생긴다. 따라서
 * 단순 페이징 쿼리가 래퍼를 타는지를 <b>SQL 횟수로</b> 고정한다.
 */
public class PagingWithLockTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "PlItem") @Table(name = "PL_ITEM")
    public static class PlItem {
        @Id public Long id;
        public String name;
        @Version public long ver;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{PlItem.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        cfg.setProperty("hibernate.session_factory.statement_inspector",
                SqlCaptureInspector.class.getName());
    }

    @Before
    public void seed() {
        // 세션 팩토리와 스키마는 테스트 클래스 단위로 유지되므로 매번 비우고 다시 채운다.
        inTransaction(session ->
                session.createMutationQuery("delete from PlItem").executeUpdate());
        inTransaction(session -> {
            for (long i = 1; i <= 10; i++) {
                PlItem it = new PlItem();
                it.id = i;
                it.name = "n" + i;
                session.persist(it);
            }
        });
    }

    // ------------------------------------------------------------------
    // 래퍼가 적용되는 경우 — SQL 1회
    // ------------------------------------------------------------------

    @Test
    public void maxResultsWithPessimisticWrite_isOneStatement() {
        List<String> sqls = capture(() -> inTransactionReturning(session ->
                session.createQuery("from PlItem e order by e.id", PlItem.class)
                        .setMaxResults(3)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList()));

        assertEquals("페이징+락은 locking wrapper 로 한 문장에 끝나야 함 — follow-on 이면 1+N 회가 됨: "
                + sqls, 1, sqls.size());

        String sql = sqls.get(0).toLowerCase(Locale.ROOT);
        assertTrue("바깥 쿼리에 for update 가 붙어야 함: " + sql, sql.contains("for update"));
        assertTrue("페이징은 서브쿼리 안으로 들어가야 함: " + sql,
                sql.matches(".*\\bin \\(select\\b.*fetch first.*\\).*"));
        assertTrue("래퍼는 id in (subquery) 형태여야 함: " + sql, sql.contains(" in (select "));
    }

    @Test
    public void firstAndMaxResultsWithPessimisticWrite_isOneStatement() {
        List<String> sqls = capture(() -> inTransactionReturning(session ->
                session.createQuery("from PlItem e order by e.id", PlItem.class)
                        .setFirstResult(2)
                        .setMaxResults(3)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList()));

        assertEquals("offset+fetch+락도 한 문장이어야 함: " + sqls, 1, sqls.size());
        String sql = sqls.get(0).toLowerCase(Locale.ROOT);
        assertTrue("offset 이 서브쿼리 안에 있어야 함: " + sql, sql.contains("offset"));
        assertTrue("바깥에 for update: " + sql, sql.contains("for update"));
    }

    @Test
    public void pagedLockedQuery_returnsCorrectRows() {
        List<PlItem> rows = inTransactionReturning(session ->
                session.createQuery("from PlItem e order by e.id", PlItem.class)
                        .setFirstResult(2)
                        .setMaxResults(3)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList());

        assertEquals("건수", 3, rows.size());
        assertEquals("offset 2 → id 3 부터", Long.valueOf(3L), rows.get(0).id);
        assertEquals(Long.valueOf(5L), rows.get(2).id);
    }

    // ------------------------------------------------------------------
    // 래퍼가 필요 없는 경우 — 평문 for update
    // ------------------------------------------------------------------

    @Test
    public void lockWithoutPaging_staysPlainForUpdate() {
        List<String> sqls = capture(() -> inTransactionReturning(session ->
                session.createQuery("from PlItem e order by e.id", PlItem.class)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList()));

        assertEquals("페이징이 없으면 래퍼가 필요 없음: " + sqls, 1, sqls.size());
        String sql = sqls.get(0).toLowerCase(Locale.ROOT);
        assertTrue("for update 는 붙어야 함: " + sql, sql.contains("for update"));
        assertFalse("불필요한 id in (subquery) 래핑이 붙으면 안 됨: " + sql, sql.contains(" in (select "));
    }

    @Test
    public void pagingWithoutLock_staysNativeOffsetFetch() {
        List<String> sqls = capture(() -> inTransactionReturning(session ->
                session.createQuery("from PlItem e order by e.id", PlItem.class)
                        .setMaxResults(3)
                        .getResultList()));

        assertEquals(1, sqls.size());
        String sql = sqls.get(0).toLowerCase(Locale.ROOT);
        assertTrue("락이 없으면 네이티브 페이징 그대로: " + sql, sql.contains("fetch first"));
        assertFalse("락이 없는데 래핑되면 안 됨: " + sql, sql.contains(" in (select "));
        assertFalse(sql.contains("for update"));
    }

    // ------------------------------------------------------------------
    // 래퍼를 못 씌우는 모양 — follow-on 으로 폴백해도 문법 오류는 없어야 함
    // ------------------------------------------------------------------

    @Test
    public void distinctPagedLock_fallsBackWithoutSyntaxError() {
        // DISTINCT 는 래퍼 대상에서 제외된다 → follow-on locking 으로 폴백.
        // 핵심은 "JDBC-8004 로 깨지지 않는 것" 이다.
        List<String> sqls = capture(() -> inTransactionReturning(session ->
                session.createQuery("select distinct e from PlItem e order by e.id", PlItem.class)
                        .setMaxResults(3)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList()));

        assertFalse("SQL 이 하나도 안 잡혔음", sqls.isEmpty());
        for (String s : sqls) {
            String sql = s.toLowerCase(Locale.ROOT);
            assertFalse("offset/fetch 와 for update 가 같은 블록에 있으면 JDBC-8004: " + sql,
                    sql.contains("fetch first") && sql.contains("for update")
                            && !sql.contains(" in (select "));
        }
    }

    @Test
    public void multiRootPagedLock_fallsBackToFollowOn() {
        // 루트가 둘이면 래퍼를 씌울 수 없다(id 서브쿼리가 원본과 대응되지 않음).
        // 기본 구현에는 이 규칙이 없으므로 determineLockingStrategy 의 폴백이 실제로 동작해야 한다.
        List<String> sqls = capture(() -> inTransactionReturning(session ->
                session.createQuery("select a from PlItem a, PlItem b where a.id = b.id order by a.id", PlItem.class)
                        .setMaxResults(3)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList()));

        assertTrue("래퍼를 못 씌우면 follow-on 이라 1+N 회가 나와야 함: " + sqls, sqls.size() > 1);

        String paged = sqls.get(0).toLowerCase(Locale.ROOT);
        assertTrue("첫 문장은 페이징 조회: " + paged, paged.contains("fetch first"));
        assertFalse("페이징 문장에 for update 가 붙으면 JDBC-8004: " + paged, paged.contains("for update"));

        for (String s : sqls.subList(1, sqls.size())) {
            assertTrue("나머지는 행별 락 문장이어야 함: " + s,
                    s.toLowerCase(Locale.ROOT).contains("for update"));
        }
    }

    // ------------------------------------------------------------------

    private List<String> capture(Runnable body) {
        SqlCaptureInspector.clear();
        body.run();
        return SqlCaptureInspector.getSqls();
    }
}
