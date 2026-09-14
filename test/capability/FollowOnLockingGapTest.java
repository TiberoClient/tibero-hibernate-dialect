package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.LockModeType;
import jakarta.persistence.FetchType;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.RowLockStrategy;
import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * 페이징 + 비관적 락에서 <b>locking wrapper 가 적용되지 않는 모양</b>을 고정한다.
 *
 * <h2>무슨 이야기인가</h2>
 * "10건만 읽되 그 10건을 잠가라"({@code setMaxResults(10)} + {@code PESSIMISTIC_WRITE})를
 * 처리하는 방법이 두 가지다.
 *
 * <pre>
 * ① locking wrapper — 한 문장
 *    select … from T t where t.id in (select id from T order by … offset ? rows fetch first ? rows only)
 *    for update
 *
 * ② follow-on locking — 1 + N 문장
 *    select … from T … offset ? rows fetch first ? rows only     ← 잠그지 않고 먼저 읽고
 *    select id from T where id=? for update                       ← 한 건씩 나중에 잠근다  × N
 * </pre>
 *
 * <p>①이 낫다. 문장이 하나고, 무엇보다 <b>읽는 시점과 잠그는 시점이 같다.</b> ②는 그 사이에
 * 다른 트랜잭션이 끼어들 수 있어 <b>갱신 갭</b>이 생긴다 — {@code @Version} 이 없으면
 * 조용히 낡은 상태로 덮어쓸 수 있다.
 *
 * <h2>그런데 ①은 아무 쿼리에나 못 쓴다</h2>
 * 이 테스트가 고정하는 것이 바로 그 경계다. 조인·다중 루트·DISTINCT·UNION 은 ②로 빠진다.
 * <b>Oracle 도 똑같이 제한한다</b> — {@code OracleSqlAstTranslator.canApplyLockingWrapper} 의
 * 주석이 "We only need a locking wrapper for very simple queries" 다.
 *
 * <h2>왜 넓히지 않았나 — ps06 실측</h2>
 * 넓힐 수 있는지 직접 재봤고, <b>넓히면 안 된다</b>는 결론이 나왔다.
 *
 * <pre>
 * DISTINCT + for update      JDBC-8029 FOR UPDATE clause is not allowed for this query  → DB가 거부
 * UNION    + for update      JDBC-8028 FOR UPDATE clause is not allowed here            → DB가 거부
 * 다중 루트  + 래퍼             JDBC-8026 Invalid identifier
 *                            래퍼가 첫 루트만 서브쿼리로 옮겨 두 번째 루트의 별칭이 사라진다
 * 조인      + 래퍼             SQL 은 동작한다. 그러나 <b>잠금 범위가 넓어진다</b>
 * </pre>
 *
 * <p>마지막 항목이 핵심이다. 조인에 래퍼를 씌우면 {@code for update} 가 <b>조인 상대
 * 테이블까지 잠근다</b>(ps06 실측). 지금의 follow-on 은 루트 엔티티만 잠근다. 문장 수는
 * 5 → 3 으로 줄지만 <b>경합 범위가 넓어지고 교착 가능성이 새로 생긴다</b> — 성능을 얻으려다
 * 정확성 위험을 들이는 교환이라 하지 않았다.
 *
 * <pre>
 * 조인 래퍼 + for update            → FO_A 잠김, FO_B <b>도 잠김</b>
 * 조인 래퍼 + for update of a.id    → FO_A 잠김, FO_B 열림
 * 현재 follow-on                    → FO_A 잠김, FO_B 열림   ← 이것과 같아야 한다
 * </pre>
 *
 * <p>{@code for update of} 로 범위를 좁히면 가능하지만, 그러려면 Hibernate 의
 * {@code ForUpdateClause} 렌더링에 개입해야 하고 이득이 좁아 <b>현 상태를 유지</b>한다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator#visitQuerySpec
 */
public class FollowOnLockingGapTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "FlgDept") @Table(name = "FLG_DEPT")
    public static class Dept {
        @Id public Long id;
        public String name;
    }

    @Entity(name = "FlgEmp") @Table(name = "FLG_EMP")
    public static class Emp {
        @Id public Long id;
        public String name;
        // 지연 로딩 — 즉시 로딩이면 부수 select 가 섞여 문장 수 단언이 흐려진다
        @ManyToOne(fetch = FetchType.LAZY) public Dept dept;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Emp.class, Dept.class};
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
        inTransaction(s -> {
            s.createMutationQuery("delete from FlgEmp").executeUpdate();
            s.createMutationQuery("delete from FlgDept").executeUpdate();
        });
        inTransaction(s -> {
            for (long i = 1; i <= 5; i++) {
                Dept d = new Dept();
                d.id = i;
                d.name = "d" + i;
                s.persist(d);
                Emp e = new Emp();
                e.id = i;
                e.name = "e" + i;
                e.dept = d;
                s.persist(e);
            }
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // 래퍼가 적용되는 유일한 모양
    // ------------------------------------------------------------------

    /** 단순 페이징 + 락 — 래퍼가 붙고 문장이 하나다. */
    @Test
    public void simplePaging_usesLockingWrapper_inOneStatement() {
        final List<String> sqls = lockedPage("select e from FlgEmp e order by e.id");

        assertEquals("래퍼가 붙으면 락 문장은 하나여야 함: " + sqls, 1, sqls.size());
        final String sql = sqls.get(0);
        assertTrue("서브쿼리로 페이징을 밀어야 함: " + sql, sql.contains("in (select"));
        assertTrue("바깥에 for update 가 붙어야 함: " + sql, sql.endsWith("for update"));
        assertFalse("바깥에는 페이징이 남으면 안 됨: " + sql, sql.contains("fetch first ? rows only for update"));
    }

    // ------------------------------------------------------------------
    // 래퍼가 안 붙는 네 가지 — follow-on 폴백
    // ------------------------------------------------------------------

    /**
     * 조인 — 폴백한다. <b>SQL 은 가능하지만 잠금 범위가 넓어져서</b> 하지 않는다.
     *
     * @see #joinFallback_locksOnlyTheRootTable 그 판단의 근거
     */
    @Test
    public void join_fallsBackToFollowOn() {
        assertFollowOn("select e from FlgEmp e join e.dept d where d.name is not null order by e.id");
    }

    /** 다중 루트 — 폴백한다. 래퍼가 첫 루트만 옮겨 `JDBC-8026` 이 난다. */
    @Test
    public void multipleRoots_fallBackToFollowOn() {
        assertFollowOn("select e from FlgEmp e, FlgDept d where e.id = d.id order by e.id");
    }

    /** DISTINCT — 폴백한다. Tibero 가 `for update` 자체를 거부한다(`JDBC-8029`). */
    @Test
    public void distinct_fallsBackToFollowOn() {
        assertFollowOn("select distinct e from FlgEmp e order by e.id");
    }

    /**
     * 집계/GROUP BY — 폴백한다.
     *
     * <p>Hibernate 기본 구현이 먼저 강제하는 경로라 우리 조건이 없어도 폴백하지만,
     * 조건을 지우면 드러나므로 함께 고정한다.
     */
    @Test
    public void groupBy_fallsBackToFollowOn() {
        SqlCaptureInspector.clear();
        inTransaction(s -> s.createQuery(
                "select e.dept.id from FlgEmp e group by e.dept.id order by e.dept.id", Long.class)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .setMaxResults(2).getResultList());
        final List<String> sqls = captured();
        for (String sql : sqls) {
            assertFalse("GROUP BY 에는 래퍼가 붙으면 안 됨: " + sql,
                    sql.contains("in (select") && sql.endsWith("for update"));
        }
    }

    // ------------------------------------------------------------------
    // 폴백이 남기는 것 — 이게 이 항목의 본론
    // ------------------------------------------------------------------

    /**
     * 폴백 경로는 <b>1 + N 문장</b>이다.
     *
     * <p>문장 수가 늘어나는 것 자체보다, 첫 문장이 <b>잠그지 않고</b> 읽는다는 점이 중요하다.
     * 그 사이에 다른 트랜잭션이 같은 행을 고치면, {@code @Version} 이 없는 엔티티는
     * 낡은 상태를 덮어쓴다.
     */
    @Test
    public void followOnPath_readsUnlockedFirst_thenLocksOneByOne() {
        final List<String> sqls = lockedPage(
                "select e from FlgEmp e join e.dept d where d.name is not null order by e.id");

        assertTrue("1+N 이어야 함. 실제 " + sqls.size() + "개: " + sqls, sqls.size() >= 2);

        final String first = sqls.get(0);
        assertFalse("첫 문장은 잠그지 않는다 — 이게 갱신 갭의 원인: " + first,
                first.contains("for update"));
        assertTrue("첫 문장이 페이징을 한다: " + first, first.contains("fetch first"));

        for (String sql : sqls.subList(1, sqls.size())) {
            assertTrue("뒤따르는 문장은 한 건씩 잠근다: " + sql, sql.contains("for update"));
            assertTrue("루트 테이블만 잠근다: " + sql, sql.contains("FLG_EMP"));
            assertFalse("조인 상대 테이블은 잠그지 않는다: " + sql, sql.contains("FLG_DEPT"));
        }
    }

    /**
     * ⚠️ <b>래퍼를 조인으로 넓히지 않은 이유</b> — 폴백은 루트 테이블만 잠근다.
     *
     * <p>조인 쿼리에 래퍼를 씌우면 {@code for update} 가 조인 상대까지 잠근다(실측).
     * 이 테스트는 현재 동작이 <b>그렇지 않다</b>는 것을 고정한다. 누군가
     * {@code canApplyLockingWrapper} 에서 {@code !fromClause.hasJoins()} 를 지우면
     * 여기서 걸린다.
     */
    @Test
    public void joinFallback_locksOnlyTheRootTable() throws Exception {
        // 조인 + 페이징 + 락 을 잡은 채로
        inTransaction(holder -> {
            holder.createQuery("select e from FlgEmp e join e.dept d where d.name is not null order by e.id",
                            Emp.class)
                    .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                    .setMaxResults(2).getResultList();

            // 다른 연결에서 같은 행을 건드려 본다
            assertTrue("루트 테이블 행은 잠겨 있어야 함", isLocked("FLG_EMP", 1L));
            assertFalse("조인 상대 테이블 행은 잠기면 안 됨 — 래퍼를 넓히면 여기가 깨진다",
                    isLocked("FLG_DEPT", 1L));
        });
    }

    /**
     * {@code getWriteRowLockStrategy()} 가 {@code COLUMN} 이라는 선언이 실제와 맞는지.
     *
     * <p>§6.5 가 "실제 잠금 범위 테스트 없음"으로 남겨둔 항목이다. 선언값만 보던 것을
     * DB 동작으로 확인한다 — {@code for update of <컬럼>} 문법을 Tibero 가 받고,
     * 그 범위가 실제로 좁혀지는지.
     */
    @Test
    public void writeRowLockStrategy_columnScoping_actuallyNarrowsTheLock() throws Exception {
        assertEquals(RowLockStrategy.COLUMN, new TiberoDialect().getWriteRowLockStrategy());

        inTransaction(holder -> holder.doWork(conn -> {
            try (Statement st = conn.createStatement()) {
                // 조인 결과에 for update of 로 한쪽만 지정
                st.executeQuery("select e.id from FLG_EMP e join FLG_DEPT d on d.id=e.dept_id "
                        + "where e.id=1 for update of e.id");
            }
            assertTrue("of 로 지정한 테이블은 잠긴다", isLocked("FLG_EMP", 1L));
            assertFalse("지정하지 않은 테이블은 열려 있다", isLocked("FLG_DEPT", 1L));
        }));
    }

    // ------------------------------------------------------------------

    /** 락을 건 페이징 조회를 돌리고, 나간 select 문을 돌려준다. */
    private List<String> lockedPage(String hql) {
        SqlCaptureInspector.clear();
        inTransaction(s -> s.createQuery(hql, Emp.class)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .setMaxResults(2).getResultList());
        return captured();
    }

    /** 래퍼가 아니라 follow-on 으로 빠졌는지. */
    private void assertFollowOn(String hql) {
        SqlCaptureInspector.clear();
        try {
            inTransaction(s -> s.createQuery(hql, Object.class)
                    .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                    .setMaxResults(2).getResultList());
        }
        catch (Exception e) {
            fail("폴백이 동작해야 하므로 예외가 나면 안 됨: " + rootMessage(e));
        }
        for (String sql : captured()) {
            assertFalse("래퍼가 붙으면 안 되는 모양인데 붙었다: " + sql,
                    sql.contains("in (select") && sql.trim().endsWith("for update"));
        }
    }

    /** 엔티티 로딩 등 부수 select 를 걸러내고 대상 테이블을 건드리는 문장만 남긴다. */
    private List<String> captured() {
        final List<String> out = new ArrayList<>();
        for (String sql : SqlCaptureInspector.getSqls()) {
            final String lower = sql.toLowerCase();
            if (lower.startsWith("select") && (lower.contains("flg_emp") || lower.contains("flg_dept"))) {
                out.add(sql);
            }
        }
        SqlCaptureInspector.clear();
        return out;
    }

    /**
     * 다른 연결에서 {@code for update nowait} 를 시도해 잠겨 있는지 본다.
     *
     * <p>{@code nowait} 라 잠겨 있으면 기다리지 않고 바로 오류가 난다 — 테스트가 멈추지 않는다.
     */
    private boolean isLocked(String table, long id) {
        try (Connection probe = newConnection()) {
            probe.setAutoCommit(false);
            try (Statement st = probe.createStatement()) {
                st.setQueryTimeout(5);
                st.executeQuery("select * from " + table + " where id=" + id + " for update nowait");
                probe.rollback();
                return false;
            }
            catch (SQLException locked) {
                probe.rollback();
                return true;
            }
        }
        catch (SQLException e) {
            throw new IllegalStateException("잠금 확인용 연결 실패", e);
        }
    }

    private Connection newConnection() throws SQLException {
        return java.sql.DriverManager.getConnection(
                System.getProperty("hibernate.connection.url"),
                System.getProperty("hibernate.connection.username"),
                System.getProperty("hibernate.connection.password"));
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return String.valueOf(t.getMessage());
    }
}
