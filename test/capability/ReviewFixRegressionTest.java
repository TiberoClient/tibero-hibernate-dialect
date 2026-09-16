package capability;

import jakarta.persistence.*;
import jakarta.persistence.LockModeType;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * 전체 리뷰 §4 의 코드 결함에 대한 회귀 테스트.
 *
 * <p>다섯 건 모두 기존 테스트가 보지 않던 자리에 있었다. 같은 모양이 다시 들어오면
 * 실패하도록 고정한다. §4.5 는 {@code LimitHandlerTest}, §4.6 은
 * {@code DialectDecisionContractTest} 가 맡는다.
 */
public class ReviewFixRegressionTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "RfxRow") @Table(name = "RFX_ROW")
    public static class Row {
        @Id public Long id;
        @Column(name = "D")     public LocalDate d;
        @Column(name = "TS")    public LocalDateTime ts;
        @Column(name = "N")     public Integer n;
        @Column(name = "NAME")  public String name;
    }

    private static final String RM = "RFX_NUM";

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Row.class};
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
        inTransaction(s -> s.createMutationQuery("delete from RfxRow").executeUpdate());
        inTransaction(s -> {
            for (int i = 1; i <= 20; i++) {
                Row r = new Row();
                r.id = (long) i;
                r.d = LocalDate.of(2024, 3, 5);
                r.ts = LocalDateTime.of(2024, 3, 5, 12, 0, 30);
                r.n = i;
                r.name = String.format("n%02d", 100 - i);   // id 순서와 반대
                s.persist(r);
            }
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // §4.1  extract(epoch from <DATE>)
    // ------------------------------------------------------------------

    /**
     * EPOCH 패턴의 {@code at time zone} 은 타임존 정보를 가진 값에만 쓸 수 있다.
     * DATE 컬럼에 쓰면 {@code JDBC-11003} 이 난다. 인자가 DATE 일 때만
     * {@code from_tz} 식으로 갈아끼우는 함수가 등록돼 있어야 한다.
     */
    @Test
    public void extractEpoch_onDateAttribute_works() {
        Object v = inTransactionReturning(s -> s.createQuery(
                "select extract(epoch from e.d) from RfxRow e where e.id = 1", Object.class)
                .getSingleResult());
        assertNotNull(v);
        assertEquals("2024-03-05 00:00:00 UTC", 1709596800L, ((Number) v).longValue());
    }

    /** TIMESTAMP 경로는 기존 패턴을 그대로 써야 한다 — 함수 등록이 나머지를 덮으면 안 된다. */
    @Test
    public void extractEpoch_onTimestampAttribute_stillUsesAtTimeZone() {
        SqlCaptureInspector.clear();
        Object v = inTransactionReturning(s -> s.createQuery(
                "select extract(epoch from e.ts) from RfxRow e where e.id = 1", Object.class)
                .getSingleResult());
        assertNotNull(v);
        assertTrue("TIMESTAMP 은 at time zone 패턴이어야 함: " + lastSelect(),
                lastSelect().contains("at time zone"));
    }

    /** EPOCH 외 다른 단위는 dialect 패턴으로 위임돼야 한다. */
    @Test
    public void extractOtherUnits_stillUseDialectPattern() {
        assertEquals(2024, ((Number) inTransactionReturning(s -> s.createQuery(
                "select extract(year from e.d) from RfxRow e where e.id = 1", Object.class)
                .getSingleResult())).intValue());
        assertEquals(3, ((Number) inTransactionReturning(s -> s.createQuery(
                "select extract(month from e.d) from RfxRow e where e.id = 1", Object.class)
                .getSingleResult())).intValue());
    }

    // ------------------------------------------------------------------
    // §4.2  FROM 절 파생 테이블
    // ------------------------------------------------------------------

    /**
     * Hibernate 기본 구현은 파생 테이블의 컬럼 이름을 {@code t(x)} 로 괄호에 붙이는데
     * Tibero 가 {@code JDBC-8022} 로 거부한다. 별칭이 서브쿼리 안쪽으로 들어가야 한다.
     */
    @Test
    public void derivedTable_inFromClause_works() {
        SqlCaptureInspector.clear();
        Object v = inTransactionReturning(s -> s.createQuery(
                "select t.x from (select e.n as x from RfxRow e where e.id = 7) t", Object.class)
                .getSingleResult());
        assertEquals(7, ((Number) v).intValue());

        final String sql = lastSelect();
        assertFalse("컬럼 목록을 괄호로 붙이면 JDBC-8022: " + sql,
                sql.matches("(?s).*\\)\\s+\\w+\\s*\\([^)]*\\).*"));
    }

    // ------------------------------------------------------------------
    // §4.3  number(p,0) 역매핑 — 값을 넣고 읽어야 잡힌다
    // ------------------------------------------------------------------

    /**
     * 타입 해석만 보고 <b>값을 왕복시키지 않아서</b> 이 결함이 통과했다. 그래서 여기서는
     * dialect 가 만들지 않은 스키마에 정상 범위 값을 넣고 네이티브 쿼리로 읽는다.
     */
    @Test
    public void numberReverseMapping_roundTripsRealValues() {
        dropTableWithRetry(RM);
        inTransaction(s -> s.doWork(c -> {
            try (Statement st = c.createStatement()) {
                st.execute("create table " + RM + " (b1 number(1,0), b3 number(3,0), "
                        + "b5 number(5,0), b10 number(10,0), b19 number(19,0))");
                st.execute("insert into " + RM + " values (7, 200, 50000, 123, 456)");
            }
        }));
        try {
            assertEquals("number(1,0) 의 7 이 true 가 되면 안 된다",
                    7L, num(RM, "b1"));
            assertEquals("number(3,0) 은 -999~999 — TINYINT 로 좁히면 JDBC-590749",
                    200L, num(RM, "b3"));
            assertEquals("number(5,0) 은 -99999~99999 — SMALLINT 로 좁히면 JDBC-590749",
                    50000L, num(RM, "b5"));
            assertEquals(123L, num(RM, "b10"));
            assertEquals(456L, num(RM, "b19"));
        }
        finally {
            dropTableWithRetry(RM);
        }
    }

    /**
     * ⚠️ {@code precision != 0} 가드가 조건 <b>바깥</b>에 있어야 한다.
     *
     * <p>tbjdbc 는 맨 집계의 precision 을 0 으로 보고한다({@code avg}/{@code sum}/{@code count}).
     * 가드를 안쪽에 두면 0 이 {@code <= 19} 에 걸려 BIGINT 가 되고 소수가 깎인다
     * ({@code avg(v)} 가 3.5 → 3).
     *
     * <p>⚠️ {@code avg(7)/2} 로는 검증할 수 없다 — 나눗셈 결과는 precision 을 38 로 보고해
     * 이 분기를 타지 않으므로 가드를 잘못 옮겨도 통과한다. <b>맨 집계이면서 결과가 소수</b>
     * 여야 드러난다.
     */
    @Test
    public void aggregatePrecisionZero_keepsDecimalPrecision() {
        // n = 1..20 → 평균 10.5 (소수)
        final Object avg = inTransactionReturning(s ->
                s.createNativeQuery("select avg(N) from RFX_ROW", Object.class).getSingleResult());
        assertNotNull(avg);
        assertFalse("맨 집계가 정수 타입으로 해석되면 소수가 깎인다 — precision!=0 가드가 "
                        + "조건 안쪽에 있는지 확인할 것. 실제 타입: " + avg.getClass().getSimpleName(),
                avg instanceof Long || avg instanceof Integer);
        assertEquals("avg(1..20) = 10.5 가 그대로 나와야 한다", 0,
                new java.math.BigDecimal("10.5").compareTo(new java.math.BigDecimal(avg.toString())));
    }

    // ------------------------------------------------------------------
    // §4.4  페이징 + 비관적 락 — 바깥 order by
    // ------------------------------------------------------------------

    /**
     * locking wrapper 는 페이징을 서브쿼리로 밀고 {@code for update} 를 바깥에 둔다.
     * 서브쿼리의 정렬은 "어떤 행을 고를지", 바깥의 정렬은 "어떤 순서로 돌려줄지" 라
     * 역할이 다르다. 바깥이 없으면 순서가 실행계획에 좌우된다.
     *
     * <p>결과 순서만 단언하면 우연히 통과할 수 있으므로 <b>SQL 모양을 함께</b> 고정한다.
     */
    @Test
    public void pagedLockedQuery_keepsOuterOrderBy() {
        SqlCaptureInspector.clear();
        List<Row> rows = inTransactionReturning(s -> s.createQuery(
                "from RfxRow e order by e.name", Row.class)
                .setMaxResults(3)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getResultList());

        assertEquals(3, rows.size());
        assertEquals(List.of("n80", "n81", "n82"),
                rows.stream().map(r -> r.name).collect(Collectors.toList()));

        final String sql = SqlCaptureInspector.getSqls().stream()
                .filter(q -> q.toLowerCase(Locale.ROOT).contains("for update"))
                .reduce((a, b) -> b).orElse("");
        assertTrue("locking wrapper 가 적용돼야 함: " + sql, sql.contains("in (select"));

        // 바깥(서브쿼리 괄호 밖)에 order by 가 있어야 한다
        final String low = sql.toLowerCase(Locale.ROOT);
        final int open = low.indexOf('(');
        final int close = low.lastIndexOf(')');
        final String outer = (open >= 0 && close > open)
                ? low.substring(0, open) + low.substring(close + 1) : low;
        assertTrue("바깥 order by 가 없으면 순서가 실행계획에 좌우된다: " + sql,
                outer.contains("order by"));
    }

    /**
     * <b>정렬 없는</b> 페이징 + 비관적 락도 동작해야 한다.
     *
     * <p>locking wrapper 는 원본 질의의 정렬 사양을 서브쿼리와 바깥 래퍼 양쪽에 넣는다.
     * {@code QuerySpec.getSortSpecifications()} 는 정렬이 없으면 <b>null</b> 을 돌려주므로
     * 두 자리 모두 {@code hasSortSpecifications()} 가드가 필요하다. 한쪽이라도 빠지면
     * {@code order by} 없는 페이징+락이 {@code NullPointerException} 으로 죽는다.
     *
     * <p>락을 빼면 이 경로를 타지 않으므로 <b>비관적 락이 함께 걸려야</b> 재현된다.
     * 세 가지 페이징 형태를 모두 본다 — 셋 다 같은 자리를 지난다.
     */
    @Test
    public void pagedLockedQuery_withoutOrderBy_doesNotFail() {
        assertEquals(3, inTransactionReturning(s -> s.createQuery("from RfxRow e", Row.class)
                .setMaxResults(3)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getResultList()).size());

        assertFalse("setFirstResult 단독도 같은 자리를 지난다",
                inTransactionReturning(s -> s.createQuery("from RfxRow e", Row.class)
                        .setFirstResult(1)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList()).isEmpty());

        assertEquals(2, inTransactionReturning(s -> s.createQuery("from RfxRow e", Row.class)
                .setFirstResult(1)
                .setMaxResults(2)
                .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                .getResultList()).size());
    }

    // ------------------------------------------------------------------

    private long num(String table, String col) {
        Object v = inTransactionReturning(s ->
                s.createNativeQuery("select " + col + " from " + table, Object.class).getSingleResult());
        assertNotNull(col + " 이 null", v);
        assertFalse(col + " 이 Boolean 으로 해석됨 — 값이 조용히 바뀐다", v instanceof Boolean);
        return ((Number) v).longValue();
    }

    private String lastSelect() {
        return SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase(Locale.ROOT).startsWith("select"))
                .reduce((a, b) -> b).orElse("");
    }
}
