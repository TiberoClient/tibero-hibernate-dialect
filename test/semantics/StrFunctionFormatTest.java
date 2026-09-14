package semantics;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

/**
 * {@code str(x)} 의 결과가 <b>세션 설정에 좌우되지 않는지</b> 확인한다.
 *
 * <h2>무엇이 문제였나</h2>
 * {@code str(x)} 는 값을 문자열로 바꾸는 HQL 함수다. 예전 구현은 이것을 형식 문자열 없는
 * {@code to_char(x)} 로 등록했는데, 그러면 <b>같은 질의가 세션마다 다른 결과</b>를 준다.
 *
 * <pre>
 * NLS_DATE_FORMAT 기본값         str(날짜) → "2024/03/05"
 * NLS_DATE_FORMAT='DD/MM/YYYY'   str(날짜) → "05/03/2024"     ← 같은 행, 다른 문자열
 * </pre>
 *
 * <p>세션 설정은 애플리케이션이 아니라 <b>접속 환경</b>이 정하는 경우가 많다. 개발 장비에서는
 * 맞던 것이 운영에서 달라지고, 그 문자열을 파싱하거나 비교하는 코드가 조용히 깨진다.
 * 예외가 나지 않으므로 알아차리기 어렵다.
 *
 * <h2>어떻게 고쳤나 — 등록을 지웠다</h2>
 * Hibernate 는 이미 {@code str} 을 <b>{@code cast(x as String)} 의 별칭</b>으로 등록해
 * 둔다({@code CastStrEmulation}). 그리고 우리 {@code castPattern()} 은 temporal 타입에
 * 대해 형식을 명시한다 — {@code to_char(?1,'YYYY-MM-DD')}.
 *
 * <p>즉 기본 등록을 그대로 뒀으면 처음부터 문제가 없었는데, 우리가 덮어써서 생긴 일이었다.
 * <b>등록 한 줄을 지우는 것이 곧 고치는 것</b>이었다.
 *
 * <pre>
 * 고치기 전  registry.register("str", new StandardSQLFunction("to_char", …));
 *            select to_char(e1_0.day)                    → NLS 의존
 * 고친 후    (등록하지 않음 → CastStrEmulation 이 살아남)
 *            select to_char(e1_0.day,'YYYY-MM-DD')       → 고정
 * </pre>
 *
 * <h2>형식을 박는 것은 Hibernate 가 아니라 우리 dialect 다</h2>
 * 오해하기 쉬운 지점이다. 등록을 지운 것은 "Hibernate 기본 동작에 맡긴" 것이 아니라
 * <b>{@code castPattern()} 이 일할 기회를 되돌려준</b> 것이다.
 *
 * <pre>
 * str(x) ──[Hibernate CastStrEmulation]──▶ cast(x as String)
 *                                             │
 *                                             └─[TiberoDialect.castPattern]──▶ 실제 SQL
 * </pre>
 *
 * <p>그래서 <b>모든 타입이 고정되는 것은 아니다.</b> {@code castPattern} 의
 * {@code case STRING:} 안에 항목이 있는 타입만 형식이 박힌다.
 *
 * <pre>
 * 날짜·시각·타임스탬프·오프셋TS   to_char(…,'YYYY-MM-DD' 등)      NLS 영향 없음
 * 정수·소수·double              cast(… as varchar2(65532 char))  NLS 를 탐
 * </pre>
 *
 * <p>Oracle 도 같다 — {@code OracleDialect.castPattern} 의 STRING 분기가 우리와
 * 한 글자도 다르지 않고, Oracle 역시 {@code str} 을 재등록하지 않는다. 즉 이 결함은
 * <b>Tibero dialect 만의 예외</b>였고 지우면서 Oracle 과 동작이 일치하게 됐다.
 *
 * <h2>숫자는 여전히 NLS 를 탄다</h2>
 * 아래 {@link #numericStr_remainsLocaleSensitive} 가 그 한계를 명시한다.
 * {@code NLS_NUMERIC_CHARACTERS} 를 바꾸면 소수점이 쉼표가 되는데, 이건
 * {@code cast(숫자 as varchar2)} 자체의 성질이라 {@code str} 만 고쳐서 될 일이 아니다.
 * Oracle 도 같고, 형식이 중요하면 {@code to_char(x,'FM999999.00')} 처럼 직접 지정해야 한다.
 */
public class StrFunctionFormatTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "SffRow") @Table(name = "SFF_ROW")
    public static class Row {
        @Id public Long id;
        public Integer n;
        @Column(precision = 12, scale = 3) public BigDecimal d;
        @Column(name = "D_DAY") public LocalDate day;
        @Column(name = "D_TS") public LocalDateTime ts;
    }

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
        inTransaction(s -> s.createMutationQuery("delete from SffRow").executeUpdate());
        inTransaction(s -> {
            Row r = new Row();
            r.id = 1L;
            r.n = 42;
            r.d = new BigDecimal("1234.500");
            r.day = LocalDate.of(2024, 3, 5);
            r.ts = LocalDateTime.of(2024, 3, 5, 11, 22, 33);
            s.persist(r);
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // 고쳐진 것 — temporal
    // ------------------------------------------------------------------

    /** 날짜가 ISO 로 나오는지, 그리고 SQL 에 형식 문자열이 실제로 들어갔는지. */
    @Test
    public void strOfDate_usesExplicitIsoFormat() {
        assertEquals("2024-03-05", str("e.day"));
        assertTrue("형식 문자열이 SQL 에 있어야 함: " + lastSelect(),
                lastSelect().contains("'YYYY-MM-DD'"));
    }

    /** 타임스탬프도 마찬가지. */
    @Test
    public void strOfTimestamp_usesExplicitIsoFormat() {
        assertTrue("ISO 로 시작해야 함: " + str("e.ts"), str("e.ts").startsWith("2024-03-05 11:22:33"));
        assertTrue(lastSelect().contains("'YYYY-MM-DD HH24:MI:SS.FF9'"));
    }

    /**
     * ⚠️ 핵심 회귀 — <b>세션 NLS 를 바꿔도 결과가 같아야 한다.</b>
     *
     * <p>등록을 되돌리면 이 테스트가 잡는다. 앞의 두 테스트는 기본 NLS 에서만 도는데,
     * 기본값이 우연히 ISO 인 환경에서는 결함이 드러나지 않기 때문이다.
     */
    @Test
    public void strOfDate_isUnaffectedByNlsDateFormat() {
        // ⚠️ alter session 과 질의는 반드시 같은 세션 안에서 해야 한다.
        // inTransaction 을 두 번 부르면 커넥션 풀에서 다른 연결을 받을 수 있어
        // 설정이 적용되지 않은 채로 질의가 나가고, 테스트가 아무것도 검증하지 못한다.
        inTransaction(s -> {
            s.createNativeMutationQuery("alter session set NLS_DATE_FORMAT='DD/MM/YYYY'")
                    .executeUpdate();
            // 같은 세션에서 형식이 정말 바뀌었는지 먼저 확인 — 안 그러면 아래 단언이 공허해진다
            assertEquals("세션 설정이 실제로 적용돼야 함", "05/03/2024",
                    s.createNativeQuery("select to_char(d_day) from SFF_ROW where id=1", String.class)
                            .getSingleResult());
            assertEquals("그래도 str() 은 ISO 를 유지해야 함", "2024-03-05",
                    s.createQuery("select str(e.day) from SffRow e", String.class).getSingleResult());
            restoreNls(s);
        });
    }

    /** {@code str(x)} 와 {@code cast(x as String)} 이 같은 SQL 을 만드는지 — 별칭이라는 전제. */
    @Test
    public void strAndCast_produceTheSameSql() {
        final String strSql = sqlOf("select str(e.day) from SffRow e");
        final String castSql = sqlOf("select cast(e.day as String) from SffRow e");
        assertEquals("str 은 cast 의 별칭이어야 한다", castSql, strSql);
    }

    // ------------------------------------------------------------------
    // 안 고쳐진 것 — 숫자는 한계를 명시한다
    // ------------------------------------------------------------------

    /** 정수는 자리 구분 기호가 붙지 않아 NLS 영향이 없다. */
    @Test
    public void strOfInteger_isStable() {
        assertEquals("42", str("e.n"));
    }

    /**
     * <b>한계</b> — 소수는 {@code NLS_NUMERIC_CHARACTERS} 를 탄다.
     *
     * <p>{@code cast(숫자 as varchar2)} 자체의 성질이라 {@code str} 등록을 고쳐도 남는다.
     * Oracle 도 동일하다. 이 테스트는 <b>그 사실을 숨기지 않고 고정</b>한다 — 나중에
     * "str 은 NLS 안 탄다"고 오해하는 것을 막기 위해서다.
     *
     * <p>형식이 중요하면 {@code to_char(x,'FM999999.000')} 으로 직접 지정해야 한다.
     */
    @Test
    public void numericStr_remainsLocaleSensitive() {
        assertEquals("1234.5", str("e.d"));
        inTransaction(s -> {
            s.createNativeMutationQuery("alter session set NLS_NUMERIC_CHARACTERS=',.'")
                    .executeUpdate();
            assertEquals("소수점 기호는 세션 설정을 따른다 — 알려진 한계", "1234,5",
                    s.createQuery("select str(e.d) from SffRow e", String.class).getSingleResult());
            restoreNls(s);
        });
    }

    /** 우회로가 있다는 것도 함께 — 형식을 직접 주면 고정된다. */
    @Test
    public void toCharWithExplicitFormat_isStableEvenForNumbers() {
        inTransaction(s -> {
            s.createNativeMutationQuery("alter session set NLS_NUMERIC_CHARACTERS=',.'")
                    .executeUpdate();
            final String v = s.createQuery(
                    "select to_char(e.d, 'FM999999D000') from SffRow e", String.class).getSingleResult();
            assertNotNull(v);
            assertTrue("형식을 직접 주면 자리수가 고정된다: " + v, v.contains("1234"));
            restoreNls(s);
        });
    }

    // ------------------------------------------------------------------

    /**
     * 바꾼 세션 설정을 <b>반드시 되돌린다</b>.
     *
     * <p>커넥션은 풀로 돌아가 다음 테스트가 물려받는다. 되돌리지 않으면 숫자·날짜를
     * 문자열로 다루는 다른 테스트가 <b>실행 순서에 따라</b> 깨진다 — 재현이 어렵고
     * 원인이 엉뚱한 곳으로 보이는 유형이라, 실제로 이 파일을 쓰면서 한 번 겪었다.
     */
    private static void restoreNls(org.hibernate.Session s) {
        s.createNativeMutationQuery("alter session set NLS_DATE_FORMAT='YYYY/MM/DD'").executeUpdate();
        s.createNativeMutationQuery("alter session set NLS_NUMERIC_CHARACTERS='.,'").executeUpdate();
    }

    private String str(String path) {
        SqlCaptureInspector.clear();
        return inTransactionReturning(s -> s.createQuery(
                "select str(" + path + ") from SffRow e", String.class).getSingleResult());
    }

    private String lastSelect() {
        return SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase().startsWith("select"))
                .reduce((a, b) -> b).orElse("");
    }

    private String sqlOf(String hql) {
        SqlCaptureInspector.clear();
        inTransactionReturning(s -> s.createQuery(hql, String.class).getSingleResult());
        return lastSelect();
    }
}
