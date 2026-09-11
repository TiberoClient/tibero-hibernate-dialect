package semantics;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.time.LocalDate;
import java.time.LocalDateTime;

import static org.junit.Assert.*;

/**
 * D 유형 — 시간 패턴이 <b>맞는 값</b>을 돌려주는지 본다.
 *
 * <p>{@code TemporalPatternExecutionTest} 는 같은 식들을 실행만 하고 결과를 버린다(C 유형).
 * 그래서 "실행은 되는데 값이 틀린" 종류를 못 잡는다 — month 시각 유실이 그 사례였다.
 * 여기서는 기준 데이터를 고정하고 기대값과 대조한다.
 */
public class TemporalValueTest extends AbstractTiberoDialectTestBase {

    /** 기준 데이터 — 2024-03-15(금) 10:20:30, 연중 75일차, ISO 11주차 */
    private static final LocalDateTime TS = LocalDateTime.of(2024, 3, 15, 10, 20, 30);
    private static final LocalDate D = LocalDate.of(2024, 3, 15);

    /** 월말 클램프 확인용 두 번째 행 — 1/31 은 +1개월 하면 존재하지 않는 2/31 이 된다 */
    private static final LocalDateTime TS_EOM = LocalDateTime.of(2024, 1, 31, 10, 20, 30);

    @Entity(name = "TvEntity") @Table(name = "TV_T")
    public static class TvEntity {
        @Id public Long id;
        public LocalDateTime ts;
        public LocalDate d;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{TvEntity.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    @Before
    public void seed() {
        inTransaction(session -> {
            session.createMutationQuery("delete from TvEntity").executeUpdate();
            TvEntity e = new TvEntity();
            e.id = 1L; e.ts = TS; e.d = D;
            session.persist(e);
            TvEntity eom = new TvEntity();
            eom.id = 2L; eom.ts = TS_EOM; eom.d = TS_EOM.toLocalDate();
            session.persist(eom);
        });
    }

    private <T> T one(String select, Class<T> type) {
        return inTransactionReturning(session -> session.createQuery(
                "select " + select + " from TvEntity x where x.id=1", type).getSingleResult());
    }

    /** 월말 행(2024-01-31 10:20:30) 기준으로 조회한다. */
    private <T> T eom(String select, Class<T> type) {
        return inTransactionReturning(session -> session.createQuery(
                "select " + select + " from TvEntity x where x.id=2", type).getSingleResult());
    }

    private int num(String select) {
        return ((Number) one(select, Object.class)).intValue();
    }

    // ------------------------------------------------------------------
    // extractPattern — to_char 우회가 올바른 포맷을 쓰는지
    // ------------------------------------------------------------------

    @Test
    public void extract_calendarFields_returnExpectedValues() {
        assertEquals(2024, num("extract(year from x.d)"));
        assertEquals(3, num("extract(month from x.d)"));
        assertEquals(15, num("extract(day from x.d)"));
        assertEquals(1, num("extract(quarter from x.d)"));
        assertEquals("2024-03-15 는 ISO 11주차 — WW 가 아니라 IW 를 써야 함", 11, num("extract(week from x.d)"));
        assertEquals("2024-03-15 는 금요일", 6, num("extract(day of week from x.d)"));
        assertEquals("2024-03-15 는 연중 75일차", 75, num("extract(day of year from x.d)"));
        assertEquals(15, num("extract(day of month from x.d)"));
    }

    @Test
    public void extract_timeFields_returnExpectedValues() {
        assertEquals(10, num("extract(hour from x.ts)"));
        assertEquals(20, num("extract(minute from x.ts)"));
        assertEquals(30, num("extract(second from x.ts)"));
    }

    // ------------------------------------------------------------------
    // timestampaddPattern
    // ------------------------------------------------------------------

    @Test
    public void timestampadd_subDayUnits_preserveTimeOfDay() {
        assertEquals(LocalDateTime.of(2024, 3, 16, 10, 20, 30), one("x.ts + 1 day", LocalDateTime.class));
        assertEquals(LocalDateTime.of(2024, 3, 15, 11, 20, 30), one("x.ts + 1 hour", LocalDateTime.class));
        assertEquals(LocalDateTime.of(2024, 3, 15, 10, 50, 30), one("x.ts + 30 minute", LocalDateTime.class));
        assertEquals(LocalDateTime.of(2024, 3, 22, 10, 20, 30), one("x.ts + 1 week", LocalDateTime.class));
    }

    @Test
    public void timestampadd_monthUnits_clampToEndOfMonth() {
        assertEquals(LocalDate.of(2024, 4, 15), one("x.d + 1 month", LocalDate.class));
        assertEquals(LocalDate.of(2024, 6, 15), one("x.d + 1 quarter", LocalDate.class));
        assertEquals(LocalDate.of(2025, 3, 15), one("x.d + 1 year", LocalDate.class));
        assertEquals("1/31 + 1개월은 2/29 로 잘려야 함 (윤년)",
                LocalDate.of(2024, 2, 29),
                one("cast('2024-01-31' as LocalDate) + 1 month", LocalDate.class));
    }

    /**
     * month/quarter/year 연산도 TIMESTAMP 의 <b>시각을 보존</b>해야 한다.
     *
     * <p>Hibernate {@code OracleDialect.yqmSelect} 는 {@code trunc(ts,'MONTH')} 로 시작해
     * 일자만 되더하므로 시각이 {@code 00:00} 으로 깎인다. 6.6.1 초기에는 그 식을 그대로
     * 복사해 썼고 이 테스트도 깎인 값을 고정하고 있었다.
     *
     * <p>조사 결과 시각을 버리는 것은 <b>Hibernate 공통 의미가 아니라 Oracle dialect 한 곳만의
     * 특성</b>이었다 — PostgreSQL·MySQL·SQL Server·H2·DB2·HSQL 은 모두 DB 네이티브 함수를 써서
     * 시각을 보존한다. 그래서 Oracle 식을 따라가지 않고 {@code add_months} 기반으로 바꿨다.
     *
     * @see #timestampadd_monthUnits_clampToEndOfMonth 월말 클램프는 그대로 유지되어야 한다
     */
    @Test
    public void timestampadd_monthUnits_onTimestamp_preserveTimeOfDay() {
        assertEquals("month 연산이 시각을 버리면 안 됨",
                LocalDateTime.of(2024, 4, 15, 10, 20, 30), one("x.ts + 1 month", LocalDateTime.class));
        assertEquals("quarter 도 동일",
                LocalDateTime.of(2024, 6, 15, 10, 20, 30), one("x.ts + 1 quarter", LocalDateTime.class));
        assertEquals("year 도 동일",
                LocalDateTime.of(2025, 3, 15, 10, 20, 30), one("x.ts + 1 year", LocalDateTime.class));
        assertEquals("음수 방향도 동일",
                LocalDateTime.of(2024, 2, 15, 10, 20, 30), one("x.ts - 1 month", LocalDateTime.class));
    }

    /** 월말 클램프와 시각 보존이 <b>동시에</b> 성립해야 한다 — 둘 중 하나만 되면 안 된다. */
    @Test
    public void timestampadd_monthUnits_clampAndPreserveTimeTogether() {
        assertEquals("1/31 10:20:30 + 1개월 = 2/29 10:20:30 (윤년 클램프 + 시각 보존)",
                LocalDateTime.of(2024, 2, 29, 10, 20, 30),
                eom("x.ts + 1 month", LocalDateTime.class));
        assertEquals("DATE 는 시각이 없으니 날짜만 클램프",
                LocalDate.of(2024, 2, 29), eom("x.d + 1 month", LocalDate.class));
    }

    // ------------------------------------------------------------------
    // timestampdiffPattern
    // ------------------------------------------------------------------

    @Test
    public void timestampdiff_returnsExpectedCounts() {
        assertEquals(10, num("timestampdiff(day, x.ts, x.ts + 10 day)"));
        assertEquals(5, num("timestampdiff(hour, x.ts, x.ts + 5 hour)"));
        assertEquals(7, num("timestampdiff(minute, x.ts, x.ts + 7 minute)"));
        assertEquals(11, num("timestampdiff(second, x.ts, x.ts + 11 second)"));
        assertEquals(3, num("timestampdiff(week, x.d, x.d + 21 day)"));
        assertEquals(3, num("timestampdiff(month, x.d, x.d + 3 month)"));
        assertEquals(2, num("timestampdiff(year, x.d, x.d + 2 year)"));
    }

    // ------------------------------------------------------------------
    // castPattern / format — NLS 설정에 흔들리지 않아야 함
    // ------------------------------------------------------------------

    @Test
    public void format_and_castToString_useExplicitIsoFormat() {
        assertEquals("2024-03-15 10:20:30", one("format(x.ts as 'yyyy-MM-dd HH:mm:ss')", String.class));
        assertTrue("cast(date as String) 은 ISO 형태여야 함",
                one("cast(x.d as String)", String.class).startsWith("2024-03-15"));
    }
}
