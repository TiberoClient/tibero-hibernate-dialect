package semantics;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.TimeZoneStorage;
import org.hibernate.annotations.TimeZoneStorageType;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.TimeZoneSupport;
import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.time.Duration;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;

import static org.junit.Assert.*;

/**
 * 선언만 있고 <b>실제 DB 동작 확인이 없던</b> 시간 관련 훅 두 개를 값으로 검증한다.
 *
 * <p>§6.5 "아직 테스트가 없는 검증" 목록의 두 항목이다. 기존 테스트는
 * {@code contract.FeatureFlagsContractTest} 가 <b>선언값만</b> 고정하고 있었다 —
 * dialect 가 뭐라고 말하는지는 봤지만 DB 가 실제로 그렇게 동작하는지는 안 봤다.
 * 선언과 실제가 어긋나면 Hibernate 는 잘못된 SQL 을 자신 있게 만들어 낸다.
 *
 * <pre>
 * getTimeZoneSupport()                   = NATIVE            → 오프셋을 DB가 직접 보관하는가
 * getFractionalSecondPrecisionInNanos()  = 1_000_000_000     → 시각 차이의 나노초 환산 배율이 맞는가
 * </pre>
 */
public class TimeZoneAndPrecisionTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "TzpEvent") @Table(name = "TZP_EVENT")
    public static class Event {
        @Id public Long id;

        /**
         * {@code NATIVE} — 오프셋을 <b>DB 컬럼이 직접</b> 들고 있게 한다
         * ({@code timestamp with time zone}). 이게 되려면 dialect 가
         * {@code getTimeZoneSupport() == NATIVE} 라고 선언해야 한다.
         */
        @TimeZoneStorage(TimeZoneStorageType.NATIVE)
        public OffsetDateTime at;

        /** 비교용 — 같은 순간을 UTC 로 환산해 저장한다. 컬럼 타입은 NATIVE 와 같다. */
        @TimeZoneStorage(TimeZoneStorageType.NORMALIZE_UTC)
        public OffsetDateTime atUtc;

        @TimeZoneStorage(TimeZoneStorageType.NATIVE)
        public ZonedDateTime zoned;

        @Column(name = "T_FROM") public java.time.LocalDateTime from;
        @Column(name = "T_TO") public java.time.LocalDateTime to;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Event.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        cfg.setProperty("hibernate.hbm2ddl.halt_on_error", "true");
    }

    @Before
    public void clean() {
        inTransaction(s -> s.createMutationQuery("delete from TzpEvent").executeUpdate());
    }

    // ------------------------------------------------------------------
    // getTimeZoneSupport = NATIVE
    // ------------------------------------------------------------------

    /**
     * DDL 이 실제로 {@code timestamp with time zone} 으로 만들어지는지.
     *
     * <p>⚠️ Tibero 에서는 {@code NORMALIZE_UTC} 도 <b>같은 컬럼 타입</b>이 된다(실측).
     * 둘의 차이는 컬럼 타입이 아니라 <b>저장되는 값</b>에 있다 — {@code NATIVE} 는 준 오프셋을
     * 그대로 두고, {@code NORMALIZE_UTC} 는 UTC 로 환산해 넣는다. 그래서 컬럼 타입으로
     * 두 방식을 구분하려 하면 안 된다.
     */
    @Test
    public void nativeStorage_createsTimestampWithTimeZoneColumn() {
        assertEquals(TimeZoneSupport.NATIVE, new TiberoDialect().getTimeZoneSupport());

        assertEquals("NATIVE 는 오프셋을 들고 있는 컬럼 타입이어야 함", 1L, count(
                "select count(*) from user_tab_columns where table_name='TZP_EVENT' "
                        + "and column_name='AT' and data_type like 'TIMESTAMP%TIME ZONE%'"));
        assertEquals("NORMALIZE_UTC 도 Tibero 에서는 같은 타입이 된다", 1L, count(
                "select count(*) from user_tab_columns where table_name='TZP_EVENT' "
                        + "and column_name='ATUTC' and data_type like 'TIMESTAMP%TIME ZONE%'"));
    }

    /**
     * 두 저장 방식이 <b>값에서</b> 갈리는지 — 컬럼 타입이 같으니 여기서 구분된다.
     *
     * <p>{@code NATIVE} 는 {@code +09:00} 을 그대로 보관하고, {@code NORMALIZE_UTC} 는
     * 같은 순간을 {@code +00:00} 으로 환산해 넣는다. 순간은 둘 다 같다.
     */
    @Test
    public void nativeKeepsTheOffset_whileNormalizeUtcConvertsIt() {
        final OffsetDateTime seoul = OffsetDateTime.of(2024, 3, 5, 11, 22, 33, 0, ZoneOffset.ofHours(9));
        inTransaction(s -> {
            Event e = new Event();
            e.id = 6L;
            e.at = seoul;
            e.atUtc = seoul;
            s.persist(e);
        });
        final String offsets = inTransactionReturning(s -> s.createNativeQuery(
                "select to_char(at,'TZH:TZM')||'|'||to_char(atUtc,'TZH:TZM') from TZP_EVENT where id=6",
                String.class).getSingleResult());
        final String[] parts = offsets.split("\\|");
        assertEquals("NATIVE 는 준 오프셋 그대로", "+09:00", parts[0]);
        assertEquals("NORMALIZE_UTC 는 UTC 로 환산", "+00:00", parts[1]);
    }

    /**
     * ⚠️ 핵심 — 오프셋이 <b>들어간 그대로</b> 돌아오는지.
     *
     * <p>{@code +09:00} 으로 넣었는데 {@code +00:00} 으로 돌아오면 순간(instant)은 같아도
     * <b>원래 어느 지역의 시각이었는지가 사라진다.</b> 로그·감사 기록처럼 "몇 시에 일어난
     * 일인가"를 그 지역 기준으로 보여줘야 하는 데이터에서 문제가 된다.
     */
    @Test
    public void nativeStorage_preservesTheOffsetItself() {
        final OffsetDateTime seoul = OffsetDateTime.of(2024, 3, 5, 11, 22, 33, 0, ZoneOffset.ofHours(9));
        inTransaction(s -> {
            Event e = new Event();
            e.id = 1L;
            e.at = seoul;
            e.atUtc = seoul;
            s.persist(e);
        });
        inTransaction(s -> {
            Event e = s.find(Event.class, 1L);
            assertEquals("NATIVE 는 오프셋까지 보존해야 함", ZoneOffset.ofHours(9), e.at.getOffset());
            assertTrue("순간도 같아야 함", seoul.isEqual(e.at));
            // 비교군 — 이쪽은 오프셋이 사라지는 것이 정상 동작이다
            assertTrue("NORMALIZE_UTC 도 순간은 보존", seoul.isEqual(e.atUtc));
        });
    }

    /** 음수 오프셋과 30분 단위 오프셋 — 부호와 분 단위가 뭉개지지 않는지. */
    @Test
    public void nativeStorage_handlesNegativeAndHalfHourOffsets() {
        final OffsetDateTime newYork = OffsetDateTime.of(2024, 3, 5, 11, 22, 33, 0, ZoneOffset.ofHours(-5));
        final OffsetDateTime india = OffsetDateTime.of(2024, 3, 5, 11, 22, 33, 0,
                ZoneOffset.ofHoursMinutes(5, 30));
        inTransaction(s -> {
            Event a = new Event();
            a.id = 2L;
            a.at = newYork;
            s.persist(a);
            Event b = new Event();
            b.id = 3L;
            b.at = india;
            s.persist(b);
        });
        inTransaction(s -> {
            assertEquals(ZoneOffset.ofHours(-5), s.find(Event.class, 2L).at.getOffset());
            assertEquals("30분 단위 오프셋도 보존",
                    ZoneOffset.ofHoursMinutes(5, 30), s.find(Event.class, 3L).at.getOffset());
        });
    }

    /** {@code ZonedDateTime} 경로도 같은지 — 매핑이 다르게 동작할 수 있어 따로 본다. */
    @Test
    public void nativeStorage_worksForZonedDateTime() {
        final ZonedDateTime z = ZonedDateTime.of(2024, 3, 5, 11, 22, 33, 0, ZoneOffset.ofHours(9));
        inTransaction(s -> {
            Event e = new Event();
            e.id = 4L;
            e.zoned = z;
            s.persist(e);
        });
        inTransaction(s -> assertTrue(z.toInstant().equals(s.find(Event.class, 4L).zoned.toInstant())));
    }

    /** 오프셋으로 걸러지는지 — 저장만 되고 조건에 안 걸리면 반쪽이다. */
    @Test
    public void nativeStorage_isUsableInPredicates() {
        final OffsetDateTime seoul = OffsetDateTime.of(2024, 3, 5, 11, 22, 33, 0, ZoneOffset.ofHours(9));
        inTransaction(s -> {
            Event e = new Event();
            e.id = 5L;
            e.at = seoul;
            s.persist(e);
        });
        inTransaction(s -> assertEquals(Long.valueOf(5L), s.createQuery(
                "select e.id from TzpEvent e where e.at = :t", Long.class)
                .setParameter("t", seoul).getSingleResult()));
    }

    // ------------------------------------------------------------------
    // getFractionalSecondPrecisionInNanos = 1초
    // ------------------------------------------------------------------

    /**
     * 시각 차이를 {@code Duration} 으로 받을 때 <b>나노초까지 정확한지</b>.
     *
     * <h3>왜 이게 이 훅의 시험대인가</h3>
     * {@code native} 는 HQL 에 직접 쓸 수 없는 내부 단위다. Hibernate 는 DB 가 돌려준
     * 간격을 나노초로 환산할 때 {@code TemporalUnit.conversionFactor()} 를 쓰고, 그 안에서
     * {@code NATIVE} 배율로 {@code getFractionalSecondPrecisionInNanos()} 를 곱한다.
     *
     * <p>즉 이 값이 틀리면 <b>시간 차이가 배수로 어긋난다.</b> 마이크로초(1_000)로
     * 잘못 선언하면 같은 0.5초가 나노초 환산에서 10⁶배 어긋난 값으로 나온다.
     * 예외가 아니라 <b>조용히 틀린 숫자</b>로 나오는 유형이라 값으로 고정해 둔다.
     */
    @Test
    public void durationSubtraction_isNanosecondAccurate() {
        assertEquals(1_000_000_000L, new TiberoDialect().getFractionalSecondPrecisionInNanos());

        seedInterval(20L, 0, 500_000_000);
        inTransaction(s -> {
            assertEquals("0.5초 차이가 Duration 으로 정확해야 함", Duration.ofMillis(500), s.createQuery(
                    "select (e.to - e.from) from TzpEvent e where e.id = 20", Duration.class)
                    .getSingleResult());
            assertEquals("나노초로 환산해도 정확해야 함", Long.valueOf(500_000_000L), s.createQuery(
                    "select (e.to - e.from) by nanosecond from TzpEvent e where e.id = 20", Long.class)
                    .getSingleResult());
        });
    }

    /** 1시간처럼 큰 간격도 배율이 어긋나지 않는지 — 작은 값만 맞고 큰 값이 틀리는 실수를 막는다. */
    @Test
    public void durationSubtraction_scalesCorrectlyForLargeIntervals() {
        seedInterval(21L, 3600, 0);
        inTransaction(s -> {
            assertEquals(Duration.ofHours(1), s.createQuery(
                    "select (e.to - e.from) from TzpEvent e where e.id = 21", Duration.class)
                    .getSingleResult());
            assertEquals(Long.valueOf(3600L), s.createQuery(
                    "select (e.to - e.from) by second from TzpEvent e where e.id = 21", Long.class)
                    .getSingleResult());
            assertEquals(Long.valueOf(3_600_000_000_000L), s.createQuery(
                    "select (e.to - e.from) by nanosecond from TzpEvent e where e.id = 21", Long.class)
                    .getSingleResult());
        });
    }

    /**
     * {@code by second} 는 내림한다 — 선언된 해상도와 일치하는 동작.
     *
     * <p>0.5초는 {@code by second} 로 0 이 된다. 놀랄 일이 아니라 초 단위로 끊는 것이
     * 맞는 동작이며, 더 잘게 보려면 {@code by nanosecond} 를 써야 한다는 점을 고정한다.
     */
    @Test
    public void bySecond_truncatesSubSecondPart() {
        seedInterval(22L, 0, 500_000_000);
        inTransaction(s -> assertEquals(Long.valueOf(0L), s.createQuery(
                "select (e.to - e.from) by second from TzpEvent e where e.id = 22", Long.class)
                .getSingleResult()));
    }

    private void seedInterval(long id, int seconds, int nanos) {
        inTransaction(s -> {
            Event e = new Event();
            e.id = id;
            e.from = java.time.LocalDateTime.of(2024, 3, 5, 10, 0, 0, 0);
            e.to = e.from.plusSeconds(seconds).plusNanos(nanos);
            s.persist(e);
        });
    }

    // ------------------------------------------------------------------

    private long count(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
