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

import static org.junit.Assert.*;

/**
 * 전체 리뷰 §4 의 코드 결함에 대한 회귀 테스트.
 *
 * <p>다섯 건 모두 <b>기존 테스트가 보지 않던 자리</b>에 있었다. 그래서 고친 것만으로는
 * 부족하고, 같은 모양이 다시 들어오면 실패하도록 여기서 고정한다.
 *
 * <pre>
 * §4.1  extract(epoch from &lt;DATE&gt;)      JDBC-11003
 * §4.2  FROM 절 파생 테이블                  JDBC-8022
 * §4.3  number(p,0) 역매핑                  JDBC-590749 · 7 이 true 로
 * §4.4  페이징+락에서 바깥 order by 유실      조용히 순서 뒤섞임
 *       정렬 없는 페이징+락                  NullPointerException
 * </pre>
 *
 * <p>§4.5(LimitHandler 공유 상태)는 {@code LimitHandlerTest}, §4.6(최소 지원 버전)은
 * {@code DialectDecisionContractTest} 가 각각 지킨다.
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

    private String lastSelect() {
        return SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase(Locale.ROOT).startsWith("select"))
                .reduce((a, b) -> b).orElse("");
    }
}
