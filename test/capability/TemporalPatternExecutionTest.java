package capability;

import support.AbstractTiberoDialectTestBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * castPattern / extractPattern / timestampaddPattern / timestampdiffPattern 의
 * 분기를 HQL 로 실제 실행해서 Tibero 가 생성 SQL 을 받아들이는지 검증한다.
 *
 * 패턴에 numtoyminterval / year(9) to month / at time zone / 864e11 같은 구문이 들어가므로
 * 문자열 단언이 아니라 실행으로 검증한다.
 */
public class TemporalPatternExecutionTest extends AbstractTiberoDialectTestBase {

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{TpEntity.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    @Before
    public void seed() {
        inTransaction(session -> {
            Long cnt = session.createQuery("select count(e) from TpEntity e", Long.class).getSingleResult();
            if (cnt != null && cnt > 0) return;
            session.persist(new TpEntity(1L,
                    Timestamp.valueOf("2026-08-12 13:45:56.123456"),
                    Timestamp.valueOf("2025-02-27 01:02:03.000001"),
                    "2026-08-12",
                    "13:45:56",
                    "2026-08-12 13:45:56.123456789"));
        });
    }

    // ------------------------------------------------------------------------
    // extractPattern
    // ------------------------------------------------------------------------

    @Test
    public void extractPattern_allSupportedFields_executeOnTibero() {
        Map<String, String> hql = new LinkedHashMap<>();
        for (String f : new String[]{
                "year", "month", "day", "hour", "minute", "second",
                "week", "quarter", "day of week", "day of month", "day of year", "epoch"}) {
            hql.put("extract " + f, "select extract(" + f + " from e.tsA) from TpEntity e where e.id=1");
        }
        runAll(hql);
    }

    // ------------------------------------------------------------------------
    // timestampaddPattern
    // ------------------------------------------------------------------------

    @Test
    public void timestampaddPattern_allSupportedUnits_executeOnTibero() {
        Map<String, String> hql = new LinkedHashMap<>();
        for (String u : units()) {
            hql.put("timestampadd " + u + " (timestamp)",
                    "select timestampadd(" + u + ", 1, e.tsA) from TpEntity e where e.id=1");
        }
        runAll(hql);
    }

    // ------------------------------------------------------------------------
    // timestampdiffPattern
    // ------------------------------------------------------------------------

    @Test
    public void timestampdiffPattern_allSupportedUnits_executeOnTibero() {
        Map<String, String> hql = new LinkedHashMap<>();
        for (String u : units()) {
            hql.put("timestampdiff " + u + " (timestamp,timestamp)",
                    "select timestampdiff(" + u + ", e.tsB, e.tsA) from TpEntity e where e.id=1");
        }
        runAll(hql);
    }

    // ------------------------------------------------------------------------
    // castPattern
    // ------------------------------------------------------------------------

    @Test
    public void castPattern_temporalAndStringTargets_executeOnTibero() {
        Map<String, String> hql = new LinkedHashMap<>();
        hql.put("cast timestamp->String", "select cast(e.tsA as String) from TpEntity e where e.id=1");
        hql.put("cast String->Date", "select cast(e.dateStr as Date) from TpEntity e where e.id=1");
        hql.put("cast String->Time", "select cast(e.timeStr as Time) from TpEntity e where e.id=1");
        hql.put("cast String->Timestamp", "select cast(e.tsStr as Timestamp) from TpEntity e where e.id=1");
        hql.put("cast String->Integer", "select cast('123' as Integer) from TpEntity e where e.id=1");
        hql.put("cast String->Long", "select cast('123' as Long) from TpEntity e where e.id=1");
        hql.put("cast String->Double", "select cast('1.5' as Double) from TpEntity e where e.id=1");
        hql.put("cast String->BigDecimal", "select cast('1.5' as BigDecimal) from TpEntity e where e.id=1");
        hql.put("cast Integer->String", "select cast(123 as String) from TpEntity e where e.id=1");
        runAll(hql);
    }

    // ------------------------------------------------------------------------

    private static String[] units() {
        return new String[]{"year", "quarter", "month", "week", "day", "hour", "minute", "second", "nanosecond"};
    }

    /**
     * 전 분기를 돌리고 실패를 모아서 한 번에 보고한다 (첫 실패에서 멈추면 남은 분기를 못 봄)
     */
    private void runAll(Map<String, String> cases) {
        List<String> failures = new ArrayList<>();
        for (Map.Entry<String, String> c : cases.entrySet()) {
            try {
                Object v = inTransactionReturning(session ->
                        session.createQuery(c.getValue(), Object.class).getSingleResult());
            } catch (Exception e) {
                Throwable root = e;
                while (root.getCause() != null) root = root.getCause();
                failures.add(c.getKey() + "  :: " + c.getValue() + "  :: " + root.getMessage());
            }
        }
        if (!failures.isEmpty()) {
            fail("Tibero 에서 실행 실패한 분기 " + failures.size() + "/" + cases.size() + "건:\n  "
                    + String.join("\n  ", failures));
        }
    }

    @Entity(name = "TpEntity")
    @Table(name = "TP_ENTITY")
    public static class TpEntity {

        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "TS_A")
        public Timestamp tsA;

        @Column(name = "TS_B")
        public Timestamp tsB;

        @Column(name = "DATE_STR")
        public String dateStr;

        @Column(name = "TIME_STR")
        public String timeStr;

        @Column(name = "TS_STR")
        public String tsStr;

        protected TpEntity() {}

        public TpEntity(Long id, Timestamp tsA, Timestamp tsB, String dateStr, String timeStr, String tsStr) {
            this.id = id;
            this.tsA = tsA;
            this.tsB = tsB;
            this.dateStr = dateStr;
            this.timeStr = timeStr;
            this.tsStr = tsStr;
        }
    }
}
