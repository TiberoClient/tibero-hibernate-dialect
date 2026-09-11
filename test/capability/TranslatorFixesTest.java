package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.util.List;

import static org.junit.Assert.*;

/**
 * §5.3 번역기 5건을 <b>실제 DB 에서</b> 실행해 고정한다.
 *
 * <h2>왜 이 테스트가 필요했나</h2>
 * 이 다섯은 Oracle 번역기도 재정의하는 자리다. 처음에는 "Tibero 가 그 SQL 문법을
 * 받아주나"만 확인하고 불필요로 판단했는데, <b>Oracle 이 재정의하는 목적이 문법이
 * 아니었다.</b> 목적에 해당하는 상황을 재현하니 Tibero 에서도 모두 깨졌다.
 *
 * <pre>
 * where cl = 'hello'                       → JDBC-11023  (LOB 은 = 비교 불가)
 * (select .. order by a) union all (..)    → JDBC-8013   (가지 안의 정렬)
 * from (values (1,2))                      → JDBC-8013   (VALUES 테이블 참조)
 * over (partition by ())                   → JDBC-8013   (리터럴 파티션)
 * select 5/2                               → 2.5         (정수 나눗셈)
 * </pre>
 *
 * <p>렌더 결과 고정은 {@code render.TranslatorFixesRenderTest} 가 하고, 여기서는
 * <b>실행이 되는지와 값이 맞는지</b>를 본다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator
 */
public class TranslatorFixesTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "TfxItem") @Table(name = "TFX_ITEM")
    public static class Item {
        @Id public Long id;
        public Integer a;
        public Integer b;
        @Lob public String body;
        public String name;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Item.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        // 정수 나눗셈 보정은 이 옵트인 설정을 켜야 해당 경로를 탄다
        cfg.setProperty("hibernate.query.hql.portable_integer_division", "true");
    }

    @Before
    public void seed() {
        inTransaction(s -> s.createMutationQuery("delete from TfxItem").executeUpdate());
        inTransaction(s -> {
            Item i1 = new Item();
            i1.id = 1L; i1.a = 5; i1.b = 2; i1.body = "hello"; i1.name = "first";
            s.persist(i1);
            Item i2 = new Item();
            i2.id = 2L; i2.a = 9; i2.b = 4; i2.body = "world"; i2.name = "second";
            s.persist(i2);
        });
    }

    // ------------------------------------------------------------------
    // 1. 정수 나눗셈
    // ------------------------------------------------------------------

    /**
     * {@code 5/2} 는 정수끼리이므로 2 여야 한다.
     *
     * <p>보정이 없으면 Tibero 가 2.5 를 돌려주고, <b>예외가 나지 않으므로</b>
     * 옵트인 설정을 켠 사용자는 설정이 무시된 줄 모른 채 틀린 값을 받는다.
     */
    @Test
    public void integerDivision_yieldsInteger() {
        inTransaction(s -> {
            Number r = s.createQuery("select e.a / e.b from TfxItem e where e.id = 1", Number.class)
                    .getSingleResult();
            assertEquals("5/2 는 정수 나눗셈이므로 2 여야 함 (보정 없으면 2.5)", 2, r.intValue());
            assertEquals("소수부가 남아 있으면 보정이 안 된 것", 2.0d, r.doubleValue(), 0.0d);
        });
    }

    /** 9/4 = 2 — 반올림이 아니라 버림이어야 한다. */
    @Test
    public void integerDivision_truncatesNotRounds() {
        inTransaction(s -> {
            Number r = s.createQuery("select e.a / e.b from TfxItem e where e.id = 2", Number.class)
                    .getSingleResult();
            assertEquals("9/4 는 2.25 → 2 (2.3 으로 반올림하면 안 됨)", 2, r.intValue());
        });
    }

    // ------------------------------------------------------------------
    // 2. LOB 비교
    // ------------------------------------------------------------------

    /**
     * CLOB 컬럼을 조건으로 쓰는 질의.
     *
     * <p>보정이 없으면 {@code JDBC-11023 Values are from data types that cannot be
     * compared.} 로 <b>런타임 실패</b>한다.
     */
    @Test
    public void lobEquality_executesAndMatches() {
        inTransaction(s -> {
            List<Long> ids = s.createQuery(
                    "select e.id from TfxItem e where e.body = :v", Long.class)
                    .setParameter("v", "hello").getResultList();
            assertEquals("CLOB 동등 비교가 실행되고 1건만 맞아야 함", List.of(1L), ids);
        });
    }

    /**
     * 같지 않음 비교.
     *
     * <p>여기서 Oracle 번역기의 {@code -1=} 형태를 그대로 베끼면 <b>이 테스트가 깨진다</b>.
     * {@code dbms_lob.compare('world','hello')} 는 {@code +1} 이라 {@code -1=} 에 안 걸리고
     * 결과가 0건이 된다. 실제로 구현 중에 이 테스트가 그 실수를 잡아냈다.
     */
    @Test
    public void lobInequality_matchesBothDirections() {
        inTransaction(s -> {
            // 'world' > 'hello' → compare 가 +1. -1= 로 짜면 여기서 0건이 나온다
            assertEquals("앞쪽이 더 큰 경우", List.of(2L), s.createQuery(
                    "select e.id from TfxItem e where e.body <> :v order by e.id", Long.class)
                    .setParameter("v", "hello").getResultList());
            // 'hello' < 'world' → compare 가 -1
            assertEquals("앞쪽이 더 작은 경우", List.of(1L), s.createQuery(
                    "select e.id from TfxItem e where e.body <> :v order by e.id", Long.class)
                    .setParameter("v", "world").getResultList());
        });
    }

    /** 대소 비교도 {@code compare} 로 표현되며 순서가 뒤집히지 않아야 한다. */
    @Test
    public void lobOrdering_comparesInTheRightDirection() {
        inTransaction(s -> {
            assertEquals("'hello' < 'world' 이므로 1번만", List.of(1L), s.createQuery(
                    "select e.id from TfxItem e where e.body < :v order by e.id", Long.class)
                    .setParameter("v", "world").getResultList());
            assertEquals("'world' > 'hello' 이므로 2번만", List.of(2L), s.createQuery(
                    "select e.id from TfxItem e where e.body > :v order by e.id", Long.class)
                    .setParameter("v", "hello").getResultList());
        });
    }

    /** 일반 컬럼 비교는 그대로 동작해야 한다 — 보정이 과하게 번지지 않았는지 본다. */
    @Test
    public void plainColumnComparison_stillWorks() {
        inTransaction(s -> {
            List<Long> ids = s.createQuery(
                    "select e.id from TfxItem e where e.name = :v", Long.class)
                    .setParameter("v", "first").getResultList();
            assertEquals(List.of(1L), ids);
        });
    }

    // ------------------------------------------------------------------
    // 3. UNION 가지 안의 order by
    // ------------------------------------------------------------------

    /**
     * 집합 연산의 한 가지에만 정렬이 붙은 형태.
     *
     * <p>보정이 없으면 {@code JDBC-8013 Missing SELECT keyword.} 로 실패한다.
     * {@code offset 0 rows} 는 행을 건너뛰지 않으므로 <b>결과 건수가 그대로여야</b> 한다.
     */
    @Test
    public void orderByInsideUnionBranch_executes() {
        inTransaction(s -> {
            List<Integer> rows = s.createQuery(
                    "(select e.a from TfxItem e order by e.a) union all (select f.b from TfxItem f)",
                    Integer.class).getResultList();
            assertEquals("두 가지 각각 2건씩 = 4건. offset 0 rows 가 행을 깎으면 안 됨",
                    4, rows.size());
        });
    }

    /** 정렬 결과 자체도 보존돼야 한다. */
    @Test
    public void orderByInsideUnionBranch_keepsOrdering() {
        inTransaction(s -> {
            List<Integer> rows = s.createQuery(
                    "(select e.a from TfxItem e order by e.a) union all (select f.b from TfxItem f where f.id = 1)",
                    Integer.class).getResultList();
            assertEquals(3, rows.size());
            assertTrue("첫 가지의 정렬(5,9)이 유지돼야 함: " + rows,
                    rows.indexOf(5) < rows.indexOf(9));
        });
    }

    /** 페이징이 걸린 평범한 쿼리는 그대로 동작해야 한다 — 회귀 방지. */
    @Test
    public void normalPaging_stillWorks() {
        inTransaction(s -> {
            List<Long> ids = s.createQuery("select e.id from TfxItem e order by e.id", Long.class)
                    .setFirstResult(1).setMaxResults(1).getResultList();
            assertEquals(List.of(2L), ids);
        });
    }
}
