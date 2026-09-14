package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import static org.junit.Assert.*;

/**
 * <b>행 값 생성자</b>(row value constructor) 표기를 Tibero 가 받는지 값으로 확인한다.
 *
 * <h2>행 값 생성자가 무엇인가</h2>
 * 여러 컬럼을 괄호로 묶어 <b>한 덩어리처럼 비교</b>하는 SQL 표기다.
 *
 * <pre>
 * where (a, b) = (1, 2)              ← 행 값 생성자
 * where a = 1 and b = 2              ← 같은 뜻을 풀어 쓴 것
 * </pre>
 *
 * <h2>왜 중요한가 — 복합 키</h2>
 * 키가 두 컬럼인 엔티티를 조회할 때 Hibernate 가 둘 중 어느 모양으로 SQL 을 만들지가
 * 이 지원 여부로 갈린다. 특히 {@code in} 목록에서 차이가 크다.
 *
 * <pre>
 * 지원  where (a,b) in ((1,2),(3,4),(5,6))
 * 미지원 where (a=1 and b=2) or (a=3 and b=4) or (a=5 and b=6)
 * </pre>
 *
 * <p>뒤엣것은 키가 많아질수록 길어지고 <b>복합 인덱스를 제대로 타지 못할 수 있다.</b>
 *
 * <h2>이 테스트가 지키는 것</h2>
 * Oracle dialect 는 이 지원을 <b>꺼 둔다</b>. Tibero dialect 는 Hibernate 기본값(켜짐)을
 * 그대로 상속하는데, 전수검사에서 <b>그게 맞다</b>고 실측으로 확인했다.
 *
 * <p>문제는 "Oracle 이 끄니까 우리도" 하며 되돌리기 쉽다는 점이다. 그 되돌림을 막는
 * 코드 모양 고정은 {@code contract.SweepVerdictContractTest} 가 하고,
 * <b>Tibero 가 실제로 받는다</b>는 근거는 이 파일이 남긴다.
 */
public class RowValueConstructorTest extends AbstractTiberoDialectTestBase {

    public static class PairId implements Serializable {
        public Long a;
        public Long b;

        @Override public boolean equals(Object o) {
            if (!(o instanceof PairId)) {
                return false;
            }
            PairId p = (PairId) o;
            return Objects.equals(a, p.a) && Objects.equals(b, p.b);
        }

        @Override public int hashCode() {
            return Objects.hash(a, b);
        }
    }

    @Entity(name = "RvcPair") @Table(name = "RVC_PAIR") @IdClass(PairId.class)
    public static class Pair {
        @Id public Long a;
        @Id public Long b;
        public String v;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Pair.class};
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
        inTransaction(s -> s.createMutationQuery("delete from RvcPair").executeUpdate());
        inTransaction(s -> {
            for (long i = 1; i <= 4; i++) {
                Pair p = new Pair();
                p.a = i;
                p.b = i * 10;
                p.v = "v" + i;
                s.persist(p);
            }
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // Tibero 가 받는 여덟 가지 표기 — 전수검사 표 그대로
    // ------------------------------------------------------------------

    /** {@code (a,b) = (1,2)} — 리터럴 등호. */
    @Test
    public void equality_withLiterals() {
        assertEquals(Long.valueOf(1L), one("select count(*) from RVC_PAIR where (a,b) = (1,10)"));
    }

    /** {@code (a,b) = (?,?)} — 바인드 파라미터. 리터럴만 되고 바인드는 안 되는 DB 도 있다. */
    @Test
    public void equality_withBindParameters() {
        inTransaction(s -> {
            final long n = ((Number) s.createNativeQuery(
                    "select count(*) from RVC_PAIR where (a,b) = (?,?)", Object.class)
                    .setParameter(1, 1L).setParameter(2, 10L).getSingleResult()).longValue();
            assertEquals(1L, n);
        });
    }

    /** {@code (a,b) < (3,4)} — 부등호 비교. 사전식으로 비교된다. */
    @Test
    public void lessThan_comparesLexicographically() {
        assertEquals("(1,10) 과 (2,20) 두 건", Long.valueOf(2L),
                one("select count(*) from RVC_PAIR where (a,b) < (3,30)"));
    }

    /** {@code (a,b) = (select …)} — 서브쿼리와의 등호. */
    @Test
    public void equality_againstSubquery() {
        assertEquals(Long.valueOf(1L), one(
                "select count(*) from RVC_PAIR where (a,b) = (select a,b from RVC_PAIR where a=2)"));
    }

    /** {@code (a,b) in ((1,2),(3,4))} — 이 작업의 핵심. 복합 키 다건 조회가 여기로 나간다. */
    @Test
    public void inList_withMultipleTuples() {
        assertEquals(Long.valueOf(2L), one(
                "select count(*) from RVC_PAIR where (a,b) in ((1,10),(3,30))"));
    }

    /** {@code (a,b) = any (select …)} — 한정 술어. Oracle 이 끄는 두 개 중 하나. */
    @Test
    public void equalsAny_quantifiedPredicate() {
        assertEquals(Long.valueOf(2L), one(
                "select count(*) from RVC_PAIR where (a,b) = any (select a,b from RVC_PAIR where a<=2)"));
    }

    /** {@code (a,b) <> all (select …)} — 반대쪽 한정 술어. */
    @Test
    public void notEqualsAll_quantifiedPredicate() {
        assertEquals(Long.valueOf(3L), one(
                "select count(*) from RVC_PAIR where (a,b) <> all (select a,b from RVC_PAIR where a=1)"));
    }

    /** {@code update T set (a,b) = (select …)} — 대입 위치에서도 된다. */
    @Test
    public void assignment_inUpdateSet() {
        inTransaction(s -> s.createNativeMutationQuery(
                "update RVC_PAIR set (v) = (select 'changed' from dual) where a=4").executeUpdate());
        assertEquals(Long.valueOf(1L), one("select count(*) from RVC_PAIR where v='changed'"));
    }

    // ------------------------------------------------------------------
    // Hibernate 가 실제로 그 표기를 쓰는지
    // ------------------------------------------------------------------

    /**
     * ⚠️ 이 테스트가 본론이다 — 복합 키 다건 조회가 <b>튜플 표기로 나가는지</b>.
     *
     * <p>위 여덟 개는 "Tibero 가 그 SQL 을 받는다"를 확인한 것이고, 이건 "Hibernate 가
     * 실제로 그 SQL 을 만든다"를 확인한다. IN 목록의 튜플 표기를 지배하는 것은
     * {@code supportsRowValueConstructorSyntaxInInList()} 이며, 그것을 {@code false} 로
     * 내리면 {@code (a,b) in ((?,?),(?,?))} 대신
     * {@code (a=? and b=?) or (a=? and b=?)} 가 나간다 — 뮤테이션으로 확인했다.
     */
    @Test
    public void compositeKeyMultiLoad_usesTupleSyntax() {
        final PairId k1 = new PairId();
        k1.a = 1L;
        k1.b = 10L;
        final PairId k2 = new PairId();
        k2.a = 3L;
        k2.b = 30L;

        SqlCaptureInspector.clear();
        inTransaction(s -> assertEquals(2, s.byMultipleIds(Pair.class).multiLoad(List.of(k1, k2)).size()));

        final String sql = SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase().startsWith("select") && x.contains("RVC_PAIR"))
                .findFirst().orElseThrow(() -> new AssertionError("조회 SQL 을 못 찾음"));

        assertTrue("복합 키 조회가 튜플 표기로 나가야 함. 실제: " + sql,
                sql.contains(",") && sql.toLowerCase().contains("in ("));
        assertFalse("or 로 풀어 쓰면 지원 플래그가 꺼진 것이다: " + sql,
                sql.toLowerCase().contains(") or ("));
    }

    // ------------------------------------------------------------------

    private Long one(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
