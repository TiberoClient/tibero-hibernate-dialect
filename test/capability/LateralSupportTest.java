package capability;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * {@code supportsLateral() = false} 라는 판단을 <b>매 실행마다 다시 확인</b>한다.
 *
 * <h2>LATERAL 이 무엇인가</h2>
 * 조인 오른쪽의 서브쿼리가 <b>왼쪽 행의 값을 참조</b>할 수 있게 해 주는 문법이다.
 *
 * <pre>
 * select a.v, t.w
 *   from A a, lateral (select w from B b where b.aid = a.id and rownum &lt;= 1) t
 *                                                    ^^^^ 바깥 a 를 참조한다
 * </pre>
 *
 * <p>"각 부서마다 최근 주문 1건" 처럼 <b>행마다 상위 N개</b>를 뽑을 때 쓴다.
 * 이게 없으면 윈도우 함수로 돌아가거나 애플리케이션에서 N+1 조회를 해야 한다.
 *
 * <h2>이 테스트가 왜 "실패를 확인"하나</h2>
 * Tibero 7 은 LATERAL 을 받지 않는다. 그래서 dialect 가 {@code supportsLateral()} 을
 * {@code false} 로 선언한다 — 올리면 Hibernate 가 이 문법을 만들어 내고 질의가 통째로
 * 실패하기 때문이다.
 *
 * <p>문제는 <b>그 판단을 언제 다시 봐야 하는가</b> 였다. 코드에
 * "나중에 버전 올라가면 확인" 이라고 주석을 달아 두면 아무도 보지 않는다.
 *
 * <p>그래서 반대로 뒤집었다. 이 테스트는 <b>다섯 가지 LATERAL 문법이 여전히 거부되는지</b>
 * 확인한다. Tibero 가 지원을 추가하는 날 이 테스트가 <b>실패하면서</b> 알려 준다 —
 * "이제 되니 {@code supportsLateral()} 을 다시 판단하라"는 신호다.
 *
 * <p>즉 <b>실패가 나쁜 소식이 아니라 좋은 소식</b>인, 조금 특이한 테스트다.
 * 실패하면 이 파일과 {@code TiberoDialect.supportsLateral()} 을 함께 손보면 된다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialect#supportsLateral()
 */
public class LateralSupportTest extends AbstractTiberoDialectTestBase {

    private static final String PARENT = "LAT_PARENT";
    private static final String CHILD = "LAT_CHILD";

    /** ps06 에서 전부 거부됨을 확인한 다섯 가지 표기. */
    private static final String[] LATERAL_FORMS = {
            "select a.v, t.w from " + PARENT + " a, lateral "
                    + "(select w from " + CHILD + " b where b.aid=a.id and rownum<=1) t",
            "select a.v, t.w from " + PARENT + " a cross join lateral "
                    + "(select w from " + CHILD + " b where b.aid=a.id and rownum<=1) t",
            "select a.v, t.w from " + PARENT + " a cross apply "
                    + "(select w from " + CHILD + " b where b.aid=a.id and rownum<=1) t",
            "select a.v, t.w from " + PARENT + " a left join lateral "
                    + "(select w from " + CHILD + " b where b.aid=a.id and rownum<=1) t on 1=1",
            "select a.v, t.w from " + PARENT + " a outer apply "
                    + "(select w from " + CHILD + " b where b.aid=a.id and rownum<=1) t",
    };

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "none");
    }

    @Before
    public void seed() {
        dropTableWithRetry(CHILD);
        dropTableWithRetry(PARENT);
        inTransaction(s -> {
            s.createNativeMutationQuery(
                    "create table " + PARENT + " (id number(19,0), v varchar2(20))").executeUpdate();
            s.createNativeMutationQuery(
                    "create table " + CHILD + " (id number(19,0), aid number(19,0), w varchar2(20))")
                    .executeUpdate();
            s.createNativeMutationQuery("insert into " + PARENT + " values (1,'a1')").executeUpdate();
            s.createNativeMutationQuery("insert into " + CHILD + " values (1,1,'b1')").executeUpdate();
        });
    }

    // ------------------------------------------------------------------

    /** 선언값 자체. */
    @Test
    public void dialectDeclaresLateralUnsupported() {
        assertFalse(new TiberoDialect().supportsLateral());
    }

    /**
     * ⚠️ <b>이 테스트가 실패하면 좋은 소식이다.</b>
     *
     * <p>Tibero 가 LATERAL 을 지원하기 시작했다는 뜻이므로,
     * {@code supportsLateral()} 을 {@code true} 로 올릴지 다시 판단해야 한다.
     * 올릴 때는 Hibernate 가 만드는 실제 문법이 무엇인지 확인하고
     * {@code shouldEmulateLateralWithIntersect} 도 함께 봐야 한다.
     */
    @Test
    public void allLateralSyntaxes_areStillRejected() {
        final List<String> accepted = new ArrayList<>();
        for (String sql : LATERAL_FORMS) {
            if (runs(sql)) {
                accepted.add(sql);
            }
        }
        assertTrue("Tibero 가 LATERAL 을 받기 시작했다 — supportsLateral() 을 다시 판단할 것."
                        + " 통과한 문법: " + accepted,
                accepted.isEmpty());
    }

    /**
     * 대안 경로는 <b>지금도 동작한다</b>는 것을 함께 고정한다.
     *
     * <p>LATERAL 이 없다고 "행마다 상위 N개"를 못 하는 것은 아니다. 윈도우 함수로
     * 같은 일을 할 수 있고 Tibero 가 이를 지원한다. 사용자에게 알려 줄 우회로이므로
     * 함께 확인해 둔다.
     */
    @Test
    public void windowFunctionAlternative_works() {
        assertTrue("윈도우 함수 지원 선언", new TiberoDialect().supportsWindowFunctions());

        final String topNPerGroup =
                "select v, w from ("
                        + "  select a.v v, b.w w,"
                        + "         row_number() over (partition by a.id order by b.id) rn"
                        + "    from " + PARENT + " a join " + CHILD + " b on b.aid = a.id"
                        + ") where rn = 1";
        assertTrue("LATERAL 없이도 행마다 상위 N개를 뽑을 수 있어야 함", runs(topNPerGroup));
    }

    // ------------------------------------------------------------------

    /** 실행되면 {@code true}, DB 가 거부하면 {@code false}. */
    private boolean runs(String sql) {
        try {
            inTransactionReturning(s -> s.createNativeQuery(sql, Object.class).getResultList());
            return true;
        }
        catch (Exception rejected) {
            return false;
        }
    }
}
