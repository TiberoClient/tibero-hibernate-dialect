package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.dialect.OracleSqlAstTranslator;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * 타 vendor 전수검사에서 내린 판정을 <b>코드로 고정</b>한다.
 *
 * <h2>왜 이 테스트가 필요한가</h2>
 * 전수검사의 결론 중 상당수는 <b>"Oracle 이 재정의하지만 우리는 하지 않는다"</b> 였다.
 * 문제는 이런 결론이 <b>코드에 흔적을 남기지 않는다</b>는 점이다. 무언가를 하지 않기로 한
 * 결정은 파일에 아무것도 쓰이지 않으므로, 나중에 누가 Oracle 코드를 보고
 * <b>"Oracle 이 하니까 우리도 해야지"</b> 하며 넣어도 막을 장치가 없다.
 *
 * <p>그리고 그중 일부는 <b>넣으면 오히려 퇴보</b>한다. 이 테스트가 그 되돌림을 막는다.
 *
 * <h2>무엇을 고정하나</h2>
 * <pre>
 * ① 행 값 생성자 7종   — Oracle 은 2개를 false 로 내린다. Tibero 는 기본값 true 를 유지해야 한다
 * ② 번역기 플래그 3종   — Oracle 이 재정의하는 것을 우리는 일부러 상속만 한다
 * ③ 예약어 등록        — Oracle 은 비워 둔 자리를 우리는 채워야 한다
 * </pre>
 *
 * <p>실제 SQL 이 Tibero 에서 도는지는 {@code capability.RowValueConstructorTest} 가 본다.
 * 여기서는 <b>코드의 모양</b>만 DB 없이 고정한다.
 */
public class SweepVerdictContractTest {

    /**
     * 행 값 생성자 — {@code (a,b) = (1,2)} 같은 표기.
     *
     * <p>Oracle 은 이 중 둘을 {@code false} 로 내려 Hibernate 가
     * {@code a=1 and b=2} 로 풀어 쓰게 만든다. Tibero 는 <b>전부 그대로 받으므로</b>
     * 기본값({@code true})을 유지하는 것이 맞다.
     */
    private static final String[] ROW_VALUE_METHODS = {
            "supportsRowValueConstructorSyntax",
            "supportsRowValueConstructorGtLtSyntax",
            "supportsRowValueConstructorDistinctFromSyntax",
            "supportsRowValueConstructorSyntaxInSet",
            "supportsRowValueConstructorSyntaxInQuantifiedPredicates",
            "supportsRowValueConstructorSyntaxInInList",
            "supportsRowValueConstructorSyntaxInInSubQuery",
    };

    /** Oracle 이 재정의하지만 실측상 Tibero 에는 필요 없던 번역기 플래그. */
    private static final String[] TRANSLATOR_FLAGS = {
            "supportsNestedSubqueryCorrelation",
            "supportsSimpleQueryGrouping",
            "supportsDuplicateSelectItemsInQueryGroup",
    };

    // ------------------------------------------------------------------
    // ① 행 값 생성자 — 내리면 퇴보한다
    // ------------------------------------------------------------------

    /**
     * ⚠️ 행 값 생성자 지원을 <b>끄면 안 된다</b>.
     *
     * <p>Oracle 을 따라 {@code false} 로 내리면 Hibernate 가 {@code (a,b)=(1,2)} 를
     * {@code a=1 and b=2} 로 풀어 쓴다. 복합 키 조회 SQL 이 길어지고 <b>복합 인덱스
     * 사용이 나빠질 수 있다.</b> Tibero 는 8가지 표기를 전부 받는 것으로 실측 확인했다
     * ({@code capability.RowValueConstructorTest}).
     *
     * <p>재정의를 <b>하지 않는 것</b>이 결정이므로, 재정의가 생기면 실패시킨다.
     */
    @Test
    public void rowValueConstructorSupport_mustStayAtTheInheritedDefault() {
        final List<String> overridden = declaredIn(TiberoSqlAstTranslator.class, ROW_VALUE_METHODS);

        assertTrue("행 값 생성자 지원을 재정의하면 안 된다. Tibero 는 8가지 표기를 전부 받으므로"
                        + " 끄면 복합 키 조회가 a=? and b=? 로 풀려 인덱스 사용이 나빠진다."
                        + " 재정의된 것: " + overridden,
                overridden.isEmpty());
    }

    /** Oracle 은 실제로 내린다는 사실을 함께 고정한다 — "왜 우리만 다른가"의 근거. */
    @Test
    public void oracleActuallyDisablesTwoOfThem_soTheDivergenceIsDeliberate() {
        final List<String> oracleOverrides = declaredIn(OracleSqlAstTranslator.class, ROW_VALUE_METHODS);

        assertEquals("Oracle 이 내리는 개수가 바뀌면 판정 근거를 다시 확인해야 한다: " + oracleOverrides,
                2, oracleOverrides.size());
        assertTrue(oracleOverrides.contains("supportsRowValueConstructorSyntax"));
        assertTrue(oracleOverrides.contains("supportsRowValueConstructorSyntaxInQuantifiedPredicates"));
    }

    // ------------------------------------------------------------------
    // ② 번역기 플래그 — Oracle 은 재정의, 우리는 상속
    // ------------------------------------------------------------------

    /**
     * Oracle 이 재정의하는 세 플래그를 <b>우리는 일부러 상속만 한다</b>.
     *
     * <pre>
     * supportsNestedSubqueryCorrelation        Oracle: 2단계 상관 불가  → Tibero: OK
     * supportsSimpleQueryGrouping              Oracle: UNION 그룹핑 제약 → Tibero: OK
     * supportsDuplicateSelectItemsInQueryGroup Oracle: 중복 select 거부 → Tibero: OK
     * </pre>
     */
    @Test
    public void translatorFlags_mustStayInherited() {
        final List<String> overridden = declaredIn(TiberoSqlAstTranslator.class, TRANSLATOR_FLAGS);

        assertTrue("Tibero 는 이 제약들이 없으므로 재정의하지 않는다. 재정의된 것: " + overridden,
                overridden.isEmpty());
    }

    /** 반대쪽 — Oracle 은 셋 다 재정의한다. 비교 대상이 사라지면 판정을 다시 봐야 한다. */
    @Test
    public void oracleOverridesAllThree_soTheComparisonIsMeaningful() {
        assertEquals("Oracle 쪽이 바뀌면 전수검사 판정을 다시 확인할 것",
                TRANSLATOR_FLAGS.length,
                declaredIn(OracleSqlAstTranslator.class, TRANSLATOR_FLAGS).size());
    }

    // ------------------------------------------------------------------
    // ③ 예약어 — Oracle 이 비워 둔 자리
    // ------------------------------------------------------------------

    /**
     * 예약어 등록은 <b>Oracle 을 기준으로 삼았으면 영영 못 찾았을 항목</b>이다.
     *
     * <p>전수검사의 출발점이 된 사례다. Oracle dialect 는 {@code registerDefaultKeywords()}
     * 를 재정의하지 않아서, "Oracle 이 재정의한 메서드를 우리도 재정의했는가" 라는 기준으로는
     * 이 자리가 <b>비교 대상에 오르지도 않았다.</b> 다른 vendor 7곳을 함께 보고서야 드러났다.
     */
    @Test
    public void reservedWords_fillTheSlotOracleLeavesEmpty() {
        assertTrue("Tibero 는 고유 예약어를 등록해야 한다",
                declares(TiberoDialect.class, "registerDefaultKeywords"));
        assertFalse("Oracle 은 이 자리를 비워 둔다 — 그래서 Oracle 기준선으로는 안 보였다",
                declares(OracleDialect.class, "registerDefaultKeywords"));
        // 기본 구현은 Dialect 에 있다. ANSI 표준 예약어만 등록하므로 vendor 고유 예약어는
        // 각 dialect 가 덧붙여야 하고, Oracle 은 그러지 않는다
        assertTrue("기본 구현은 Dialect 가 갖고 있다",
                declares(Dialect.class, "registerDefaultKeywords"));
    }

    /** 등록이 실제로 일어나는지 — 메서드만 있고 비어 있으면 의미가 없다. */
    @Test
    public void reservedWords_areActuallyRegistered() {
        final TiberoDialect dialect = new TiberoDialect();
        assertTrue("예약어가 등록돼 있어야 함. 실제 " + dialect.getKeywords().size() + "개",
                dialect.getKeywords().size() >= 71);
        for (String word : new String[]{"least", "flashback", "connect_by_root", "varchar2", "rownum"}) {
            assertTrue("Tibero 고유 예약어 '" + word + "' 가 빠졌다",
                    dialect.getKeywords().contains(word.toUpperCase()) || dialect.getKeywords().contains(word));
        }
    }

    // ------------------------------------------------------------------

    /** {@code type} 이 직접 선언한 메서드만 골라낸다 — 상속은 제외된다. */
    private static List<String> declaredIn(Class<?> type, String[] names) {
        final List<String> found = new ArrayList<>();
        for (String name : names) {
            if (declares(type, name)) {
                found.add(name);
            }
        }
        return found;
    }

    private static boolean declares(Class<?> type, String name) {
        for (Method m : type.getDeclaredMethods()) {
            if (m.getName().equals(name) && m.getParameterCount() == 0) {
                return true;
            }
        }
        return false;
    }
}
