package render;

import jakarta.persistence.Entity;
import jakarta.persistence.LockModeType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import support.RenderSupport;

import java.util.Collections;

import static org.junit.Assert.*;

/**
 * §5.3 번역기 5건이 <b>어떤 SQL 로 렌더되는지</b> 고정한다 (DB 불필요).
 *
 * <p>실제 실행과 값 정합성은 {@code capability.TranslatorFixesTest} 가 본다.
 * 여기서는 <b>보정이 SQL 문자열에 실제로 나타나는지</b>만 확인한다 — 보정이 조용히
 * 빠지면 Tibero 에서 런타임 오류가 나거나 설정이 무시되는데, 그 회귀를 DB 없이
 * CI 에서 잡기 위한 것이다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator
 */
public class TranslatorFixesRenderTest {

    @Entity(name = "TfxDoc") @Table(name = "TFX_DOC")
    public static class Doc {
        @Id public Long id;
        public Integer a;
        public Integer b;
        public Double d;
        @Lob public String body;
        public String name;
    }

    private static RenderSupport render;

    /** 정수 나눗셈 보정은 옵트인 설정을 켜야 해당 AST 노드가 생성된다 — 아래 §1 참고. */
    private static RenderSupport portableDivision;

    @BeforeClass
    public static void boot() {
        render = RenderSupport.withEntities(Doc.class);
        portableDivision = RenderSupport.withSettings(
                Collections.singletonMap("hibernate.query.hql.portable_integer_division", "true"),
                Doc.class);
    }

    @AfterClass
    public static void shutdown() {
        if (render != null) {
            render.close();
        }
        if (portableDivision != null) {
            portableDivision.close();
        }
    }

    // ------------------------------------------------------------------
    // 1. 정수 나눗셈 — floor 보정
    //
    // Tibero 는 Oracle 과 같이 5/2 를 2.5 로 돌려준다. Hibernate 는 이 차이를
    // 알고 있어서 "정수끼리는 정수로" 를 원하는 사용자에게 옵션을 준다 —
    // hibernate.query.hql.portable_integer_division (기본 false).
    //
    // 옵션을 켜면 SQM 이 DIVIDE_PORTABLE 노드를 만들고, 그것을 floor 로 감싸는
    // 것은 dialect 의 몫이다. 재정의가 없으면 옵션을 켜도 SQL 이 그대로라
    // 설정이 조용히 무시된다 — §3.3 의 저장 프로시저 이름 파라미터와 같은 모양이다.
    // ------------------------------------------------------------------

    /** 옵션을 켜면 정수 나눗셈이 {@code floor} 로 감싸져야 한다. */
    @Test
    public void integerDivision_isWrappedInFloor_whenPortableDivisionEnabled() {
        final String sql = portableDivision.sql("select e.a / e.b from TfxDoc e");
        assertTrue("정수 나눗셈에 floor 가 붙어야 함: " + sql,
                sql.toLowerCase().contains("floor("));
    }

    /** 옵션을 켜도 실수가 섞이면 붙이면 안 된다 — 값이 잘려 오히려 틀려진다. */
    @Test
    public void nonIntegerDivision_isNotWrapped() {
        final String sql = portableDivision.sql("select e.d / e.b from TfxDoc e");
        assertFalse("실수 나눗셈에는 floor 가 붙으면 안 됨: " + sql,
                sql.toLowerCase().contains("floor("));
    }

    /** 기본값(옵션 꺼짐)에서는 Oracle 과 같이 그대로 둔다. */
    @Test
    public void integerDivision_isUntouched_byDefault() {
        final String sql = render.sql("select e.a / e.b from TfxDoc e");
        assertFalse("옵션을 안 켰으면 floor 가 붙으면 안 됨 (Oracle 과 동일): " + sql,
                sql.toLowerCase().contains("floor("));
    }

    /** 나눗셈이 아닌 산술은 손대지 않는다. */
    @Test
    public void otherArithmetic_isUntouched() {
        for (String op : new String[]{"+", "-", "*"}) {
            final String sql = portableDivision.sql("select e.a " + op + " e.b from TfxDoc e");
            assertFalse(op + " 에 floor 가 붙으면 안 됨: " + sql,
                    sql.toLowerCase().contains("floor("));
        }
    }

    // ------------------------------------------------------------------
    // 2. LOB 비교 — dbms_lob.compare
    // ------------------------------------------------------------------

    /**
     * Tibero 는 CLOB 을 {@code =} 로 비교하지 못한다({@code JDBC-11023}).
     * {@code dbms_lob.compare} 가 같으면 0 을 돌려주므로 {@code 0=} 을 앞에 붙인다.
     */
    @Test
    public void lobEquality_usesDbmsLobCompare() {
        final String sql = render.sql("select e.id from TfxDoc e where e.body = 'x'").toLowerCase();
        assertTrue("dbms_lob.compare 로 렌더돼야 함: " + sql, sql.contains("dbms_lob.compare("));
        assertTrue("같음은 0= 이어야 함: " + sql, sql.contains("0=dbms_lob.compare("));
    }

    /**
     * 같지 않음은 {@code 0<>} 다.
     *
     * <p>Oracle 번역기는 여기서 {@code -1=} 을 내는데, {@code compare} 가 앞쪽이 더 크면
     * {@code +1} 을 돌려주므로 그 행을 놓친다. 우리는 {@code 0<>} 로 양쪽을 모두 잡는다.
     */
    @Test
    public void lobInequality_usesZeroNotEqual() {
        final String sql = render.sql("select e.id from TfxDoc e where e.body <> 'x'").toLowerCase();
        assertTrue("같지 않음은 0<> 이어야 함: " + sql, sql.contains("0<>dbms_lob.compare("));
        assertFalse("Oracle 의 -1= 형태를 따라가면 +1 케이스를 놓친다: " + sql,
                sql.contains("-1=dbms_lob.compare("));
    }

    /** 대소 비교는 좌우가 뒤집히므로 부등호도 뒤집힌다. */
    @Test
    public void lobOrdering_flipsTheOperator() {
        assertTrue(render.sql("select e.id from TfxDoc e where e.body < 'x'").toLowerCase()
                .contains("0>dbms_lob.compare("));
        assertTrue(render.sql("select e.id from TfxDoc e where e.body > 'x'").toLowerCase()
                .contains("0<dbms_lob.compare("));
        assertTrue(render.sql("select e.id from TfxDoc e where e.body <= 'x'").toLowerCase()
                .contains("0>=dbms_lob.compare("));
        assertTrue(render.sql("select e.id from TfxDoc e where e.body >= 'x'").toLowerCase()
                .contains("0<=dbms_lob.compare("));
    }

    /** LOB 이 아닌 컬럼은 그대로 {@code =} 로 둔다 — 보정이 과하게 번지면 안 된다. */
    @Test
    public void plainColumnComparison_staysPlain() {
        final String sql = render.sql("select e.id from TfxDoc e where e.name = 'x'").toLowerCase();
        assertFalse("일반 컬럼에 dbms_lob.compare 가 붙으면 안 됨: " + sql,
                sql.contains("dbms_lob.compare"));
    }

    // ------------------------------------------------------------------
    // 3. UNION 가지 안의 order by — offset 0 rows
    // ------------------------------------------------------------------

    /**
     * 집합 연산의 가지에 정렬만 있으면 Tibero 가 문장을 거부한다({@code JDBC-8013}).
     * {@code offset 0 rows} 는 행을 건너뛰지 않으므로 결과는 그대로다.
     */
    @Test
    public void orderByInsideUnionBranch_getsOffsetZeroRows() {
        final String sql = render.sql(
                "(select e.a from TfxDoc e order by e.a) union all (select f.b from TfxDoc f)").toLowerCase();
        assertTrue("union 가지의 order by 뒤에 offset 0 rows 가 붙어야 함: " + sql,
                sql.contains("offset 0 rows"));
    }

    /** 단일 쿼리의 정렬에는 붙지 않는다. */
    @Test
    public void orderByOutsideUnion_isUntouched() {
        final String sql = render.sql("select e.a from TfxDoc e order by e.a").toLowerCase();
        assertFalse("단일 쿼리에는 offset 0 rows 가 붙으면 안 됨: " + sql,
                sql.contains("offset 0 rows"));
    }

    /** 페이징이 걸린 평범한 쿼리는 정상적인 offset/fetch 로 나가야 한다. */
    @Test
    public void normalPaging_stillRendersOffsetFetch() {
        final String sql = render.sqlWithLock(
                "select e.a from TfxDoc e order by e.a", LockModeType.NONE, 5, 10).toLowerCase();
        assertTrue("일반 페이징은 offset ? rows fetch 로 나가야 함: " + sql,
                sql.contains("offset ") && sql.contains("fetch first"));
        assertFalse("일반 페이징에 offset 0 rows 보정이 끼면 안 됨: " + sql,
                sql.contains("offset 0 rows"));
    }
}
