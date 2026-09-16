package capability;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.hibernate.query.sqm.FetchClauseType;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.lang.reflect.Method;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * 페이징 절 중 <b>Tibero 가 받는 것과 못 받는 것</b>의 경계를 고정한다.
 *
 * <h2>세 가지 페이징 절</h2>
 * SQL:2008 은 "몇 건만 가져오기"에 세 가지 변형을 둔다.
 *
 * <pre>
 * fetch first 10 rows only         앞에서 10건.                          ✅ Tibero OK
 * fetch first 10 rows with ties    10건 + <b>마지막과 동점인 행은 다 포함</b>.  ❌ JDBC-3006
 * fetch first 50 percent rows only 전체의 50%.                           ❌ JDBC-3006
 * </pre>
 *
 * <p>{@code with ties} 는 "성적 상위 10명"처럼 <b>경계에서 값이 같으면 잘라내면 안 되는</b>
 * 경우에 쓴다. 10등과 11등의 점수가 같으면 둘 다 넣는다.
 *
 * <h2>지금 상태 — 명시적으로 실패한다</h2>
 * dialect 는 {@code supportsFetchClause()} 로 "ROWS ONLY 만 된다"고 선언해 두었다.
 * 그런데 <b>그 선언을 보고 대신 처리해 주는 장치가 없다.</b>
 *
 * <p>Hibernate 의 {@code AbstractSqlAstTranslator} 는 {@code supportsFetchClause} 를
 * <b>아예 참조하지 않는다.</b> 전체 소스에서 이 값을 에뮬레이션 판단에 쓰는 곳은
 * {@code PostgreSQLSqlAstTranslator} 한 곳뿐이고, 거기서도 <b>자기 번역기에 직접</b>
 * {@code shouldEmulateFetchClause} 훅을 구현해서 켠다.
 *
 * <p>우리는 그 훅이 없다. 그래서 선언은 {@code false} 인데 아무 일도 일어나지 않고,
 * {@code fetch first 2 rows with ties} 가 그대로 나가 Tibero 가 거부한다.
 *
 * <pre>
 * HQL   select i.id from Item i order by i.score fetch first 2 rows with ties
 * SQL   select i1_0.id from FCT_ITEM i1_0 order by i1_0.score fetch first 2 rows with ties
 * 결과  JDBC-3006 Feature not yet implemented
 * </pre>
 *
 * <h2>왜 고치지 않았나</h2>
 * <ul>
 *   <li><b>드러난 실패다.</b> 예외로 즉시 터지므로 조용히 틀린 결과가 나오지 않는다.
 *       이 프로젝트에서 우선순위를 높게 둔 결함들(NLS 형식·락 범위)은 전부
 *       "조용히 어긋나는" 부류였는데 이건 다르다.</li>
 *   <li><b>우회로가 있다.</b> {@code rank()} 윈도우 함수로 같은 의미를 낼 수 있고
 *       Tibero 가 지원한다 — {@link #rankWindowFunction_givesWithTiesSemantics}.</li>
 *   <li><b>Oracle 대비가 아니다.</b> Oracle 은 {@code supportsFetchClause} 가 모든 타입에
 *       {@code true} 다(12c 부터 네이티브 지원). 우리가 에뮬레이션을 넣는다면 Oracle 을
 *       따라가는 게 아니라 PostgreSQL 방식을 새로 들이는 것이다.</li>
 * </ul>
 *
 * <p>그래서 <b>현재 동작을 고정</b>하는 쪽을 택했다. Tibero 가 지원을 추가하면 아래
 * 테스트가 실패하면서 알려 준다 — {@code capability.LateralSupportTest} 와 같은 방식이다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialect#supportsFetchClause(FetchClauseType)
 */
public class FetchClauseTypeTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "FctItem") @Table(name = "FCT_ITEM")
    public static class Item {
        @Id public Long id;
        public Integer score;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Item.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        cfg.setProperty("hibernate.session_factory.statement_inspector",
                SqlCaptureInspector.class.getName());
    }

    /** 점수 1,1,2,2,3 — 경계에서 동점이 생기도록 깔아 둔다. */
    @Before
    public void seed() {
        inTransaction(s -> s.createMutationQuery("delete from FctItem").executeUpdate());
        inTransaction(s -> {
            final int[] scores = {1, 1, 2, 2, 3};
            for (int i = 0; i < scores.length; i++) {
                Item it = new Item();
                it.id = (long) (i + 1);
                it.score = scores[i];
                s.persist(it);
            }
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // 선언
    // ------------------------------------------------------------------

    /** dialect 가 ROWS ONLY 만 지원한다고 선언하는지. */
    @Test
    public void dialectDeclaresRowsOnly() {
        final TiberoDialect dialect = new TiberoDialect();
        assertTrue(dialect.supportsFetchClause(FetchClauseType.ROWS_ONLY));
        assertFalse(dialect.supportsFetchClause(FetchClauseType.ROWS_WITH_TIES));
        assertFalse(dialect.supportsFetchClause(FetchClauseType.PERCENT_ONLY));
        assertFalse(dialect.supportsFetchClause(FetchClauseType.PERCENT_WITH_TIES));
    }

    /**
     * ⚠️ 그 선언이 <b>SQL 생성에는 영향을 주지 않는다</b>는 사실을 고정한다.
     *
     * <p>에뮬레이션을 켜려면 번역기에 {@code shouldEmulateFetchClause} 를 직접 구현해야
     * 한다(PostgreSQL 방식). 우리는 구현하지 않았고, 그래서 선언이 {@code false} 여도
     * 네이티브 문법이 그대로 나간다.
     *
     * <p>이 테스트가 실패하면 누군가 에뮬레이션을 넣었다는 뜻이므로, 아래 "거부된다"
     * 테스트들도 함께 다시 봐야 한다.
     */
    @Test
    public void emulationHookIsDeliberatelyAbsent() {
        boolean declared = false;
        for (Method m : TiberoSqlAstTranslator.class.getDeclaredMethods()) {
            if (m.getName().equals("shouldEmulateFetchClause")) {
                declared = true;
            }
        }
        assertFalse("에뮬레이션 훅을 넣었다면 이 파일의 다른 단언들을 다시 볼 것", declared);
    }

    // ------------------------------------------------------------------
    // 되는 것
    // ------------------------------------------------------------------

    /** ROWS ONLY 는 네이티브로 나가고 정상 동작한다. */
    @Test
    public void rowsOnly_worksNatively() {
        final List<Long> ids = inTransactionReturning(s -> s.createQuery(
                "select i.id from FctItem i order by i.score, i.id", Long.class)
                .setMaxResults(2).getResultList());

        assertEquals(List.of(1L, 2L), ids);
        assertTrue("네이티브 fetch 절이어야 함: " + lastSelect(),
                lastSelect().contains("fetch first ? rows only"));
    }

    // ------------------------------------------------------------------
    // 안 되는 것 — 실패가 좋은 소식인 테스트
    // ------------------------------------------------------------------

    /**
     * ⚠️ <b>이 테스트가 실패하면 좋은 소식이다.</b>
     *
     * <p>Tibero 가 {@code with ties} 를 지원하기 시작했다는 뜻이므로
     * {@code supportsFetchClause} 를 다시 판단해야 한다.
     */
    @Test
    public void withTies_isRejectedByTibero() {
        final String message = expectFailure(
                "select i.id from FctItem i order by i.score fetch first 2 rows with ties");

        assertTrue("Tibero 가 with ties 를 받기 시작했다면 supportsFetchClause 를 다시 판단할 것. "
                + "실제 오류: " + message, message.contains("JDBC-3006"));
        assertTrue("에뮬레이션 없이 네이티브 문법이 그대로 나가야 함: " + lastSelect(),
                lastSelect().contains("with ties"));
    }

    /** PERCENT 도 같다. */
    @Test
    public void percent_isRejectedByTibero() {
        final String message = expectFailure(
                "select i.id from FctItem i order by i.score fetch first 50 percent rows only");

        assertTrue("실제 오류: " + message, message.contains("JDBC-3006"));
        assertTrue("네이티브 문법이 그대로 나가야 함: " + lastSelect(),
                lastSelect().contains("percent"));
    }

    /**
     * {@code order by} 없는 {@code with ties} 는 <b>HQL 파서가 먼저 막는다</b>.
     *
     * <p>전수검사 문서가 "order by 가 없으면 빈 {@code over()} 가 나가 Tibero 가 거부한다"고
     * 적어 두었는데, 그 경우는 <b>HQL 로 도달할 수 없다</b>. 동점을 판정할 기준이 없으니
     * 문법적으로 당연하다. DB 까지 가지도 않는다.
     */
    @Test
    public void withTiesWithoutOrderBy_isRejectedByTheHqlParser() {
        try {
            inTransactionReturning(s -> s.createQuery(
                    "select i.id from FctItem i fetch first 2 rows with ties", Long.class)
                    .getResultList());
            fail("order by 없는 with ties 는 파싱 단계에서 막혀야 함");
        }
        catch (Exception expected) {
            final String m = rootMessage(expected);
            assertTrue("DB 오류가 아니라 HQL 파싱 오류여야 함: " + m,
                    m.contains("mismatched input") || m.contains("fetch"));
            assertFalse("DB 까지 가면 안 됨: " + m, m.contains("JDBC-"));
        }
    }

    // ------------------------------------------------------------------
    // 우회로
    // ------------------------------------------------------------------

    /**
     * {@code rank()} 로 {@code with ties} 와 같은 결과를 낼 수 있다.
     *
     * <p>{@code rank()} 는 동점에 같은 순위를 주므로 {@code rank() <= N} 이 곧
     * "N건 + 경계 동점 포함"이 된다. 점수가 1,1,2,2,3 일 때 상위 1건을 뽑으면
     * 동점인 두 건이 함께 나온다.
     *
     * <p>사용자에게 안내할 우회로라 동작을 함께 고정한다.
     */
    @Test
    public void rankWindowFunction_givesWithTiesSemantics() {
        final List<Long> ids = inTransactionReturning(s -> s.createNativeQuery(
                "select id from (select id, rank() over (order by score) rk from FCT_ITEM) "
                        + "where rk <= 1 order by id", Object.class)
                .getResultList().stream().map(o -> ((Number) o).longValue())
                        .collect(Collectors.toList()));

        assertEquals("동점인 두 건이 함께 나와야 함 — 이것이 with ties 의 의미",
                List.of(1L, 2L), ids);
    }

    /** 윈도우 함수 지원 선언이 위 우회로의 전제다. */
    @Test
    public void windowFunctionsAreSupported() {
        assertTrue(new TiberoDialect().supportsWindowFunctions());
    }

    // ------------------------------------------------------------------

    /** 질의를 돌려 실패시키고 뿌리 메시지를 돌려준다. */
    private String expectFailure(String hql) {
        SqlCaptureInspector.clear();
        try {
            inTransactionReturning(s -> s.createQuery(hql, Long.class).getResultList());
            fail("Tibero 가 받지 않아야 하는 문법인데 통과했다: " + hql);
            return "";
        }
        catch (AssertionError e) {
            throw e;
        }
        catch (Exception expected) {
            return rootMessage(expected);
        }
    }

    private String lastSelect() {
        return SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase().startsWith("select"))
                .reduce((a, b) -> b).orElse("");
    }

    private static String rootMessage(Throwable t) {
        while (t.getCause() != null && t.getCause() != t) {
            t = t.getCause();
        }
        return String.valueOf(t.getMessage());
    }
}
