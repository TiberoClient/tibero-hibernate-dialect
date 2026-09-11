package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * 다중 키 배치 로딩이 <b>여러 문장으로 나뉘는지</b> 본다.
 *
 * <h2>어떤 문제인가</h2>
 * Tibero 는 IN 원소 개수에 한도가 없다 — 바인드를 50,000개 넣어도 실행된다. 그래서
 * {@code getInExpressionCountLimit()} 은 {@code 0}(무제한)이 맞다.
 *
 * <p>하지만 <b>서버 파스 시간이 원소 수의 제곱으로 늘어난다.</b> 10,000개면 15.6초,
 * 30,000개면 138초다(ps06 실측). 실행이 아니라 <b>파스</b>가 비싼 것이라, 값 바인딩을
 * 아무리 최적화해도 줄지 않는다.
 *
 * <pre>
 * 3,000키 byMultipleIds
 *   나누지 않으면   select 1개 (? 3,000개)   1,411 ms
 *   500씩 나누면    select 6개 (? 500개)       ~50 ms
 * </pre>
 *
 * <p>이걸 고치는 것은 이름이 비슷한 {@code getInExpressionCountLimit} 이 <b>아니라</b>
 * {@code getParameterCountLimit} 이다. 앞엣것은 한 문장 안에서 {@code or} 로 이어붙일
 * 뿐이고, 뒤엣것만 실제로 문장을 나눈다. 이 테스트가 그 구분을 고정한다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialect#getParameterCountLimit()
 */
public class BatchLoadSplitTest extends AbstractTiberoDialectTestBase {

    /** 배치 크기 500 을 넘도록 넉넉히 넣는다. */
    private static final int ROWS = 1_200;

    @Entity(name = "BlsItem") @Table(name = "BLS_ITEM")
    public static class Item {
        @Id public Long id;
        public String v;
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

    @Before
    public void seed() {
        inTransaction(s -> s.createMutationQuery("delete from BlsItem").executeUpdate());
        inTransaction(s -> {
            for (int i = 1; i <= ROWS; i++) {
                Item it = new Item();
                it.id = (long) i;
                it.v = "v" + i;
                s.persist(it);
            }
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------

    /** 배치 크기가 500 으로 선언돼 있는지 — 값이 바뀌면 아래 단언들의 전제가 무너진다. */
    @Test
    public void parameterCountLimitIs500() {
        final Dialect d = new TiberoDialect();
        assertEquals("배치 로딩을 500키씩 나눈다", 500, d.getParameterCountLimit());
        assertEquals("IN 원소 수는 여전히 무제한 — 둘은 다른 값이다",
                0, d.getInExpressionCountLimit());
    }

    /**
     * 500 을 넘는 키로 {@code byMultipleIds} 를 하면 문장이 나뉜다.
     *
     * <p>오버라이드가 빠지면 한 문장에 1,200개가 전부 들어가고, 파스 비용이
     * {@code 1200² × 1.55e-4 ≈ 220ms} 로 뛴다. 키가 많아질수록 제곱으로 나빠진다.
     */
    @Test
    public void multiLoad_isSplitIntoSeveralStatements() {
        final List<Long> ids = new ArrayList<>();
        for (long i = 1; i <= ROWS; i++) {
            ids.add(i);
        }

        inTransaction(s -> assertEquals(ROWS, s.byMultipleIds(Item.class).multiLoad(ids).size()));

        final List<String> selects = selectsWithBinds();
        assertTrue("1,200키면 500씩 나뉘어 여러 문장이어야 함. 실제 " + selects.size() + "개",
                selects.size() >= 2);

        for (String sql : selects) {
            final long binds = sql.chars().filter(c -> c == '?').count();
            assertTrue("한 문장의 바인드가 500 을 넘으면 안 됨: " + binds, binds <= 500);
        }
    }

    /**
     * 500 이하면 나누지 않는다 — 작은 조회에 불필요한 왕복을 넣지 않는다.
     *
     * <p>배치 분할이 항상 이득인 것은 아니다. 파스가 싼 크기에서는 문장을 나눌수록
     * 왕복만 늘어난다.
     */
    @Test
    public void smallMultiLoad_staysAsOneStatement() {
        final List<Long> ids = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            ids.add(i);
        }

        inTransaction(s -> assertEquals(100, s.byMultipleIds(Item.class).multiLoad(ids).size()));

        assertEquals("100키는 한 문장으로 나가야 함", 1, selectsWithBinds().size());
    }

    /**
     * 값이 맞는지 — 나눠 읽어도 결과가 온전해야 한다.
     *
     * <p>분할은 성능 최적화지 의미를 바꾸는 것이 아니다. 경계에서 행이 빠지거나
     * 중복되지 않는지 확인한다.
     */
    @Test
    public void splitLoad_returnsEveryRowExactlyOnce() {
        final List<Long> ids = new ArrayList<>();
        for (long i = 1; i <= ROWS; i++) {
            ids.add(i);
        }

        inTransaction(s -> {
            final List<Item> loaded = s.byMultipleIds(Item.class).multiLoad(ids);
            assertEquals(ROWS, loaded.size());
            for (int i = 0; i < ROWS; i++) {
                assertEquals("순서가 요청한 id 순서와 같아야 함",
                        Long.valueOf(i + 1L), loaded.get(i).id);
                assertEquals("v" + (i + 1), loaded.get(i).v);
            }
        });
    }

    /**
     * HQL 의 {@code in :list} 는 <b>나뉘지 않는다</b> — 한계를 명시한다.
     *
     * <p>{@code getParameterCountLimit} 은 배치 로딩 경로에만 쓰인다. HQL 로 큰 리스트를
     * 넘기면 여전히 한 문장이고 파스 비용을 그대로 문다. 사용자가 알아야 할 사실이라
     * 테스트로 남긴다.
     */
    @Test
    public void hqlInList_isNotSplit() {
        final List<Long> ids = new ArrayList<>();
        for (long i = 1; i <= ROWS; i++) {
            ids.add(i);
        }

        inTransaction(s -> assertEquals(ROWS, s.createQuery(
                        "select i.id from BlsItem i where i.id in :ids", Long.class)
                .setParameter("ids", ids).getResultList().size()));

        final List<String> selects = selectsWithBinds();
        assertEquals("HQL IN 은 한 문장으로 나간다 (배치 로딩과 다른 경로)", 1, selects.size());
        assertEquals("바인드가 전부 한 문장에 들어간다", ROWS,
                selects.get(0).chars().filter(c -> c == '?').count());
    }

    // ------------------------------------------------------------------

    /** 바인드가 있는 select 만 — 시드/정리 문장을 걸러낸다. */
    private List<String> selectsWithBinds() {
        final List<String> out = new ArrayList<>();
        for (String sql : SqlCaptureInspector.getSqls()) {
            final String lower = sql.toLowerCase();
            if (lower.startsWith("select") && sql.indexOf('?') >= 0) {
                out.add(sql);
            }
        }
        SqlCaptureInspector.clear();
        return out;
    }
}
