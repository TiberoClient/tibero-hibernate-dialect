package capability;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.util.List;

import static org.junit.Assert.*;

/**
 * jacoco 로 드러난 <b>한 번도 실행되지 않던 경로</b>를 HQL 로 덮는다.
 *
 * <h2>왜 커버리지를 봤나</h2>
 * 이 프로젝트에서 커버리지 수치 자체는 목표가 아니다. 의미 있는 질문은 "몇 %인가"가 아니라
 * <b>"우리가 직접 재정의한 훅 중 아직 한 번도 안 돌아본 것이 무엇인가"</b> 다.
 * 재정의는 했는데 실행된 적이 없다면, 그 코드가 맞는지 아무도 모르는 상태다.
 *
 * <p>실제로 리포트를 보고 <b>세 가지</b>가 드러났다.
 *
 * <pre>
 * ① 재귀 CTE 번역기 훅 3개가 미실행
 *    → 기존 테스트가 HQL 이 못 받는 문법을 쓰고 예외를 삼켜 조용히 통과하고 있었다
 *
 * ② visitValuesTableReference · renderMergeUpdateClause 미실행
 *    → §5.3 에서 추가한 오버라이드인데 이를 타는 HQL 을 아무도 쓰지 않았다
 *
 * ③ getQuerySequencesString 미실행
 * </pre>
 *
 * <p>①은 {@code render.RenderContractTest} 에서 고쳤고, ②③을 여기서 덮는다.
 */
public class HqlCoverageMatrixTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "HcmItem") @Table(name = "HCM_ITEM")
    public static class Item {
        @Id public Long id;
        public String v;
        public Integer n;
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
        inTransaction(s -> s.createMutationQuery("delete from HcmItem").executeUpdate());
        inTransaction(s -> {
            Item i = new Item();
            i.id = 1L;
            i.v = "orig";
            i.n = 1;
            s.persist(i);
        });
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // ② upsert — MERGE 에뮬레이션 경로
    // ------------------------------------------------------------------

    /**
     * {@code insert … on conflict do update} 가 MERGE 로 번역되는지.
     *
     * <p>Tibero 에는 {@code on conflict} 문법이 없으므로 Hibernate 가 MERGE 로 에뮬레이션한다.
     * 이때 <b>VALUES 목록을 테이블처럼 참조</b>해야 하는데 Tibero 는 {@code (values (…))} 표기를
     * 받지 않는다({@code JDBC-8013}). 그래서 {@code visitValuesTableReference} 가
     * {@code select … from dual} 로 바꿔 준다.
     *
     * <pre>
     * insert into HcmItem (id,v) values (1,'new') on conflict(id) do update set v='updated'
     *   ↓
     * merge into HCM_ITEM i1_0
     *  using (select 1 id,'new' v from dual) excluded on (i1_0.id=excluded.id)
     *   when matched then update set i1_0.v='updated'
     *   when not matched then insert (id,v) values (excluded.id,excluded.v)
     * </pre>
     */
    @Test
    public void insertOnConflictDoUpdate_translatesToMerge() {
        inTransaction(s -> s.createMutationQuery(
                "insert into HcmItem (id,v,n) values (1,'new',9) "
                        + "on conflict(id) do update set v='updated'").executeUpdate());

        final String sql = mergeSql();
        assertTrue("MERGE 로 번역돼야 함: " + sql, sql.startsWith("merge into"));
        assertTrue("VALUES 대신 select … from dual 을 써야 함: " + sql,
                sql.contains("from dual") && !sql.contains("(values ("));
        assertTrue("matched 절이 있어야 함: " + sql, sql.contains("when matched then update set"));
        assertTrue("not matched 절이 있어야 함: " + sql, sql.contains("when not matched then insert"));

        inTransaction(s -> assertEquals("기존 행이 갱신돼야 함", "updated",
                s.createQuery("select i.v from HcmItem i where i.id=1", String.class).getSingleResult()));
    }

    /** 없는 키면 insert 쪽으로 간다 — MERGE 의 두 갈래가 다 도는지. */
    @Test
    public void insertOnConflict_insertsWhenKeyIsAbsent() {
        inTransaction(s -> s.createMutationQuery(
                "insert into HcmItem (id,v,n) values (2,'two',2) "
                        + "on conflict(id) do update set v='never'").executeUpdate());

        inTransaction(s -> assertEquals("새 키는 그대로 삽입", "two",
                s.createQuery("select i.v from HcmItem i where i.id=2", String.class).getSingleResult()));
    }

    /**
     * {@code do nothing} 은 MERGE 로 가지 않는다 — 평문 INSERT 로 둔다.
     *
     * <p>unique 위반이 나면 그냥 실패하게 두는 편이 MERGE 로 감싸는 것보다 싸다.
     * {@code TiberoSqlAstTranslator.visitInsertStatementOnly} 의 판단이다.
     */
    @Test
    public void insertOnConflictDoNothing_staysAPlainInsert() {
        SqlCaptureInspector.clear();
        inTransaction(s -> s.createMutationQuery(
                "insert into HcmItem (id,v,n) values (3,'three',3) on conflict do nothing")
                .executeUpdate());

        final String sql = SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase().contains("hcm_item"))
                .findFirst().orElse("");
        assertTrue("평문 INSERT 여야 함: " + sql, sql.toLowerCase().startsWith("insert into"));
        assertFalse("MERGE 로 감싸면 안 됨: " + sql, sql.toLowerCase().contains("merge"));
    }

    // ------------------------------------------------------------------
    // ① 재귀 CTE — 실제로 실행까지
    // ------------------------------------------------------------------

    /**
     * 재귀 CTE 가 <b>돌고 값도 맞는지</b>.
     *
     * <p>{@code render.RenderContractTest} 는 SQL 문자열만 보므로 실행까지 확인한다.
     * HQL 은 {@code with rec as (…)} 형태만 받는다 — 컬럼 목록을 붙인
     * {@code with rec (n) as (…)} 는 파싱 단계에서 거부된다.
     */
    @Test
    public void recursiveCte_runsAndProducesRows() {
        final List<Integer> ns = inTransactionReturning(s -> s.createQuery(
                "with rec as ("
                        + "  select 1 as n from HcmItem i where i.id = 1"
                        + "  union all"
                        + "  select r.n + 1 as n from rec r where r.n < 3"
                        + ") select r.n from rec r order by r.n", Integer.class).getResultList());

        assertEquals("1,2,3 이 나와야 함", List.of(1, 2, 3), ns);
    }

    /** 생성된 SQL 에 {@code recursive} 키워드가 없어야 한다 — Tibero 는 그 키워드를 안 쓴다. */
    @Test
    public void recursiveCte_doesNotEmitRecursiveKeyword() {
        SqlCaptureInspector.clear();
        inTransactionReturning(s -> s.createQuery(
                "with rec as (select 1 as n from HcmItem i where i.id=1"
                        + " union all select r.n+1 as n from rec r where r.n<3)"
                        + " select r.n from rec r", Integer.class).getResultList());

        final String sql = SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase().contains("with "))
                .findFirst().orElseThrow(() -> new AssertionError("with 절 SQL 을 못 찾음"));
        assertFalse("recursive 키워드가 붙으면 Tibero 가 거부한다: " + sql,
                sql.toLowerCase().contains("with recursive"));
    }

    // ------------------------------------------------------------------
    // ③ 시퀀스 목록 조회
    // ------------------------------------------------------------------

    /** {@code getQuerySequencesString()} 이 실제로 도는 SQL 인지. */
    @Test
    public void querySequencesString_actuallyRuns() {
        final String sql = new TiberoDialect().getQuerySequencesString();
        assertNotNull(sql);

        final String seq = uniqueObjectName("HCM_SEQ");
        inTransaction(s -> s.createNativeMutationQuery("create sequence " + seq).executeUpdate());
        try {
            final long found = inTransactionReturning(s -> ((Number) s.createNativeQuery(
                    "select count(*) from (" + sql + ") where sequence_name = '" + seq + "'", Object.class)
                    .getSingleResult()).longValue());
            assertEquals("방금 만든 시퀀스가 목록에 있어야 함", 1L, found);
        }
        finally {
            dropSequenceWithRetry(seq);
        }
    }

    // ------------------------------------------------------------------

    private String mergeSql() {
        return SqlCaptureInspector.getSqls().stream()
                .filter(x -> x.toLowerCase().startsWith("merge"))
                .findFirst().orElseThrow(() -> new AssertionError(
                        "MERGE 문을 못 찾음: " + SqlCaptureInspector.getSqls()));
    }
}
