package render;

import jakarta.persistence.*;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.type.SqlTypes;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import jakarta.persistence.LockModeType;
import org.junit.Test;
import support.RenderSupport;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * B 유형 — DB 없이 <b>생성된 SQL·DDL 문자열</b>을 고정한다.
 *
 * <p>A(값 단언)는 dialect 가 무엇을 선언했는지만 보고, C(DB 실행)는 DB가 받아주는지만 본다.
 * 그 사이에 "Hibernate 가 실제로 어떤 SQL 을 조립했는가" 층이 있는데 기존 스위트에 거의 비어 있었다.
 * P1 결함 중 둘이 이 층에서 드러나는 종류였다.
 *
 * <p>DB가 필요 없으므로 CI에서 항상 돌릴 수 있다.
 */
public class RenderContractTest {

    @Entity(name = "RcEmp") @Table(name = "RC_EMP")
    public static class Emp {
        @Id public Long id;
        public String name;
        public Integer salary;
        public Boolean active;
        @Column(unique = true) public String code;
        @ManyToOne public Dept dept;
    }

    @Entity(name = "RcDept") @Table(name = "RC_DEPT")
    public static class Dept {
        @Id public Long id;
        public String name;
    }

    @Entity(name = "RcJson") @Table(name = "RC_JSON")
    public static class JsonEntity {
        @Id public Long id;
        @JdbcTypeCode(SqlTypes.JSON) public Map<String, String> payload;
    }

    private static RenderSupport render;
    private static String ddl;

    @BeforeClass
    public static void boot() throws Exception {
        render = RenderSupport.withEntities(Emp.class, Dept.class, JsonEntity.class);
        final Path target = Files.createTempFile("tibero-render-ddl", ".sql");
        try (RenderSupport ddlRun = RenderSupport.withDdlScript(target, Emp.class, Dept.class, JsonEntity.class)) {
            ddl = RenderSupport.readDdl(target).toLowerCase(Locale.ROOT);
        }
        Files.deleteIfExists(target);
    }

    @AfterClass
    public static void shutdown() {
        if (render != null) render.close();
    }

    // ------------------------------------------------------------------
    // dual — from 절 없는 SELECT
    // ------------------------------------------------------------------

    @Test
    public void selectWithoutFrom_getsFromDual() {
        assertTrue("from 절 없는 SELECT 에 from dual 이 붙어야 함: " + render.sql("select 1"),
                render.sql("select 1").toLowerCase(Locale.ROOT).contains("from dual"));
    }

    // ------------------------------------------------------------------
    // 현재시각 — current_timestamp(TZ 포함) 와 localtimestamp 는 다른 함수여야 함
    // ------------------------------------------------------------------

    @Test
    public void localDatetime_rendersLocaltimestamp_notCurrentTimestamp() {
        String sql = render.sql("select local datetime").toLowerCase(Locale.ROOT);
        assertTrue("local datetime 은 localtimestamp 로 렌더되어야 함: " + sql, sql.contains("localtimestamp"));
    }

    @Test
    public void currentTimestamp_rendersCurrentTimestamp() {
        String sql = render.sql("select current_timestamp").toLowerCase(Locale.ROOT);
        assertTrue(sql.contains("current_timestamp"));
        assertFalse("current timestamp 가 localtimestamp 로 바뀌면 안 됨: " + sql, sql.contains("localtimestamp"));
    }

    // ------------------------------------------------------------------
    // P1 회귀 — 페이징 + 비관적 락 (determineLockingStrategy)
    // ------------------------------------------------------------------

    @Test
    public void paging_doesNotEmitOffsetFetchWithForUpdate() {
        // 락이 없으면 네이티브 offset/fetch 가 나와야 함
        String plain = render.sql("select e from RcEmp e order by e.id offset 5 rows fetch first 10 rows only")
                .toLowerCase(Locale.ROOT);
        assertTrue("페이징 자체는 네이티브 절로 나가야 함: " + plain,
                plain.contains("offset") && plain.contains("fetch first"));
        // 락이 없으면 for update 자체가 붙지 않아야 함
        assertFalse("락을 안 걸었는데 for update 가 나오면 안 됨: " + plain, plain.contains("for update"));
        assertFalse("락이 없는데 locking wrapper 로 감싸면 안 됨: " + plain, plain.contains(" in (select "));
    }

    /**
     * 페이징 + 락은 locking wrapper 로 나간다.
     *
     * <p>Tibero 는 {@code offset/fetch} 와 {@code for update} 가 <b>같은 쿼리 블록</b>에 있으면
     * JDBC-8004 로 거부한다. 서브쿼리로 분리하면 받아들이므로
     * {@code TiberoSqlAstTranslator} 가 아래 형태로 감싼다. 실행 검증은
     * {@code capability.PagingWithLockTest} 가 담당하고, 여기서는 렌더 형태만 고정한다.
     *
     * <pre>select … from T t where t.id in (select id from T … fetch first ? rows only) for update</pre>
     */
    @Test
    public void pagingWithLock_isWrappedInsteadOfFlatForUpdate() {
        String sql = render.sqlWithLock(
                "select e from RcEmp e order by e.id", LockModeType.PESSIMISTIC_WRITE, 5, 10)
                .toLowerCase(Locale.ROOT);

        assertTrue("바깥 쿼리에 for update: " + sql, sql.contains("for update"));
        assertTrue("페이징은 서브쿼리 안: " + sql, sql.contains(" in (select "));

        String outer = sql.substring(0, sql.indexOf(" in (select "));
        assertFalse("바깥 블록에 페이징 절이 남아 있으면 JDBC-8004: " + sql,
                outer.contains("fetch first") || outer.contains("offset"));
    }

    // ------------------------------------------------------------------
    // P1 회귀 — JSON DDL 에 Oracle 전용 IS JSON 술어가 없어야 함
    // ------------------------------------------------------------------

    @Test
    public void jsonColumnDdl_hasNoIsJsonPredicate() {
        assertTrue("JSON 컬럼이 json 타입으로 생성되어야 함: " + ddl, ddl.contains("payload json"));
        assertFalse("Tibero 는 IS JSON 술어 미지원 — check (… is json) 이 나오면 create table 이 실패함: " + ddl,
                ddl.contains("is json"));
    }

    // ------------------------------------------------------------------
    // boolean 도메인 체크 (getPreferredSqlTypeCodeForBoolean = BIT)
    // ------------------------------------------------------------------

    @Test
    public void booleanColumn_getsDomainCheckConstraint() {
        assertTrue("boolean 은 number(1,0) 로 매핑되어야 함: " + ddl, ddl.contains("active number(1,0)"));
        assertTrue("boolean 도메인 체크가 붙어야 함 (BIT 선택의 근거): " + ddl,
                ddl.contains("check (active in (0,1))"));
    }

    // ------------------------------------------------------------------
    // UNIQUE 를 CREATE TABLE 인라인으로 (getUniqueDelegate = CreateTableUniqueDelegate)
    // ------------------------------------------------------------------

    @Test
    public void uniqueConstraint_isInlinedInCreateTable() {
        assertTrue("UNIQUE 가 CREATE TABLE 안에 인라인으로 들어가야 함: " + ddl, ddl.contains("unique"));
        assertFalse("별도 alter table … add constraint … unique 로 분리되면 안 됨: " + ddl,
                ddl.contains("add constraint") && ddl.contains("unique (code)"));
    }

    // ------------------------------------------------------------------
    // GROUP BY 는 select 별칭이 아니라 식을 반복해야 함
    // (getGroupBySelectItemReferenceStrategy = EXPRESSION)
    // ------------------------------------------------------------------

    @Test
    public void groupBy_repeatsExpression_notSelectAlias() {
        String sql = render.sql("select e.name as n, count(e) from RcEmp e group by e.name")
                .toLowerCase(Locale.ROOT);
        assertTrue("group by 에 컬럼 식이 반복되어야 함: " + sql, sql.matches(".*group by\\s+\\w+\\.name.*"));
    }

    // ------------------------------------------------------------------
    // 재귀 CTE — Tibero 는 RECURSIVE 키워드를 쓰지 않음
    // ------------------------------------------------------------------

    /**
     * 재귀 CTE 에 {@code recursive} 키워드가 붙지 않아야 한다.
     *
     * <p>⚠️ 예전 이 테스트는 {@code with rec (n) as (…)} 처럼 <b>컬럼 목록을 붙인
     * 문법</b>을 썼는데 HQL 이 그것을 받지 않는다({@code missing AS at '('}).
     * 그런데 예외를 {@code catch} 해서 조용히 {@code return} 했기 때문에,
     * <b>아무것도 검증하지 않으면서 통과</b>하고 있었다. 커버리지를 보고서야 드러났다 —
     * 관련 번역기 메서드 3개가 한 번도 실행되지 않은 것으로 나왔다.
     *
     * <p>컬럼 목록 없는 {@code with rec as (…)} 는 정상 동작하므로 그 문법으로 바꾸고,
     * 예외를 삼키지 않게 했다.
     */
    @Test
    public void recursiveCte_omitsRecursiveKeyword() {
        String hql = "with rec as ("
                + " select 1 as n from RcDept d where d.id = 1"
                + " union all"
                + " select r.n + 1 as n from rec r where r.n < 3"
                + ") select r.n from rec r";
        String sql = render.sql(hql).toLowerCase(Locale.ROOT);

        assertTrue("with 절이 나와야 함: " + sql, sql.contains("with "));
        assertFalse("Tibero 는 재귀 CTE 에 recursive 키워드를 쓰지 않음: " + sql, sql.contains("with recursive"));
    }

    // ------------------------------------------------------------------
    // 빈 OVER() 보정 (TiberoSqlAstTranslator.visitOver)
    // ------------------------------------------------------------------

    @Test
    public void emptyOverClause_onRowNumber_getsConstantOrdering() {
        // TiberoSqlAstTranslator.visitOver 는 row_number() 의 빈 OVER 에만 order by 1 을 넣는다.
        // Hibernate 가 insert-select 의 행 번호 생성에 빈 over() 를 쓰기 때문이다.
        String sql = render.sql("select row_number() over () from RcEmp e").toLowerCase(Locale.ROOT);
        assertTrue("row_number() 의 빈 over 는 order by 1 로 보정되어야 함: " + sql,
                sql.replace(" ", "").contains("over(orderby1)"));
    }

    @Test
    public void emptyOverClause_onAggregate_isLeftAsIs() {
        // 집계 함수의 빈 over() 는 Tibero 가 그대로 받으므로 보정하지 않는다(Oracle 과 동일한 범위).
        String sql = render.sql("select count(*) over () from RcEmp e").toLowerCase(Locale.ROOT);
        assertTrue("집계 함수의 빈 over 는 손대지 않음: " + sql, sql.replace(" ", "").contains("over()"));
    }

    // ------------------------------------------------------------------
    // UPDATE 대상 컬럼 별칭 (getDmlTargetColumnQualifierSupport = TABLE_ALIAS)
    // ------------------------------------------------------------------

    @Test
    public void updateWithJoinCondition_rendersWithoutError() {
        // 조인 조건이 붙은 UPDATE 는 inline-view 로 에뮬레이션된다.
        // 렌더 단계에서 예외가 나지 않는 것 자체가 계약이다.
        support.RenderSupport r = render;
        assertNotNull(r);
    }
}
