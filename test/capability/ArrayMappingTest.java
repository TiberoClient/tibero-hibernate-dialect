package capability;

import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Array;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.util.List;

import static org.junit.Assert.*;

/**
 * 배열 필드를 Tibero 의 <b>네이티브 VARRAY 컬럼</b>에 매핑한다.
 *
 * <h2>이전 동작과의 차이</h2>
 * 6.6.0 은 배열을 {@code VARBINARY} 로 직렬화해 하나의 이진 컬럼에 담았다. 왕복은 되지만
 * DB 에서 배열로 다룰 수 없어 {@code table()} 언네스트도, {@code array_*} 함수도 못 썼다.
 *
 * <pre>
 * 6.6.0        int[] nums  ->  NUMS raw(255)        (직렬화된 덩어리)
 * 6.6.1        int[] nums  ->  NUMS INTEGERARRAY    (varying array(n) of number)
 * </pre>
 *
 * @see com.tmax.tibero.hibernate.type.TiberoArrayJdbcType
 * @see com.tmax.tibero.hibernate.tool.schema.internal.TiberoUserDefinedTypeExporter
 */
public class ArrayMappingTest extends AbstractTiberoDialectTestBase {

    @Entity(name = "AmtDoc") @Table(name = "AMT_DOC")
    public static class Doc {
        @Id public Long id;
        @Array(length = 10) public String[] tags;
        @Array(length = 5) public Integer[] nums;
        /** 상한 미지정 — 기본 127 이 적용되어야 한다 */
        public Long[] refs;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Doc.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    @Before
    public void clean() {
        inTransaction(s -> s.createMutationQuery("delete from AmtDoc").executeUpdate());
    }

    // ------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------

    @Test
    public void ddl_createsVarrayTypes_withDeclaredBounds() {
        assertEquals("String[] -> StringArray", 1L, count(
                "select count(*) from user_coll_types "
                        + "where type_name='STRINGARRAY' and coll_type='VARYING ARRAY' and upper_bound=10"));
        assertEquals("Integer[] -> IntegerArray", 1L, count(
                "select count(*) from user_coll_types "
                        + "where type_name='INTEGERARRAY' and coll_type='VARYING ARRAY' and upper_bound=5"));
        assertEquals("@Array 없으면 상한 127 (Oracle 과 같은 기본값)", 1L, count(
                "select count(*) from user_coll_types "
                        + "where type_name='LONGARRAY' and coll_type='VARYING ARRAY' and upper_bound=127"));
    }

    @Test
    public void ddl_columnTypeIsTheVarrayType_notBinary() {
        assertEquals(1L, count("select count(*) from user_tab_columns "
                + "where table_name='AMT_DOC' and column_name='TAGS' and data_type='STRINGARRAY'"));
        assertEquals(1L, count("select count(*) from user_tab_columns "
                + "where table_name='AMT_DOC' and column_name='NUMS' and data_type='INTEGERARRAY'"));
    }



    // ------------------------------------------------------------------
    // 값 왕복
    // ------------------------------------------------------------------

    @Test
    public void roundTrip_values() {
        inTransaction(s -> {
            Doc d = new Doc(); d.id = 1L;
            d.tags = new String[]{"a", "b", "c"};
            d.nums = new Integer[]{1, 2, 3};
            d.refs = new Long[]{10L, 20L};
            s.persist(d);
        });
        inTransaction(s -> {
            Doc d = s.find(Doc.class, 1L);
            assertArrayEquals(new String[]{"a", "b", "c"}, d.tags);
            assertArrayEquals(new Integer[]{1, 2, 3}, d.nums);
            assertArrayEquals(new Long[]{10L, 20L}, d.refs);
        });
    }

    /**
     * {@code null} 배열.
     *
     * <p>기본 바인더는 {@code setNull(i, Types.ARRAY)} 를 부르는데 tbjdbc 가
     * {@code JDBC-590703 Unsupported data type. - VARRAY} 로 거부한다.
     */
    @Test
    public void roundTrip_nullArray() {
        inTransaction(s -> {
            Doc d = new Doc(); d.id = 2L; d.tags = null; d.nums = null; d.refs = null;
            s.persist(d);
        });
        inTransaction(s -> {
            Doc d = s.find(Doc.class, 2L);
            assertNull(d.tags);
            assertNull(d.nums);
        });
    }

    /**
     * <b>빈</b> 배열은 null 과 구분되어야 한다.
     *
     * <p>tbjdbc 는 원소가 없는 VARRAY 를 읽을 때 {@code Array.getArray()} 로
     * {@code null} 을 돌려준다. 그대로 두면 Hibernate 가 NPE 를 던지고, 빈 배열이
     * null 로 뭉개지면 데이터 의미가 바뀐다.
     */
    @Test
    public void roundTrip_emptyArray_isNotNull() {
        inTransaction(s -> {
            Doc d = new Doc(); d.id = 3L;
            d.tags = new String[0]; d.nums = new Integer[0];
            s.persist(d);
        });
        inTransaction(s -> {
            Doc d = s.find(Doc.class, 3L);
            assertNotNull("빈 배열이 null 로 뭉개지면 안 됨", d.tags);
            assertEquals(0, d.tags.length);
            assertNotNull(d.nums);
            assertEquals(0, d.nums.length);
        });
    }

    @Test
    public void roundTrip_nullElementsInsideArray() {
        inTransaction(s -> {
            Doc d = new Doc(); d.id = 4L;
            d.tags = new String[]{"a", null, "c"};
            s.persist(d);
        });
        inTransaction(s -> assertArrayEquals(new String[]{"a", null, "c"}, s.find(Doc.class, 4L).tags));
    }

    // ------------------------------------------------------------------
    // DB 가 배열로 다룰 수 있는지 — VARBINARY 였으면 불가능한 것들
    // ------------------------------------------------------------------

    /** VARRAY 이므로 SQL 에서 언네스트가 된다. VARBINARY 덩어리였으면 불가능하다. */
    @Test
    public void nativeArray_canBeUnnestedInSql() {
        seed(10L, "a", "b", "c");
        assertEquals(3L, count(
                "select count(*) from AMT_DOC d, table(d.tags) t where d.id=10"));
        assertEquals("b", inTransactionReturning(s -> s.createNativeQuery(
                "select t.column_value from AMT_DOC d, table(d.tags) t where d.id=10 and t.column_value='b'",
                String.class).getSingleResult()));
    }

    // ------------------------------------------------------------------
    // HQL array_* 함수 — 의도적으로 등록하지 않았다
    // ------------------------------------------------------------------

    /**
     * {@code array_*} HQL 함수는 <b>하나도 등록하지 않았다</b>.
     *
     * <p>Hibernate 의 Oracle 변종은 배열 타입마다 만들어지는 PL/SQL 헬퍼를 호출하는데,
     * 그 헬퍼를 Tibero 에 적용하자 <b>DB 가 반복적으로 응답 불능</b>에 빠졌다.
     * 특히 {@code <타입>_concat} 은 <b>두 번째 호출부터 서버 워커가 멈추고</b>
     * 인스턴스 재기동으로만 풀렸다.
     *
     * <pre>
     * 1회차 select T_length(T_concat(T('a'), T('b'))) from dual   ->  2  (9ms)
     * 2회차 같은 문장                                              ->  응답 없음 (500s+)
     * </pre>
     *
     * <p>애플리케이션에 노출하면 DB 를 멈추게 하는 종류의 고장이라, 등록하지 않아
     * <b>HQL 파싱 단계</b>에서 걸리게 했다. 배열 컬럼 매핑·왕복은 이와 무관하게 동작하고,
     * 네이티브 SQL 의 {@code table()} 언네스트로 같은 일을 할 수 있다.
     *
     * <p>헬퍼가 고쳐져 등록할 수 있게 되면 이 테스트를 지원 시나리오로 교체할 것.
     *
     * @see com.tmax.tibero.hibernate.tool.schema.internal.TiberoUserDefinedTypeExporter
     */
    @Test
    public void hql_arrayFunctions_areNotRegistered() {
        seed(30L, "a", "b", "c");
        for (String hql : new String[]{
                "select array_length(d.tags) from AmtDoc d where d.id=30",
                "select array_get(d.tags, 1) from AmtDoc d where d.id=30",
                "select array_position(d.tags, 'b') from AmtDoc d where d.id=30",
                "select array_to_string(d.tags, ',') from AmtDoc d where d.id=30",
                "select array_concat(d.tags, d.tags) from AmtDoc d where d.id=30",
                "select array_remove(d.tags, 'b') from AmtDoc d where d.id=30"}) {
            try {
                list(hql, Object.class);
                fail("등록되지 않아야 할 함수가 통과함: " + hql
                        + " — PL/SQL 헬퍼가 안정화되어 등록했다면 이 테스트를 교체할 것");
            } catch (Exception expected) {
                // 파싱 단계에서 미등록으로 걸린다
            }
        }
    }

    /** PL/SQL 헬퍼는 <b>하나도</b> 만들어지지 않아야 한다 — 만들어지면 DB 가 멈출 수 있다. */
    @Test
    public void ddl_noPlsqlHelpersAreCreated() {
        final java.util.List<?> names = inTransactionReturning(s -> s.createNativeQuery(
                "select object_type||' '||object_name from user_objects "
                        + "where object_name like '%ARRAY%' order by 1", Object.class).getResultList());
        assertEquals("배열 헬퍼 PL/SQL 이 생성되면 안 됨. 실제: " + names, 0L, count(
                "select count(*) from user_objects where object_type='FUNCTION' "
                        + "and (object_name like 'STRINGARRAY!_%' escape '!' "
                        + "  or object_name like 'INTEGERARRAY!_%' escape '!' "
                        + "  or object_name like 'LONGARRAY!_%' escape '!')"));
    }

    // ------------------------------------------------------------------

    private void seed(long id, String... tags) {
        inTransaction(s -> {
            Doc d = new Doc(); d.id = id; d.tags = tags;
            s.persist(d);
        });
    }

    private <T> T one(String hql, Class<T> type) {
        return inTransactionReturning(s -> s.createQuery(hql, type).getSingleResult());
    }

    private <T> List<T> list(String hql, Class<T> type) {
        return inTransactionReturning(s -> s.createQuery(hql, type).getResultList());
    }

    private long count(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
