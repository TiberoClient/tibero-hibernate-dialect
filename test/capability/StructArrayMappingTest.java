package capability;

import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Array;
import org.hibernate.annotations.Struct;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.util.List;

import static org.junit.Assert.*;

/**
 * {@code @Struct} 임베더블의 <b>배열</b> 필드를 object UDT 의 VARRAY 컬럼에 매핑한다.
 *
 * <h2>어떤 기능인가</h2>
 * {@code @Struct} 는 값 객체 하나를 object UDT 컬럼 하나에 담는다. 이 테스트가 다루는 것은
 * 그 값 객체를 <b>여럿</b> 담는 경우다.
 *
 * <pre>
 * &#64;Struct Address           →  ADDR  SMT_ADDR                    (값 하나)
 * &#64;Struct Address[] stops   →  STOPS SMT_ADDRArray               (값 여럿)
 *                                 create type SMT_ADDRArray
 *                                   as varying array(127) of SMT_ADDR
 * </pre>
 *
 * <h2>무엇이 막고 있었나</h2>
 * 이름 규칙과 exporter 는 네이티브 배열 컬럼 작업(§5.2 P3)에서 이미 들어와 있었다.
 * 빠진 것은 <b>UDT 등록</b> 한 곳뿐이었다 —
 * {@code TiberoArrayJdbcType.addAuxiliaryDatabaseObjects} 는 요소가 struct 이면 등록을
 * 건너뛰고(그 시점에는 요소 object 타입 이름을 알 수 없다), 그 일을 맡아야 할
 * {@code TiberoAggregateSupport} 가 아무것도 하지 않고 있었다.
 *
 * @see com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport#aggregateAuxiliaryDatabaseObjects
 */
public class StructArrayMappingTest extends AbstractTiberoDialectTestBase {

    @Embeddable
    @Struct(name = "SAM_STOP")
    public static class Stop {
        public String city;
        public Integer seq;
    }

    @Entity(name = "SamTrip") @Table(name = "SAM_TRIP")
    public static class Trip {
        @Id public Long id;
        /**
         * {@code @Array} 를 일부러 붙여 두었다 — struct 배열에서는 <b>무시된다</b>는 것을
         * {@link #arrayLengthAnnotation_isIgnoredForStructArrays} 가 고정한다.
         */
        @Array(length = 5) public Stop[] stops;
        /** 같은 struct 타입을 쓰는 두 번째 필드 — VARRAY 타입을 공유한다. */
        public Stop[] extras;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Trip.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        // DDL 오류를 삼키지 않게 한다 — UDT 등록이 빠지면 create table 이 조용히 실패하고
        // 뒤의 단언이 엉뚱한 이유(JDBC-8033)로 깨진다
        cfg.setProperty("hibernate.hbm2ddl.halt_on_error", "true");
    }

    @Before
    public void clean() {
        inTransaction(s -> s.createMutationQuery("delete from SamTrip").executeUpdate());
    }

    // ------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------

    /** 요소 object 타입이 만들어졌는지. */
    @Test
    public void ddl_createsElementObjectType() {
        assertEquals("SAM_STOP 이 object 타입으로 있어야 함", 1L, count(
                "select count(*) from user_types where type_name='SAM_STOP' and typecode='OBJECT'"));
        assertEquals("속성 2개", 2L, count(
                "select count(*) from user_type_attrs where type_name='SAM_STOP'"));
    }

    /**
     * 그 타입의 VARRAY 가 만들어졌는지 — 이 작업으로 새로 생기는 부분.
     *
     * <p>등록이 빠지면 이 타입이 없고, 컬럼 타입이 해석되지 않아 {@code create table} 이
     * 실패한다.
     *
     * <p>카탈로그 이름이 <b>대문자</b>인 점에 주의 — DDL 은
     * {@code create or replace type SAM_STOPArray ...} 로 나가지만 인용하지 않은 식별자는
     * Tibero 가 대문자로 접어 저장한다.
     */
    @Test
    public void ddl_createsVarrayOfTheObjectType() {
        assertEquals("SAM_STOP 의 VARRAY 타입이 있어야 함", 1L, count(
                "select count(*) from user_coll_types "
                        + "where type_name='SAM_STOPARRAY' and coll_type='VARYING ARRAY' "
                        + "and elem_type_name='SAM_STOP'"));
    }

    /**
     * ⚠️ {@code @Array(length = n)} 은 <b>struct 배열에는 반영되지 않는다</b>.
     *
     * <p>엔티티의 {@code stops} 에는 {@code @Array(length = 5)} 가 붙어 있는데도 상한이
     * <b>127</b>(기본값)로 만들어진다. Hibernate 의 바인더가 struct 집계 컬럼에는 그 값을
     * 실어주지 않기 때문이고, <b>Oracle 도 같은 코드라 같은 한계</b>다
     * ({@code OracleAggregateSupport} 가 {@code aggregateColumn.getArrayLength()} 를
     * 읽는데 그 값이 늘 {@code null} 이다).
     *
     * <p>사실 반영할 수도 없다 — 아래 {@link #twoFieldsShareOneVarrayType} 처럼
     * <b>같은 struct 타입을 쓰는 필드들이 VARRAY 타입 하나를 공유</b>하므로, 필드마다 다른
     * 상한을 주려면 타입이 갈라져야 한다. 일반 배열 컬럼({@code String[]} 등)에서는
     * {@code @Array} 가 정상 반영된다 — 거기는 요소 타입마다 타입이 하나씩이라 충돌이 없다.
     *
     * <p>이 테스트는 그 동작을 <b>고정</b>하는 것이지 바람직하다고 주장하는 게 아니다.
     */
    @Test
    public void arrayLengthAnnotation_isIgnoredForStructArrays() {
        assertEquals("@Array(length=5) 를 붙여도 기본 상한 127 이 쓰인다", 1L, count(
                "select count(*) from user_coll_types "
                        + "where type_name='SAM_STOPARRAY' and upper_bound=127"));
    }

    /**
     * 같은 struct 타입을 쓰는 두 필드가 <b>VARRAY 타입 하나를 공유</b>한다.
     *
     * <p>타입 이름이 요소 타입에서만 만들어지므로({@code SAM_STOP} → {@code SAM_STOPArray})
     * 필드가 몇 개든 타입은 하나다. 값은 물론 필드마다 따로 간다
     * ({@link #twoArrayFields_shareTheTypeButNotTheValue}).
     */
    @Test
    public void twoFieldsShareOneVarrayType() {
        assertEquals("SAM_STOP 의 VARRAY 타입은 하나만 만들어진다", 1L, count(
                "select count(*) from user_coll_types where elem_type_name='SAM_STOP'"));
        assertEquals("두 컬럼이 같은 타입을 쓴다", 2L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='SAM_TRIP' and data_type='SAM_STOPARRAY'"));
    }

    /** 테이블 컬럼이 그 VARRAY 타입인지 — VARBINARY 로 직렬화되면 안 된다. */
    @Test
    public void ddl_columnTypeIsTheVarray() {
        assertEquals(1L, count("select count(*) from user_tab_columns "
                + "where table_name='SAM_TRIP' and column_name='STOPS' and data_type='SAM_STOPARRAY'"));
    }

    // ------------------------------------------------------------------
    // 값 왕복
    // ------------------------------------------------------------------

    /** 값을 넣고 빼는 기본 경로. */
    @Test
    public void roundTrip_values() {
        inTransaction(s -> {
            Trip t = new Trip();
            t.id = 1L;
            t.stops = new Stop[]{stop("Seoul", 1), stop("Busan", 2)};
            s.persist(t);
        });
        inTransaction(s -> {
            Trip t = s.find(Trip.class, 1L);
            assertNotNull(t.stops);
            assertEquals(2, t.stops.length);
            assertEquals("Seoul", t.stops[0].city);
            assertEquals(Integer.valueOf(1), t.stops[0].seq);
            assertEquals("Busan", t.stops[1].city);
            assertEquals(Integer.valueOf(2), t.stops[1].seq);
        });
    }

    /** 배열 자체가 null. */
    @Test
    public void roundTrip_nullArray() {
        inTransaction(s -> {
            Trip t = new Trip();
            t.id = 2L;
            s.persist(t);
        });
        inTransaction(s -> assertNull(s.find(Trip.class, 2L).stops));
    }

    /**
     * 빈 배열이 null 로 뭉개지지 않는지.
     *
     * <p>일반 배열 컬럼에서 겪었던 문제다 — 원소가 0개인 VARRAY 를 읽으면
     * {@code Array.getArray()} 가 {@code null} 을 돌려준다.
     */
    @Test
    public void roundTrip_emptyArray_isNotNull() {
        inTransaction(s -> {
            Trip t = new Trip();
            t.id = 3L;
            t.stops = new Stop[0];
            s.persist(t);
        });
        inTransaction(s -> {
            Trip t = s.find(Trip.class, 3L);
            assertNotNull("빈 배열이 null 이 되면 안 됨", t.stops);
            assertEquals(0, t.stops.length);
        });
    }

    /** 원소 안의 필드가 null 인 경우 — struct 읽기 경로가 또 다르다. */
    @Test
    public void roundTrip_nullFieldInsideElement() {
        inTransaction(s -> {
            Trip t = new Trip();
            t.id = 4L;
            t.stops = new Stop[]{stop(null, 7), stop("Daegu", null)};
            s.persist(t);
        });
        inTransaction(s -> {
            Trip t = s.find(Trip.class, 4L);
            assertEquals(2, t.stops.length);
            assertNull(t.stops[0].city);
            assertEquals(Integer.valueOf(7), t.stops[0].seq);
            assertEquals("Daegu", t.stops[1].city);
            assertNull(t.stops[1].seq);
        });
    }

    /** 두 배열 필드가 같은 VARRAY 타입을 공유해도 각자 값을 갖는지. */
    @Test
    public void twoArrayFields_shareTheTypeButNotTheValue() {
        inTransaction(s -> {
            Trip t = new Trip();
            t.id = 5L;
            t.stops = new Stop[]{stop("A", 1)};
            t.extras = new Stop[]{stop("B", 2), stop("C", 3)};
            s.persist(t);
        });
        inTransaction(s -> {
            Trip t = s.find(Trip.class, 5L);
            assertEquals(1, t.stops.length);
            assertEquals("A", t.stops[0].city);
            assertEquals(2, t.extras.length);
            assertEquals("C", t.extras[1].city);
        });
    }

    // ------------------------------------------------------------------
    // SQL 접근
    // ------------------------------------------------------------------

    /**
     * {@code table()} 로 원소를 펼쳐 성분에 접근한다.
     *
     * <p>배열로 담는 값어치가 여기 있다 — VARBINARY 로 직렬화했다면 불가능하다.
     */
    @Test
    public void nativeSql_canUnnestAndFilterByComponent() {
        inTransaction(s -> {
            Trip t = new Trip();
            t.id = 6L;
            t.stops = new Stop[]{stop("Seoul", 1), stop("Busan", 2), stop("Seoul", 3)};
            s.persist(t);
        });
        assertEquals("table() 로 펼쳐 성분 조건을 걸 수 있어야 함", 2L, count(
                "select count(*) from SAM_TRIP t, table(t.stops) e "
                        + "where t.id = 6 and e.city = 'Seoul'"));
    }

    // ------------------------------------------------------------------

    private static Stop stop(String city, Integer seq) {
        Stop s = new Stop();
        s.city = city;
        s.seq = seq;
        return s;
    }

    private long count(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
