package capability;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.cfg.Configuration;
import org.hibernate.type.SqlTypes;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;

import static org.junit.Assert.*;

/**
 * {@code @Embeddable} 값 객체를 <b>JSON 컬럼 하나</b>에 담고, 그 안의 필드를 SQL 로 다룬다.
 *
 * <h2>@Struct 와 무엇이 다른가</h2>
 * 같은 자리를 채우는 두 방식이다 — 값 객체를 컬럼 하나에 담되, 담는 그릇이 다르다.
 *
 * <pre>
 * &#64;Embedded              ADDR_CITY varchar2, ADDR_ZIP number   (컬럼으로 펼침)
 * &#64;Struct                ADDR      ADDR_T                       (object UDT)
 * &#64;JdbcTypeCode(JSON)    ADDR      json                         (JSON 한 덩어리)  ← 이것
 * </pre>
 *
 * <p>값을 통째로 넣고 빼는 것은 원래도 됐다. 이 작업으로 새로 되는 것은
 * <b>JSON 안쪽 필드를 SQL 로 찌르는 것</b>이다 — {@code where p.addr.city = ?},
 * {@code select p.addr.zip}, {@code update ... set p.addr.city = ?}.
 *
 * <h2>Oracle 과 갈라지는 세 지점</h2>
 * Oracle 구현을 그대로 베끼면 Tibero 에서 셋 다 깨진다. 이 테스트들이 그 회귀를 지킨다.
 *
 * <pre>
 * ① coalesce(&lt;json 컬럼&gt;,…)     JDBC-11022   → nvl(…)
 * ② json_object(returning json)  JDBC-8004    → json_query('{}','$' returning json)
 * ③ json_object 에 date 바인딩    "2024/03/05" → to_char(…,'YYYY-MM-DD')
 * </pre>
 *
 * ③이 가장 위험하다 — 예외 없이 <b>조용히</b> 잘못된 형식으로 저장되고, 나중에 읽을 때
 * 엔티티 로드가 통째로 실패한다.
 *
 * @see com.tmax.tibero.hibernate.dialect.aggregate.TiberoJsonAggregateWriter
 * @see com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport
 */
public class JsonAggregateMappingTest extends AbstractTiberoDialectTestBase {

    public enum Kind { HOME, WORK }

    @Embeddable
    public static class Addr {
        public String city;
        public Integer zip;
        public Long big;
        @Column(precision = 10, scale = 4) public BigDecimal lat;
        public Double dbl;
        public Boolean flag;
        public LocalDate since;
        public LocalDateTime touchedAt;
        public LocalTime at;
        public byte[] raw;
        @Lob public String memo;
        @Enumerated(EnumType.STRING) public Kind kind;
        public String[] tags;
        public Integer[] nums;
        public Boolean[] flags;
        public LocalDate[] days;
        public Double[] dbls;
    }

    @Embeddable
    public static class Inner {
        public String label;
        public Integer level;
    }

    @Embeddable
    public static class Outer {
        public String name;
        @JdbcTypeCode(SqlTypes.JSON) public Inner inner;
    }

    @Entity(name = "JamPerson") @Table(name = "JAM_PERSON")
    public static class Person {
        @Id public Long id;
        public String name;
        @JdbcTypeCode(SqlTypes.JSON) public Addr addr;
    }

    @Entity(name = "JamNest") @Table(name = "JAM_NEST")
    public static class Nest {
        @Id public Long id;
        @JdbcTypeCode(SqlTypes.JSON) public Outer o;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Person.class, Nest.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        // DDL 오류를 삼키지 않게 한다 — is json 체크 제약이 되살아나면 여기서 바로 걸린다
        cfg.setProperty("hibernate.hbm2ddl.halt_on_error", "true");
        cfg.setProperty("hibernate.session_factory.statement_inspector",
                SqlCaptureInspector.class.getName());
    }

    @Before
    public void clean() {
        inTransaction(s -> s.createMutationQuery("delete from JamPerson").executeUpdate());
        inTransaction(s -> s.createMutationQuery("delete from JamNest").executeUpdate());
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------

    /**
     * 컬럼 하나짜리 {@code json} 으로 만들어지는지, 그리고 <b>체크 제약이 없는지</b>.
     *
     * <p>Oracle 은 JSON 컬럼에 {@code check (col is json)} 을 붙이는데 Tibero 는 그 술어를
     * 받지 않는다({@code JDBC-8014}). §3.1 에서 {@code TiberoJsonBlobJdbcType} 이 그걸
     * 걷어냈고, 집계로 승격될 때도 유지되도록 {@code resolveAggregateJdbcType} 을
     * 재정의해 두었다 — 이 테스트가 그 회귀를 지킨다.
     */
    @Test
    public void ddl_createsSingleJsonColumn_withoutCheckConstraint() {
        assertEquals("ADDR 가 json 컬럼 하나로 만들어져야 함", 1L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='JAM_PERSON' and column_name='ADDR' and data_type='JSON'"));
        assertEquals("펼쳐진 컬럼이 있으면 안 됨 (집계가 아니라 @Embedded 로 매핑된 것)", 0L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='JAM_PERSON' and column_name like 'ADDR!_%' escape '!'"));
        assertEquals("is json 체크 제약이 없어야 함", 0L, count(
                "select count(*) from user_constraints "
                        + "where table_name='JAM_PERSON' and constraint_type='C' "
                        + "and upper(search_condition) like '%IS JSON%'"));
    }

    // ------------------------------------------------------------------
    // 값 왕복
    // ------------------------------------------------------------------

    /** 타입별 왕복 — JSON 은 결국 문자열이라 타입 되돌리기가 읽기 경로의 전부다. */
    @Test
    public void roundTrip_allBasicTypes() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 1L; p.name = "n";
            p.addr = new Addr();
            p.addr.city = "Seoul";
            p.addr.zip = 6236;
            p.addr.big = 9_000_000_000L;
            p.addr.lat = new BigDecimal("37.1234");
            p.addr.dbl = 1.5d;
            p.addr.flag = true;
            p.addr.since = LocalDate.of(2024, 3, 5);
            p.addr.touchedAt = LocalDateTime.of(2024, 3, 5, 11, 22, 33);
            p.addr.at = LocalTime.of(11, 22, 33);
            p.addr.raw = new byte[]{(byte) 0xCA, (byte) 0xFE};
            p.addr.memo = "a long memo";
            p.addr.kind = Kind.WORK;
            s.persist(p);
        });
        inTransaction(s -> {
            Addr a = s.find(Person.class, 1L).addr;
            assertEquals("Seoul", a.city);
            assertEquals(Integer.valueOf(6236), a.zip);
            assertEquals(Long.valueOf(9_000_000_000L), a.big);
            assertEquals(0, new BigDecimal("37.1234").compareTo(a.lat));
            assertEquals(1.5d, a.dbl, 0.0d);
            assertEquals(Boolean.TRUE, a.flag);
            assertEquals(LocalDate.of(2024, 3, 5), a.since);
            assertEquals(LocalDateTime.of(2024, 3, 5, 11, 22, 33), a.touchedAt);
            assertEquals(LocalTime.of(11, 22, 33), a.at);
            assertArrayEquals(new byte[]{(byte) 0xCA, (byte) 0xFE}, a.raw);
            assertEquals("a long memo", a.memo);
            assertEquals(Kind.WORK, a.kind);
        });
    }

    /**
     * 저장된 원문이 <b>ISO-8601</b> 인지.
     *
     * <p>날짜가 {@code "2024/03/05"} 로 들어가면 읽기 쪽({@code to_date(…,'YYYY-MM-DD')})과
     * Java 쪽({@code JsonHelper})이 둘 다 깨진다. 원문을 직접 확인해 형식을 고정한다.
     */
    @Test
    public void storedJson_usesIsoDateFormat() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 2L;
            p.addr = new Addr();
            p.addr.since = LocalDate.of(2024, 3, 5);
            p.addr.touchedAt = LocalDateTime.of(2024, 3, 5, 11, 22, 33);
            s.persist(p);
        });
        final String raw = rawJson("select addr from JAM_PERSON where id=2");
        assertTrue("날짜가 ISO 여야 함: " + raw, raw.contains("\"since\":\"2024-03-05\""));
        assertTrue("타임스탬프에 T 구분자가 있어야 함: " + raw, raw.contains("\"touchedAt\":\"2024-03-05T"));
        assertFalse("NLS 형식(슬래시)이 들어가면 안 됨: " + raw, raw.contains("2024/03/05"));
    }

    /** 임베더블 자체가 null. */
    @Test
    public void roundTrip_nullEmbeddable() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 3L;
            s.persist(p);
        });
        inTransaction(s -> assertNull(s.find(Person.class, 3L).addr));
    }

    /** 안쪽 필드만 null — 경로가 다르다. */
    @Test
    public void roundTrip_partiallyNullFields() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 4L;
            p.addr = new Addr();
            p.addr.city = "Busan";
            s.persist(p);
        });
        inTransaction(s -> {
            Addr a = s.find(Person.class, 4L).addr;
            assertEquals("Busan", a.city);
            assertNull(a.zip);
            assertNull(a.since);
            assertNull(a.flag);
        });
    }

    // ------------------------------------------------------------------
    // 질의
    // ------------------------------------------------------------------

    /** 성분을 조건·투영에 쓴다 — 이 작업의 본론. */
    @Test
    public void query_byComponent() {
        seedSeoul(10L);
        inTransaction(s -> {
            assertEquals("Seoul", s.createQuery(
                    "select p.addr.city from JamPerson p where p.addr.zip = 6236", String.class)
                    .getSingleResult());
            assertEquals(Integer.valueOf(6236), s.createQuery(
                    "select p.addr.zip from JamPerson p where p.addr.city = 'Seoul'", Integer.class)
                    .getSingleResult());
        });
    }

    /** 정렬·집계에도 걸리는지. */
    @Test
    public void query_orderAndAggregateOnComponent() {
        seedSeoul(11L);
        seedCity(12L, "Busan", 1000);
        inTransaction(s -> {
            List<String> cities = s.createQuery(
                    "select p.addr.city from JamPerson p order by p.addr.zip", String.class)
                    .getResultList();
            assertEquals(List.of("Busan", "Seoul"), cities);
            assertEquals(Long.valueOf(2), s.createQuery(
                    "select count(p.addr.city) from JamPerson p", Long.class).getSingleResult());
        });
    }

    // ------------------------------------------------------------------
    // 부분 갱신
    // ------------------------------------------------------------------

    /** 필드 하나를 바꿔도 나머지가 보존되는지 — 머지패치가 제 역할을 하는지. */
    @Test
    public void update_singleComponent_preservesOthers() {
        seedSeoul(20L);
        inTransaction(s -> assertEquals(1, s.createMutationQuery(
                "update JamPerson p set p.addr.city = 'Incheon' where p.id = 20").executeUpdate()));
        inTransaction(s -> {
            Addr a = s.find(Person.class, 20L).addr;
            assertEquals("Incheon", a.city);
            assertEquals("건드리지 않은 필드는 남아야 함", Integer.valueOf(6236), a.zip);
            assertEquals(Boolean.TRUE, a.flag);
        });
    }

    /** 여러 필드를 한 문장에. */
    @Test
    public void update_multipleComponents() {
        seedSeoul(21L);
        inTransaction(s -> s.createMutationQuery(
                "update JamPerson p set p.addr.city = 'Ulsan', p.addr.zip = 1111 where p.id = 21")
                .executeUpdate());
        inTransaction(s -> {
            Addr a = s.find(Person.class, 21L).addr;
            assertEquals("Ulsan", a.city);
            assertEquals(Integer.valueOf(1111), a.zip);
            assertEquals("나머지 보존", Boolean.TRUE, a.flag);
        });
    }

    /**
     * 컬럼이 NULL 인 행에 부분 갱신 — 객체가 새로 만들어지는지.
     *
     * <p>{@code json_mergepatch(NULL, …)} 은 NULL 이다. 바닥을 깔지 않으면
     * <b>오류 없이 아무 일도 안 일어난다.</b> Oracle 은 {@code coalesce} 로 까는데 Tibero 는
     * json 컬럼에 {@code coalesce} 를 못 써서({@code JDBC-11022}) {@code nvl} 을 쓴다.
     */
    @Test
    public void update_onNullAggregate_createsTheObject() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 22L;
            s.persist(p);
        });
        inTransaction(s -> s.createMutationQuery(
                "update JamPerson p set p.addr.city = 'FromNull' where p.id = 22").executeUpdate());
        inTransaction(s -> {
            Addr a = s.find(Person.class, 22L).addr;
            assertNotNull("NULL 컬럼에도 객체가 만들어져야 함", a);
            assertEquals("FromNull", a.city);
        });
    }

    /**
     * ⚠️ <b>가장 중요한 회귀</b> — 날짜를 부분 갱신한 뒤에도 엔티티가 로드되는지.
     *
     * <p>{@code to_char} 래핑이 빠지면 {@code json_object} 가 날짜를 {@code "2025/01/02"} 로
     * 넣는다. UPDATE 는 <b>성공</b>하고 아무 경고도 없다. 그런데 그 행을 읽으려 하면
     * {@code DateTimeParseException} 으로 <b>엔티티 로드가 통째로 실패</b>한다 —
     * 조용히 데이터를 망가뜨리는 유형이다.
     */
    @Test
    public void update_temporalComponent_survivesEntityLoad() {
        seedSeoul(23L);
        inTransaction(s -> s.createMutationQuery(
                "update JamPerson p set p.addr.since = :d where p.id = 23")
                .setParameter("d", LocalDate.of(2025, 1, 2)).executeUpdate());

        final String raw = rawJson("select addr from JAM_PERSON where id=23");
        assertTrue("부분 갱신도 ISO 로 써야 함: " + raw, raw.contains("\"since\":\"2025-01-02\""));

        inTransaction(s -> {
            Addr a = s.find(Person.class, 23L).addr;
            assertEquals(LocalDate.of(2025, 1, 2), a.since);
            assertEquals("다른 필드도 온전해야 함", "Seoul", a.city);
        });
    }

    /**
     * 일반 flush 는 통째로 바인딩한다 — 렌더러가 그 경로로 새지 않는지.
     *
     * <p>부분 갱신 렌더러는 HQL 벌크 UPDATE 전용이다. 더티 체크로 나가는 UPDATE 까지
     * {@code json_mergepatch} 로 바뀌면 불필요하게 무겁고 의미도 달라진다.
     */
    @Test
    public void entityFlush_bindsWholeAggregate_notMergepatch() {
        seedSeoul(24L);
        SqlCaptureInspector.clear();
        inTransaction(s -> s.find(Person.class, 24L).addr.city = "Daegu");

        final List<String> updates = updatesCaptured();
        assertFalse("UPDATE 가 나가야 함", updates.isEmpty());
        for (String sql : updates) {
            assertFalse("더티 체크 경로에 mergepatch 가 끼면 안 됨: " + sql,
                    sql.toLowerCase().contains("json_mergepatch"));
        }
    }

    // ------------------------------------------------------------------
    // 중첩
    // ------------------------------------------------------------------

    /** 중첩 임베더블 왕복. */
    @Test
    public void nested_roundTrip() {
        seedNest(30L);
        inTransaction(s -> {
            Outer o = s.find(Nest.class, 30L).o;
            assertEquals("HQ", o.name);
            assertEquals("A", o.inner.label);
            assertEquals(Integer.valueOf(3), o.inner.level);
        });
    }

    /** 중첩 안쪽 성분을 SQL 로 찌른다 — 부모 {@code json_query} 를 벗겨 경로를 이어 붙인다. */
    @Test
    public void nested_queryInnerComponent() {
        seedNest(31L);
        inTransaction(s -> assertEquals("A", s.createQuery(
                "select e.o.inner.label from JamNest e where e.id = 31", String.class)
                .getSingleResult()));
    }

    /**
     * 중첩 안쪽 한 필드만 바꿔도 형제 필드가 보존되는지.
     *
     * <p>머지패치가 <b>재귀적으로</b> 병합해야 성립한다. 안쪽 객체를 통째로 갈아끼우면
     * {@code label} 이 사라진다.
     */
    @Test
    public void nested_updateInnerComponent_preservesSiblings() {
        seedNest(32L);
        inTransaction(s -> s.createMutationQuery(
                "update JamNest e set e.o.inner.level = 9 where e.id = 32").executeUpdate());
        inTransaction(s -> {
            Outer o = s.find(Nest.class, 32L).o;
            assertEquals(Integer.valueOf(9), o.inner.level);
            assertEquals("형제 필드가 살아 있어야 함", "A", o.inner.label);
            assertEquals("바깥 필드도 살아 있어야 함", "HQ", o.name);
        });
    }

    /** 바깥과 안쪽을 한 문장에서 함께. */
    @Test
    public void nested_updateBothLevels() {
        seedNest(33L);
        inTransaction(s -> s.createMutationQuery(
                "update JamNest e set e.o.name = 'HQ2', e.o.inner.level = 7 where e.id = 33")
                .executeUpdate());
        inTransaction(s -> {
            Outer o = s.find(Nest.class, 33L).o;
            assertEquals("HQ2", o.name);
            assertEquals(Integer.valueOf(7), o.inner.level);
            assertEquals("A", o.inner.label);
        });
    }

    /** 중첩 임베더블이 null 인 경우. */
    @Test
    public void nested_nullInner() {
        inTransaction(s -> {
            Nest n = new Nest();
            n.id = 34L;
            n.o = new Outer();
            n.o.name = "NoInner";
            s.persist(n);
        });
        inTransaction(s -> {
            Outer o = s.find(Nest.class, 34L).o;
            assertEquals("NoInner", o.name);
            assertNull(o.inner);
        });
    }

    // ------------------------------------------------------------------
    // 배열 성분
    // ------------------------------------------------------------------

    /**
     * 임베더블 안의 배열 필드가 JSON 배열로 왕복하는지 (Java 경로).
     *
     * <p>엔티티를 통째로 넣고 빼는 경로는 Java 직렬화({@code JsonHelper})라 원래도 됐다.
     * 아래 두 테스트가 진짜 검증 대상인 SQL 경로를 본다.
     */
    @Test
    public void array_roundTripThroughJavaPath() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 50L;
            p.addr = new Addr();
            p.addr.tags = new String[]{"a", "b"};
            p.addr.nums = new Integer[]{1, 2};
            p.addr.flags = new Boolean[]{true, false};
            p.addr.days = new LocalDate[]{LocalDate.of(2024, 3, 5)};
            p.addr.dbls = new Double[]{1.5, 2.5};
            s.persist(p);
        });
        inTransaction(s -> {
            Addr a = s.find(Person.class, 50L).addr;
            assertArrayEquals(new String[]{"a", "b"}, a.tags);
            assertArrayEquals(new Integer[]{1, 2}, a.nums);
            assertArrayEquals(new Boolean[]{true, false}, a.flags);
            assertArrayEquals(new LocalDate[]{LocalDate.of(2024, 3, 5)}, a.days);
            assertArrayEquals(new Double[]{1.5, 2.5}, a.dbls);
        });
    }

    /**
     * ⚠️ 배열 성분을 HQL 로 <b>부분 갱신</b>한다 — Oracle 에는 없는 보강.
     *
     * <p>배열을 그대로 {@code json_object} 에 넘기면 Tibero 가 거부한다
     * ({@code json_object('tags':cast(? as StringArray) …)} → {@code JDBC-11003}).
     * {@code TiberoJsonAggregateWriter} 가 {@code json_arrayagg} 로 감싸서 푼다.
     *
     * <p>Oracle 은 원소가 CLOB 이나 boolean 일 때만 감싸고 나머지는 바인드를 날것으로
     * 넘긴다 — Oracle 에서는 그래도 통하기 때문이다. 우리는 모든 원소 타입을 감싼다.
     */
    @Test
    public void array_bulkUpdateEachElementType() {
        seedSeoul(51L);
        inTransaction(s -> s.createMutationQuery(
                "update JamPerson p set p.addr.tags = :t, p.addr.nums = :n, p.addr.flags = :f, "
                        + "p.addr.days = :d, p.addr.dbls = :b where p.id = 51")
                .setParameter("t", new String[]{"x", "y"})
                .setParameter("n", new Integer[]{7, 8})
                .setParameter("f", new Boolean[]{true, false})
                .setParameter("d", new LocalDate[]{LocalDate.of(2025, 1, 2)})
                .setParameter("b", new Double[]{9.5})
                .executeUpdate());
        inTransaction(s -> {
            Addr a = s.find(Person.class, 51L).addr;
            assertArrayEquals(new String[]{"x", "y"}, a.tags);
            assertArrayEquals(new Integer[]{7, 8}, a.nums);
            assertArrayEquals(new Boolean[]{true, false}, a.flags);
            assertArrayEquals(new LocalDate[]{LocalDate.of(2025, 1, 2)}, a.days);
            assertArrayEquals(new Double[]{9.5}, a.dbls);
            assertEquals("다른 필드는 보존", "Seoul", a.city);
        });
    }

    /**
     * 배열 성분을 <b>SELECT</b> 한다 — Oracle 과 방식이 다른 읽기 경로.
     *
     * <p>Oracle 의 {@code json_value(w,'$.tags' returning StringArray)} 는 Tibero 에서
     * {@code JDBC-8004}, {@code cast(json_value(…) as StringArray)} 는 {@code JDBC-11021}.
     * {@code json_table} 로 행을 펼쳐 {@code multiset} 으로 다시 모으는 경로를 쓴다.
     */
    @Test
    public void array_selectComponent() {
        inTransaction(s -> {
            Person p = new Person();
            p.id = 52L;
            p.addr = new Addr();
            p.addr.tags = new String[]{"a", "b"};
            p.addr.nums = new Integer[]{1, 2};
            p.addr.flags = new Boolean[]{true, false};
            p.addr.days = new LocalDate[]{LocalDate.of(2024, 3, 5)};
            p.addr.dbls = new Double[]{1.5, 2.5};
            s.persist(p);
        });
        inTransaction(s -> {
            assertArrayEquals(new String[]{"a", "b"}, s.createQuery(
                    "select p.addr.tags from JamPerson p where p.id=52", String[].class).getSingleResult());
            assertArrayEquals(new Integer[]{1, 2}, s.createQuery(
                    "select p.addr.nums from JamPerson p where p.id=52", Integer[].class).getSingleResult());
            assertArrayEquals(new Double[]{1.5, 2.5}, s.createQuery(
                    "select p.addr.dbls from JamPerson p where p.id=52", Double[].class).getSingleResult());
            assertArrayEquals(new LocalDate[]{LocalDate.of(2024, 3, 5)}, s.createQuery(
                    "select p.addr.days from JamPerson p where p.id=52", LocalDate[].class).getSingleResult());
            // 원소가 boolean 이면 decode 를 한 겹 둘러야 한다 — 없으면 JDBC-5074
            assertArrayEquals(new Boolean[]{true, false}, s.createQuery(
                    "select p.addr.flags from JamPerson p where p.id=52", Boolean[].class).getSingleResult());
        });
    }

    // ------------------------------------------------------------------
    // 두 쓰기 경로의 표현 차이
    // ------------------------------------------------------------------

    /**
     * 같은 값이라도 <b>Java 경로와 SQL 경로가 다른 JSON</b>을 만든다 — 읽기는 같다.
     *
     * <pre>
     * Java  (persist / flush)  {"flag":true}     진짜 JSON 참/거짓
     * SQL   (HQL 벌크 UPDATE)   {"flag":"true"}   문자열
     * </pre>
     *
     * <p>Oracle 도 똑같다 — {@code decode(?,1,'true',0,'false',null)} 이 문자열을 만든다.
     * 읽기 경로가 양쪽을 다 받아 주므로 애플리케이션에서는 차이가 없지만, 저장된 원문을
     * 직접 들여다보거나 다른 도구로 파싱하는 코드는 알고 있어야 한다.
     *
     * <p>이 테스트는 그 사실을 <b>고정</b>한다. 한쪽 표현만 지원하도록 읽기 경로를 고치면
     * 여기서 걸린다.
     */
    @Test
    public void booleanHasTwoRepresentations_butBothReadBackTheSame() {
        inTransaction(s -> {
            Person java = new Person();
            java.id = 60L;
            java.addr = new Addr();
            java.addr.flag = true;
            s.persist(java);
            Person sql = new Person();
            sql.id = 61L;
            sql.addr = new Addr();
            sql.addr.city = "seed";
            s.persist(sql);
        });
        inTransaction(s -> s.createMutationQuery(
                "update JamPerson p set p.addr.flag = true where p.id = 61").executeUpdate());

        assertTrue("Java 경로는 진짜 JSON 참/거짓",
                rawJson("select addr from JAM_PERSON where id=60").contains("\"flag\":true"));
        assertTrue("SQL 경로는 문자열",
                rawJson("select addr from JAM_PERSON where id=61").contains("\"flag\":\"true\""));

        inTransaction(s -> {
            // 성분 SELECT
            assertEquals(List.of(60L, 61L), s.createQuery(
                    "select p.id from JamPerson p where p.addr.flag = true order by p.id", Long.class)
                    .getResultList());
            // 엔티티 로드
            assertEquals(Boolean.TRUE, s.find(Person.class, 60L).addr.flag);
            assertEquals(Boolean.TRUE, s.find(Person.class, 61L).addr.flag);
        });
    }

    // ------------------------------------------------------------------
    // 한계
    // ------------------------------------------------------------------

    /**
     * {@code null} 을 대입하면 JSON 에서 <b>키가 사라진다</b>.
     *
     * <p>머지패치의 표준 동작이다(RFC 7386) — 값이 {@code null} 인 키는 삭제로 해석된다.
     * 읽을 때는 "키 없음"과 "값 null" 이 똑같이 NULL 로 나오므로 애플리케이션에서는
     * 차이가 없지만, 원문을 직접 보는 코드는 영향을 받는다. Oracle 도 동일하다.
     */
    @Test
    public void update_toNull_removesTheKey() {
        seedSeoul(40L);
        inTransaction(s -> s.createMutationQuery(
                "update JamPerson p set p.addr.zip = null where p.id = 40").executeUpdate());

        final String raw = rawJson("select addr from JAM_PERSON where id=40");
        assertFalse("키 자체가 사라진다: " + raw, raw.contains("\"zip\""));
        inTransaction(s -> {
            Addr a = s.find(Person.class, 40L).addr;
            assertNull("읽기 결과는 null 로 같다", a.zip);
            assertEquals("Seoul", a.city);
        });
    }

    // ------------------------------------------------------------------

    private void seedSeoul(long id) {
        seedCity(id, "Seoul", 6236);
    }

    private void seedCity(long id, String city, int zip) {
        inTransaction(s -> {
            Person p = new Person();
            p.id = id;
            p.addr = new Addr();
            p.addr.city = city;
            p.addr.zip = zip;
            p.addr.flag = true;
            s.persist(p);
        });
    }

    private void seedNest(long id) {
        inTransaction(s -> {
            Nest n = new Nest();
            n.id = id;
            n.o = new Outer();
            n.o.name = "HQ";
            n.o.inner = new Inner();
            n.o.inner.label = "A";
            n.o.inner.level = 3;
            s.persist(n);
        });
    }

    private String rawJson(String sql) {
        return inTransactionReturning(s ->
                s.createNativeQuery(sql, String.class).getSingleResult());
    }

    private List<String> updatesCaptured() {
        final List<String> out = new java.util.ArrayList<>();
        for (String sql : SqlCaptureInspector.getSqls()) {
            if (sql.toLowerCase().startsWith("update")) {
                out.add(sql);
            }
        }
        SqlCaptureInspector.clear();
        return out;
    }

    private long count(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
