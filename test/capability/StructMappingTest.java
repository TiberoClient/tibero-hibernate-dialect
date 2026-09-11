package capability;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.Version;
import org.hibernate.annotations.Struct;
import org.hibernate.cfg.Configuration;
import org.junit.Before;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

import static org.junit.Assert.*;

/**
 * {@code @Struct} 임베더블 매핑 — 값이 실제로 왕복하고 SQL 이 Tibero 에서 도는지 본다.
 *
 * <p>{@code @Struct} 는 임베더블을 <b>컬럼 하나</b>에 담는다. 컬럼 타입은 DB 의 object UDT 다.
 * 필드가 컬럼으로 펼쳐지는 {@code @Embedded} 와 스키마 모양이 다르다.
 *
 * <pre>
 * &#64;Embedded                     &#64;Struct(name = "ADDR_T")
 *   STREET varchar2(255)            ADDR  ADDR_T
 *   CITY   varchar2(255)
 * </pre>
 *
 * <p>Tibero 는 세 가지 지점에서 표준·Oracle 과 다르게 굴어 각각 손을 봐야 했다.
 * 아래 테스트들이 그 세 가지를 각각 고정한다.
 *
 * @see com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport
 * @see com.tmax.tibero.hibernate.type.TiberoStructJdbcType
 * @see com.tmax.tibero.hibernate.dialect.TiberoSqlAstTranslator#visitSetAssignment
 */
public class StructMappingTest extends AbstractTiberoDialectTestBase {

    // ------------------------------------------------------------------
    // 픽스처
    // ------------------------------------------------------------------

    @Embeddable
    @Struct(name = "SMT_ADDR")
    public static class Address {
        public String street;
        public String city;
        public Integer zip;
        @Column(precision = 10, scale = 4) public BigDecimal lat;
        public LocalDate since;
        public LocalDateTime touchedAt;
    }

    @Entity(name = "SmtPerson") @Table(name = "SMT_PERSON")
    public static class Person {
        @Id public Long id;
        public String name;
        public Address addr;
    }

    /** 중첩 — @Struct 안에 @Struct */
    @Embeddable @Struct(name = "SMT_GEO")
    public static class Geo {
        @Column(precision = 10, scale = 4) public BigDecimal x;
        @Column(precision = 10, scale = 4) public BigDecimal y;
    }

    @Embeddable @Struct(name = "SMT_SITE")
    public static class Site {
        public String label;
        public Geo geo;
    }

    @Entity(name = "SmtPlace") @Table(name = "SMT_PLACE")
    public static class Place {
        @Id public Long id;
        public Site site;
        @Version public long ver;
    }


    public enum Kind { HOME, WORK }

    /** boolean·enum·varchar·raw — 전부 STRUCT 안에서 지원되는 타입 */
    @Embeddable @Struct(name = "SMT_MIX")
    public static class Mix {
        public Boolean flag;
        @jakarta.persistence.Enumerated(jakarta.persistence.EnumType.STRING) public Kind kind;
        public String memo;
        public byte[] raw;
    }

    @Entity(name = "SmtFlagged") @Table(name = "SMT_FLAGGED")
    public static class Flagged {
        @Id public Long id;
        public Mix mix;
    }

    /** 지원하지 않는 속성 — 기동이 거부되어야 하므로 getAnnotatedClasses 에 넣지 않는다 */
    @Embeddable @Struct(name = "SMT_BAD_DBL")
    public static class BadDoubleAttr { public Double d; }
    @Entity(name = "SmtBadDouble") @Table(name = "SMT_BAD_DBL_T")
    public static class BadDouble { @Id public Long id; public BadDoubleAttr v; }

    @Embeddable @Struct(name = "SMT_BAD_LOB")
    public static class BadLobAttr { @jakarta.persistence.Lob public String memo; }
    @Entity(name = "SmtBadLob") @Table(name = "SMT_BAD_LOB_T")
    public static class BadLob { @Id public Long id; public BadLobAttr v; }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Person.class, Place.class, Flagged.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    /** create-drop 은 SessionFactory 단위라 테스트끼리 행이 샌다. 매번 비운다. */
    @Before
    public void clean() {
        inTransaction(s -> {
            s.createMutationQuery("delete from SmtPerson").executeUpdate();
            s.createMutationQuery("delete from SmtPlace").executeUpdate();
            s.createMutationQuery("delete from SmtFlagged").executeUpdate();
        });
    }

    // ------------------------------------------------------------------
    // DDL — object UDT 로 나가는가
    // ------------------------------------------------------------------

    /**
     * {@code getCreateUserDefinedTypeKindString()} 이 {@code "object"} 를 돌려주지 않으면
     * {@code create type T as (...)} 가 나가 Tibero 가 거부한다.
     */
    @Test
    public void ddl_createsObjectUdt_andSingleColumn() {
        assertEquals("SMT_ADDR 는 object 타입이어야 함", 1L, count(
                "select count(*) from user_types where type_name='SMT_ADDR' and typecode='OBJECT'"));
        assertEquals("임베더블 필드 6개가 UDT 속성으로 들어가야 함", 6L, count(
                "select count(*) from user_type_attrs where type_name='SMT_ADDR'"));
        assertEquals("테이블에는 컬럼이 하나만 늘어야 함 (id, name, addr)", 3L, count(
                "select count(*) from user_tab_columns where table_name='SMT_PERSON'"));
        assertEquals("addr 컬럼의 타입이 UDT 여야 함", 1L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='SMT_PERSON' and column_name='ADDR' and data_type='SMT_ADDR'"));
    }

    // ------------------------------------------------------------------
    // 값 왕복
    // ------------------------------------------------------------------

    @Test
    public void roundTrip_allBasicTypes() {
        Address a = new Address();
        a.street = "Gangnam-daero 396";
        a.city = "Seoul";
        a.zip = 6236;
        a.lat = new BigDecimal("37.4979");
        a.since = LocalDate.of(2020, 3, 15);
        a.touchedAt = LocalDateTime.of(2024, 1, 31, 10, 20, 30);

        inTransaction(s -> { Person p = new Person(); p.id = 1L; p.name = "kim"; p.addr = a; s.persist(p); });
        inTransaction(s -> {
            Person p = s.find(Person.class, 1L);
            assertEquals("Gangnam-daero 396", p.addr.street);
            assertEquals("Seoul", p.addr.city);
            assertEquals(Integer.valueOf(6236), p.addr.zip);
            assertEquals(0, new BigDecimal("37.4979").compareTo(p.addr.lat));
            assertEquals(LocalDate.of(2020, 3, 15), p.addr.since);
            assertEquals("struct 안에서도 시각이 보존되어야 함",
                    LocalDateTime.of(2024, 1, 31, 10, 20, 30), p.addr.touchedAt);
        });
    }

    /**
     * 임베더블 자체가 {@code null} 인 경우.
     *
     * <p>기본 바인더는 {@code setNull(i, Types.STRUCT)} 를 호출하는데 tbjdbc 가
     * {@code JDBC-590703 Unsupported data type. - OBJECT} 로 거부한다.
     * {@code TiberoStructJdbcType} 이 UDT 이름을 함께 넘겨 해결한다.
     */
    @Test
    public void roundTrip_nullEmbeddable() {
        inTransaction(s -> { Person p = new Person(); p.id = 2L; p.name = "no-addr"; p.addr = null; s.persist(p); });
        inTransaction(s -> assertNull(s.find(Person.class, 2L).addr));
    }

    /** 임베더블은 있는데 안쪽 필드만 null 인 경우 — 위와 경로가 다르다. */
    @Test
    public void roundTrip_partiallyNullFields() {
        inTransaction(s -> {
            Person p = new Person(); p.id = 3L; p.name = "partial";
            p.addr = new Address(); p.addr.city = "Busan";   // 나머지 전부 null
            s.persist(p);
        });
        inTransaction(s -> {
            Person p = s.find(Person.class, 3L);
            assertEquals("Busan", p.addr.city);
            assertNull(p.addr.street);
            assertNull(p.addr.zip);
            assertNull(p.addr.touchedAt);
        });
    }

    // ------------------------------------------------------------------
    // 질의 — 집계 컬럼 안의 필드
    // ------------------------------------------------------------------

    /** {@code aggregateComponentCustomReadExpression} 이 만드는 {@code 별칭.컬럼.필드} */
    @Test
    public void query_byComponent_readsIntoStruct() {
        seedSeoul(10L);
        inTransaction(s -> {
            List<String> r = s.createQuery(
                    "select p.addr.city from SmtPerson p where p.addr.zip = 6236", String.class).getResultList();
            assertEquals(List.of("Seoul"), r);
        });
    }

    @Test
    public void query_orderAndAggregate_onComponent() {
        seedSeoul(11L);
        inTransaction(s -> {
            Person p = new Person(); p.id = 12L; p.name = "b";
            p.addr = new Address(); p.addr.city = "Busan"; p.addr.zip = 48000;
            s.persist(p);
        });
        inTransaction(s -> {
            List<String> r = s.createQuery(
                    "select p.addr.city from SmtPerson p order by p.addr.zip desc", String.class).getResultList();
            assertEquals(List.of("Busan", "Seoul"), r);
            assertEquals(Long.valueOf(2), s.createQuery(
                    "select count(p.addr.city) from SmtPerson p", Long.class).getSingleResult());
        });
    }

    // ------------------------------------------------------------------
    // 갱신 — SET 절에 별칭이 붙는가
    // ------------------------------------------------------------------

    /**
     * {@code TiberoSqlAstTranslator.visitSetAssignment} 가 없으면
     * {@code set addr.city=?} 가 나가 {@code JDBC-8026 Invalid identifier} 로 실패한다.
     */
    @Test
    public void update_singleComponent_qualifiesWithAlias() {
        seedSeoul(20L);
        inTransaction(s -> assertEquals(1, s.createMutationQuery(
                "update SmtPerson p set p.addr.city = 'Busan' where p.id = 20").executeUpdate()));
        inTransaction(s -> assertEquals("Busan", s.find(Person.class, 20L).addr.city));
    }

    /** 엔티티를 더티체크로 갱신하는 경로 — 위와 다른 SQL 이 나간다. */
    @Test
    public void update_viaDirtyChecking() {
        seedSeoul(21L);
        inTransaction(s -> s.find(Person.class, 21L).addr.city = "Daegu");
        inTransaction(s -> assertEquals("Daegu", s.find(Person.class, 21L).addr.city));
    }

    /** 보통 컬럼의 UPDATE 도 별칭이 붙은 채로 정상 동작해야 한다 (회귀 방지). */
    @Test
    public void update_plainColumn_stillWorks() {
        seedSeoul(22L);
        inTransaction(s -> assertEquals(1, s.createMutationQuery(
                "update SmtPerson p set p.name = 'renamed' where p.id = 22").executeUpdate()));
        inTransaction(s -> assertEquals("renamed", s.find(Person.class, 22L).name));
    }

    // ------------------------------------------------------------------
    // 중첩 @Struct
    // ------------------------------------------------------------------

    /**
     * {@code @Struct} 안의 {@code @Struct}.
     *
     * <p>Hibernate 는 안쪽 값을 {@code ValueBinder.getBindValue()} 로 가져가는데 기본 구현이
     * 도메인 객체를 그대로 돌려주어 {@code Object[]} 가 넘어간다. tbjdbc 는 중첩 자리에
     * {@link java.sql.Struct} 를 요구해 {@code JDBC-90651} 을 던진다.
     */
    @Test
    public void nested_struct_roundTrips() {
        inTransaction(s -> {
            Place p = new Place(); p.id = 1L;
            p.site = new Site(); p.site.label = "HQ";
            p.site.geo = new Geo(); p.site.geo.x = new BigDecimal("1.5000"); p.site.geo.y = new BigDecimal("2.5000");
            s.persist(p);
        });
        inTransaction(s -> {
            Place p = s.find(Place.class, 1L);
            assertEquals("HQ", p.site.label);
            assertEquals(0, new BigDecimal("1.5").compareTo(p.site.geo.x));
            assertEquals(0, new BigDecimal("2.5").compareTo(p.site.geo.y));
        });
    }

    @Test
    public void nested_struct_ddlCreatesBothTypes() {
        assertEquals(1L, count("select count(*) from user_types where type_name='SMT_GEO' and typecode='OBJECT'"));
        assertEquals(1L, count("select count(*) from user_types where type_name='SMT_SITE' and typecode='OBJECT'"));
        assertEquals("SMT_SITE.geo 속성이 SMT_GEO 여야 함", 1L, count(
                "select count(*) from user_type_attrs "
                        + "where type_name='SMT_SITE' and attr_name='GEO' and attr_type_name='SMT_GEO'"));
    }

    /** 중첩 안쪽이 null 인 경우 */
    @Test
    public void nested_struct_innerNull() {
        inTransaction(s -> {
            Place p = new Place(); p.id = 2L;
            p.site = new Site(); p.site.label = "no-geo"; p.site.geo = null;
            s.persist(p);
        });
        inTransaction(s -> {
            Place p = s.find(Place.class, 2L);
            assertEquals("no-geo", p.site.label);
            assertNull(p.site.geo);
        });
    }

    // ------------------------------------------------------------------
    // 다른 매핑 기능과의 조합
    // ------------------------------------------------------------------

    /** {@code @Version} 낙관적 락과 함께 써도 되는지 */
    @Test
    public void struct_withVersion_incrementsOnUpdate() {
        inTransaction(s -> {
            Place p = new Place(); p.id = 3L;
            p.site = new Site(); p.site.label = "v0";
            p.site.geo = new Geo(); p.site.geo.x = BigDecimal.ONE; p.site.geo.y = BigDecimal.ONE;
            s.persist(p);
        });
        inTransaction(s -> {
            Place p = s.find(Place.class, 3L);
            assertEquals(0L, p.ver);
            p.site.label = "v1";
        });
        inTransaction(s -> {
            Place p = s.find(Place.class, 3L);
            assertEquals("v1", p.site.label);
            assertEquals(1L, p.ver);
        });
    }



    // ------------------------------------------------------------------
    // 속성 타입 경계 — tbjdbc 가 STRUCT 안에서 못 다루는 것들
    // ------------------------------------------------------------------

    /**
     * boolean · enum 성분이 있어도 <b>check 제약을 만들지 않아야</b> 한다.
     *
     * <p>Hibernate 기본값({@code supportsComponentCheckConstraints() == true})으로 두면
     * {@code check (mix is null or (...))} 가 붙는데 Tibero 가 {@code JDBC-8147} 로 거부한다.
     * 그런데 {@code hbm2ddl.auto} 는 DDL 오류를 삼키므로 <b>테이블이 조용히 안 만들어지고</b>
     * 첫 질의에서 {@code JDBC-8033} 이 난다 — 이 테스트가 그 회귀를 막는다.
     *
     * @see com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport#supportsComponentCheckConstraints
     */
    @Test
    public void componentCheckConstraint_isSuppressed_soTableIsCreated() {
        // 성분 check 제약이 붙으면 create table 이 JDBC-8147 로 실패하고, hbm2ddl 이 그 오류를
        // 삼키므로 테이블이 아예 생기지 않는다. 따라서 "테이블이 있다" 가 곧 "제약이 안 붙었다" 다.
        assertEquals("SMT_FLAGGED 테이블이 만들어져야 함 — 없으면 성분 check 제약이 되살아난 것",
                1L, count("select count(*) from user_tables where table_name='SMT_FLAGGED'"));
        inTransaction(s -> {
            Flagged f = new Flagged(); f.id = 1L;
            f.mix = new Mix(); f.mix.flag = Boolean.TRUE; f.mix.kind = Kind.WORK;
            f.mix.memo = "plain"; f.mix.raw = new byte[]{1, 2, 3};
            s.persist(f);
        });
        inTransaction(s -> {
            Flagged f = s.find(Flagged.class, 1L);
            assertEquals(Boolean.TRUE, f.mix.flag);
            assertEquals(Kind.WORK, f.mix.kind);
            assertEquals("plain", f.mix.memo);
            assertArrayEquals(new byte[]{1, 2, 3}, f.mix.raw);
        });
    }

    /**
     * {@code binary_double} / {@code binary_float} 속성은 <b>기동 시점에</b> 거부되어야 한다.
     *
     * <p>tbjdbc 가 STRUCT 안의 이 타입을 바인딩도 추출도 못 한다({@code JDBC-590703}).
     * 검사가 없으면 DDL 은 만들어지고 첫 INSERT 에서 알 수 없는 오류가 난다.
     *
     * @see com.tmax.tibero.hibernate.type.TiberoStructJdbcType 지원 타입 표
     */
    @Test
    public void unsupportedAttribute_binaryFloatingPoint_failsAtBootstrap() {
        assertBootstrapRejects(BadDouble.class, "binary_double");
    }

    /** LOB 속성도 마찬가지다 ({@code JDBC-90651}). */
    @Test
    public void unsupportedAttribute_lob_failsAtBootstrap() {
        assertBootstrapRejects(BadLob.class, "LOB");
    }

    private void assertBootstrapRejects(Class<?> entity, String expectedInMessage) {
        // Environment.getProperties() 는 hibernate.properties 위에 -D 오버라이드를 얹은 것이라
        // 이 스위트와 같은 DB 를 가리킨다. sessionFactory().getProperties() 에는 접속 정보가 없다.
        org.hibernate.boot.registry.StandardServiceRegistry reg =
                new org.hibernate.boot.registry.StandardServiceRegistryBuilder()
                        .applySettings(org.hibernate.cfg.Environment.getProperties())
                        .applySetting("hibernate.dialect",
                                com.tmax.tibero.hibernate.dialect.TiberoDialect.class.getName())
                        .applySetting("hibernate.hbm2ddl.auto", "none")
                        .build();
        try {
            new org.hibernate.boot.MetadataSources(reg)
                    .addAnnotatedClass(entity).buildMetadata().buildSessionFactory().close();
            fail("지원하지 않는 속성 타입인데 기동에 성공함 — 드라이버가 고쳐졌다면 "
                    + "TiberoStructJdbcType 의 지원 타입 표를 갱신할 것");
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            assertTrue("메시지에 원인 타입이 드러나야 함: " + root.getMessage(),
                    String.valueOf(root.getMessage()).contains(expectedInMessage));
        } finally {
            org.hibernate.boot.registry.StandardServiceRegistryBuilder.destroy(reg);
        }
    }

    // ------------------------------------------------------------------

    private void seedSeoul(long id) {
        inTransaction(s -> {
            Person p = new Person(); p.id = id; p.name = "kim";
            p.addr = new Address(); p.addr.city = "Seoul"; p.addr.zip = 6236;
            s.persist(p);
        });
    }

    private long count(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
