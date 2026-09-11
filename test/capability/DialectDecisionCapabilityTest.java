package capability;

import support.AbstractTiberoDialectTestBase;
import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.annotations.Struct;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.cfg.Configuration;
import org.junit.Test;

import java.io.InputStream;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * Dialect 판단의 DB 실측 고정.
 *
 * - 미지원: Tibero가 여전히 해당 DDL을 거절하는지 (ANSI array 문법, enum domain)
 * - 검증 완료 경로: @Enumerated, int[] 왕복, Oracle식 array UDT 수용
 * - 구현됨: @Struct · 네이티브 배열 컬럼 — 전환 시 여기 가드가 먼저 실패한다
 *
 * 실패하면 DB/드라이버/Hibernate 스펙 변경 가능성이 있으므로
 * 코드를 고치기 전에 docs/dialect-decisions.md 를 갱신한다.
 *
 * @see docs/dialect-decisions.md
 */
public class DialectDecisionCapabilityTest extends AbstractTiberoDialectTestBase {

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{EnumEntity.class, ArrayEntity.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    // -------------------------------------------------------------------------
    // 미지원 — DB가 거부해야 함
    // -------------------------------------------------------------------------

    @Test
    public void unsupported_ansiSqlArrayDdl_stillRejectedByTibero() {
        // ANSI 표준 array 문법만 거부된다. Oracle Dialect 는 이 문법을 쓰지 않으므로
        // 이 테스트는 "array 전체 미지원"의 근거가 아니다 — 아래 UDT 테스트를 함께 볼 것.
        assertNativeFails(
                "create table DEC_ARR_T (c number array)",
                "ANSI SQL array 미지원 판단이 깨짐 — docs/dialect-decisions.md §2 재검토");
        assertNativeFails(
                "create table DEC_ARR_T2 (c int array[10])",
                "ANSI SQL array[n] 미지원 판단이 깨짐 — docs/dialect-decisions.md §2 재검토");
    }

    /**
     * Oracle Dialect 가 실제로 쓰는 array 경로는 UDT 다.
     *
     * <p><b>이제 이 경로를 dialect 가 실제로 쓴다</b> — 이 테스트는 DB 가 그 DDL 을
     * 계속 받아주는지만 지킨다. 매핑 동작은 {@code capability.ArrayMappingTest} 가 본다.
     * {@code OracleUserDefinedTypeExporter} 가 {@code as varying array(n) of …} /
     * {@code as table of …} 를 내고, JDBC 는 {@code createArrayOf} 로 왕복한다.
     * Tibero 가 이를 수용하므로 array 는 "미지원"이 아니라 "미구현"이다.
     *
     * @see docs/dialect-decisions.md §1.2, §2
     */
    @Test
    public void unimplemented_oracleStyleArrayUdt_isAcceptedByTibero() {
        try {
            assertNativeOk("create or replace type DEC_VARR_T as varying array(10) of number",
                    "Oracle식 VARRAY UDT 가 거부됨 — docs/dialect-decisions.md §1.2/§2 재검토");
            assertNativeOk("create or replace type DEC_NTBL_T as table of number",
                    "Oracle식 nested table UDT 가 거부됨 — docs/dialect-decisions.md §1.2/§2 재검토");
            assertNativeOk("create table DEC_VARR_TBL (id number primary key, v DEC_VARR_T)",
                    "VARRAY 를 컬럼 타입으로 쓸 수 없음 — docs/dialect-decisions.md §2 재검토");
            assertNativeOk("insert into DEC_VARR_TBL values (1, DEC_VARR_T(10,20,30))",
                    "VARRAY 생성자 리터럴 insert 실패 — docs/dialect-decisions.md §2 재검토");

            Long cnt = inTransactionReturning(session -> session.createNativeQuery(
                    "select count(*) from DEC_VARR_TBL t, table(t.v)", Long.class).getSingleResult());
            assertEquals("TABLE() 언네스트 결과가 3이어야 함", Long.valueOf(3L), cnt);

            String collType = inTransactionReturning(session -> session.createNativeQuery(
                    "select coll_type from user_coll_types where type_name='DEC_VARR_T'", String.class)
                    .getSingleResult());
            assertEquals("VARYING ARRAY", collType);
        } finally {
            dropTableWithRetry("DEC_VARR_TBL");
            for (String t : new String[]{"DEC_VARR_T", "DEC_NTBL_T"}) {
                try {
                    inTransaction(session -> session.createNativeMutationQuery("drop type " + t).executeUpdate());
                } catch (Exception ignored) {}
            }
        }
    }

    @Test
    public void unsupported_enumDomainDdl_stillRejectedByTibero() {
        assertNativeFails(
                "create domain dec_color as enum (R, G)",
                "enum domain 미지원 판단이 깨짐 — docs/dialect-decisions.md §1.1 재검토");
    }

    // -------------------------------------------------------------------------
    // 검증 완료 경로 — 계속 통과해야 함
    // -------------------------------------------------------------------------

    @Test
    public void verified_enumerated_stringAndOrdinal_roundTrip() {
        inTransaction(session -> {
            EnumEntity e = new EnumEntity();
            e.id = 1L;
            e.asString = Color.RED;
            e.asOrdinal = Color.GREEN;
            session.persist(e);
        });
        inTransaction(session -> {
            EnumEntity e = session.get(EnumEntity.class, 1L);
            assertEquals(Color.RED, e.asString);
            assertEquals(Color.GREEN, e.asOrdinal);
        });
    }

    @Test
    public void verified_intArray_varbinaryPath_roundTrip() {
        inTransaction(session -> {
            ArrayEntity e = new ArrayEntity();
            e.id = 1L;
            e.nums = new int[]{1, 2, 3};
            session.persist(e);
        });
        inTransaction(session -> {
            ArrayEntity e = session.get(ArrayEntity.class, 1L);
            assertArrayEquals(new int[]{1, 2, 3}, e.nums);
        });
    }

    // -------------------------------------------------------------------------
    // @Struct — 지원 추가됨. 이전에는 여기서 fail-fast 를 고정하고 있었다
    // -------------------------------------------------------------------------

    /**
     * {@code @Struct} 엔티티로 SessionFactory 가 기동되는지만 본다.
     *
     * <p>지원 전에는 이 자리에 {@code unimplemented_structMapping_stillFailsFastAtBootstrap}
     * 이 있었다 — 기동이 {@code UnsupportedOperationException} 으로 실패하는 것을 고정해
     * "미구현"이라는 판단을 지키는 가드였다. 지원을 넣으면서 지원 시나리오로 뒤집었다.
     *
     * <p>매핑·값 왕복까지 보는 것은 {@code capability.StructMappingTest} 다.
     * 여기서는 {@code docs/dialect-decisions.md} 의 분류가 코드와 어긋나지 않는지만 지킨다.
     */
    @Test
    public void struct_mapping_bootsSuccessfully() throws Exception {
        Properties props = loadHibernateProperties();
        StandardServiceRegistry reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", com.tmax.tibero.hibernate.dialect.TiberoDialect.class.getName())
                .applySetting("hibernate.connection.driver_class", setting(props, "hibernate.connection.driver_class"))
                .applySetting("hibernate.connection.url", setting(props, "hibernate.connection.url"))
                .applySetting("hibernate.connection.username", setting(props, "hibernate.connection.username"))
                .applySetting("hibernate.connection.password", setting(props, "hibernate.connection.password"))
                .applySetting("hibernate.hbm2ddl.auto", "none")
                .build();
        try {
            new MetadataSources(reg)
                    .addAnnotatedClass(StructPerson.class)
                    .addAnnotatedClass(StructAddress.class)
                    .buildMetadata()
                    .buildSessionFactory()
                    .close();
        } catch (Exception e) {
            Throwable root = e;
            while (root.getCause() != null) {
                root = root.getCause();
            }
            fail("@Struct 기동이 실패함 — getAggregateSupport / getCreateUserDefinedTypeKindString "
                    + "오버라이드가 빠졌을 수 있다. 실제: " + root);
        } finally {
            StandardServiceRegistryBuilder.destroy(reg);
        }
    }

    /**
     * 접속 설정 한 건 — <b>커맨드라인 {@code -D} 를 파일보다 우선</b>한다.
     *
     * <p>이 테스트는 {@code BaseCoreFunctionalTestBase} 를 거치지 않고 레지스트리를 직접
     * 만든다. 그래서 {@code hibernate.properties} 만 읽으면 {@code init-test.gradle} 이
     * 넘겨 준 {@code -Dhibernate.connection.url=...} 이 <b>조용히 무시되고</b> 파일에 적힌
     * 인스턴스로 붙는다 — 다른 테스트는 9999 를 쓰는데 이 테스트만 딴 곳을 보는,
     * 알아차리기 어려운 형태다. 시스템 프로퍼티를 먼저 본다.
     */
    private static String setting(Properties props, String key) {
        return System.getProperty(key, props.getProperty(key));
    }

    private static Properties loadHibernateProperties() throws Exception {
        Properties props = new Properties();
        try (InputStream in = DialectDecisionCapabilityTest.class.getClassLoader()
                .getResourceAsStream("hibernate.properties")) {
            assertNotNull("test/resources/hibernate.properties 필요", in);
            props.load(in);
        }
        return props;
    }

    // -------------------------------------------------------------------------

    private void assertNativeOk(String sql, String revisitHint) {
        try {
            inTransaction(session -> session.createNativeMutationQuery(sql).executeUpdate());
        } catch (Exception e) {
            fail(revisitHint + " SQL=" + sql + " / " + e.getMessage());
        }
    }

    private void assertNativeFails(String sql, String revisitHint) {
        try {
            inTransaction(session -> session.createNativeMutationQuery(sql).executeUpdate());
            fail(revisitHint + " SQL=" + sql);
        } catch (Exception expected) {
            // OK — still unsupported
        }
    }

    public enum Color { RED, GREEN, BLUE }

    @Entity(name = "DecEnumEntity")
    @Table(name = "DEC_ENUM_E")
    public static class EnumEntity {
        @Id
        public Long id;
        @Enumerated(EnumType.STRING)
        @Column(name = "AS_STR")
        public Color asString;
        @Enumerated(EnumType.ORDINAL)
        @Column(name = "AS_ORD")
        public Color asOrdinal;
    }

    @Entity(name = "DecArrayEntity")
    @Table(name = "DEC_ARR_E")
    public static class ArrayEntity {
        @Id
        public Long id;
        public int[] nums;
    }

    @Entity
    @Table(name = "DEC_STRUCT_PERSON")
    public static class StructPerson {
        @Id
        public Long id;
        public StructAddress address;
    }

    @Embeddable
    @Struct(name = "DEC_STRUCT_ADDR")
    public static class StructAddress {
        @Column(name = "CITY")
        public String city;
    }
}
