package audit;

import jakarta.persistence.*;
import org.hibernate.SessionFactory;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.type.SqlTypes;

import java.util.*;

/** JSON 컬럼이 FormatMapper 가 있을 때 실제로 왕복되는지 확인하는 수동 프로브. */
public class JsonRoundTripProbe {

    @Entity(name = "JP_Doc")
    @Table(name = "JP_DOC")
    public static class Doc {
        @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
        Long id;
        @JdbcTypeCode(SqlTypes.JSON)
        Map<String, Object> payload;
        @JdbcTypeCode(SqlTypes.JSON)
        Addr addr;
    }

    public static class Addr {
        public String city;
        public int zip;
        public Addr() {}
        public Addr(String city, int zip) { this.city = city; this.zip = zip; }
    }

    private SessionFactory boot() {
        var reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", "com.tmax.tibero.hibernate.dialect.TiberoDialect")
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url",
                        System.getProperty("hibernate.connection.url", "jdbc:tibero:thin:@localhost:9999:tibero"))
                .applySetting("hibernate.connection.username", "tibero")
                .applySetting("hibernate.connection.password", "tmax")
                .applySetting("hibernate.hbm2ddl.auto", "create-drop")
                .applySetting("hibernate.show_sql", "true")
                .applySetting("hibernate.format_sql", "false")
                .build();
        return new MetadataSources(reg).addAnnotatedClass(Doc.class)
                .buildMetadata().buildSessionFactory();
    }

    void jsonRoundTrip() {
        System.out.println("@@ jackson on classpath = " + hasJackson());
        try (SessionFactory sf = boot()) {
            System.out.println("@@ FormatMapper = " + sf.getSessionFactoryOptions().getJsonFormatMapper());

            Long id;
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                Doc d = new Doc();
                d.payload = new LinkedHashMap<>(Map.of("k", "v", "n", 42));
                d.addr = new Addr("Seoul", 6236);
                s.persist(d);
                tx.commit();
                id = d.id;
            }
            System.out.println("@@ INSERT OK id=" + id);

            try (var s = sf.openSession()) {
                Doc r = s.find(Doc.class, id);
                System.out.println("@@ payload = " + r.payload);
                System.out.println("@@ addr    = " + r.addr.city + "/" + r.addr.zip);
            }

            try (var s = sf.openSession()) {
                Object raw = s.createNativeQuery(
                        "select data_type from user_tab_columns where table_name='JP_DOC' and column_name='PAYLOAD'",
                        Object.class).getSingleResult();
                System.out.println("@@ PAYLOAD 컬럼 타입 = " + raw);
                Object v = s.createNativeQuery(
                        "select cast(payload as varchar2(4000)) from JP_DOC", Object.class).getSingleResult();
                System.out.println("@@ 저장된 원문 = " + v);
            } catch (Exception e) {
                System.out.println("@@ 카탈로그/원문 조회 실패: " + e.getClass().getSimpleName() + " " + e.getMessage());
            }
        } catch (Throwable t) {
            System.out.println("@@ FAIL " + t.getClass().getName() + " : " + t.getMessage());
            Throwable c = t;
            while ((c = c.getCause()) != null) System.out.println("@@   cause " + c.getClass().getName() + " : " + c.getMessage());
        }
    }

    private static boolean hasJackson() {
        try { Class.forName("com.fasterxml.jackson.databind.ObjectMapper"); return true; }
        catch (Throwable t) { return false; }
    }

    /**
     * 수동 프로브 진입점.
     *
     * <p>audit 프로브는 DB 부하가 크고 판단 근거를 눈으로 보려는 것이라 자동 실행
     * 대상이 아니다. JUnit 애노테이션을 쓰면 {@code build.gradle} 의 실행 엔진 설정에
     * 따라 조용히 실행되거나 조용히 빠지므로, 13개 전부 {@code main()} 으로 통일한다.
     */
    public static void main(String[] args) throws Exception {
        new JsonRoundTripProbe().jsonRoundTrip();
    }
}
