package audit;

import jakarta.persistence.*;
import org.hibernate.SessionFactory;
import org.hibernate.annotations.JdbcTypeCode;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.type.SqlTypes;

/** JSON 컬럼에 String 을 매핑해도 FormatMapper 가 필요한지 확인하는 수동 프로브. */
public class JsonStringProbe {

    @Entity(name = "JS_Doc")
    @Table(name = "JS_DOC")
    public static class Doc {
        @Id Long id;
        @JdbcTypeCode(SqlTypes.JSON)
        String payload;              // ← POJO 가 아니라 String
    }

    /** @JdbcTypeCode 없이 columnDefinition 으로만 json 컬럼을 만드는 우회 경로. */
    @Entity(name = "JS_Doc2")
    @Table(name = "JS_DOC2")
    public static class Doc2 {
        @Id Long id;
        @Column(columnDefinition = "json")
        String payload;
    }

    void columnDefinitionJson() {
        System.out.println("@@ [우회] jackson = " + has("com.fasterxml.jackson.databind.ObjectMapper"));
        try (SessionFactory sf = boot2()) {
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                Doc2 d = new Doc2(); d.id = 1L; d.payload = "{\"a\":1}";
                s.persist(d); tx.commit();
            }
            try (var s = sf.openSession()) {
                System.out.println("@@ [우회] 읽은 값 = " + s.find(Doc2.class, 1L).payload);
                System.out.println("@@ [우회] 컬럼 타입 = " + s.createNativeQuery(
                    "select data_type from user_tab_columns where table_name='JS_DOC2' and column_name='PAYLOAD'",
                    Object.class).getSingleResult());
            }
        } catch (Throwable t) {
            System.out.println("@@ [우회] FAIL " + t.getClass().getName() + " : " + t.getMessage());
            Throwable c = t;
            while ((c = c.getCause()) != null)
                System.out.println("@@   [우회] cause " + c.getClass().getName() + " : " + c.getMessage());
        }
    }

    private SessionFactory boot2() { return build(Doc2.class); }
    private SessionFactory boot()  { return build(Doc.class); }

    private SessionFactory build(Class<?> entity) {
        var reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", "com.tmax.tibero.hibernate.dialect.TiberoDialect")
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url",
                        System.getProperty("hibernate.connection.url", "jdbc:tibero:thin:@localhost:9999:tibero"))
                .applySetting("hibernate.connection.username", "tibero")
                .applySetting("hibernate.connection.password", "tmax")
                .applySetting("hibernate.hbm2ddl.auto", "create-drop")
                .build();
        return new MetadataSources(reg).addAnnotatedClass(entity)
                .buildMetadata().buildSessionFactory();
    }

    void stringJson() {
        System.out.println("@@ jackson = " + has("com.fasterxml.jackson.databind.ObjectMapper"));
        try (SessionFactory sf = boot()) {
            System.out.println("@@ FormatMapper = " + sf.getSessionFactoryOptions().getJsonFormatMapper());
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                Doc d = new Doc(); d.id = 1L; d.payload = "{\"a\":1}";
                s.persist(d);
                tx.commit();
            }
            System.out.println("@@ INSERT OK");
            try (var s = sf.openSession()) {
                System.out.println("@@ 읽은 값 = " + s.find(Doc.class, 1L).payload);
            }
        } catch (Throwable t) {
            System.out.println("@@ FAIL " + t.getClass().getName() + " : " + t.getMessage());
            Throwable c = t;
            while ((c = c.getCause()) != null)
                System.out.println("@@   cause " + c.getClass().getName() + " : " + c.getMessage());
        }
    }

    private static boolean has(String cn) {
        try { Class.forName(cn); return true; } catch (Throwable t) { return false; }
    }

    /**
     * 수동 프로브 진입점.
     *
     * <p>audit 프로브는 DB 부하가 크고 판단 근거를 눈으로 보려는 것이라 자동 실행
     * 대상이 아니다. JUnit 애노테이션을 쓰면 {@code build.gradle} 의 실행 엔진 설정에
     * 따라 조용히 실행되거나 조용히 빠지므로, 13개 전부 {@code main()} 으로 통일한다.
     */
    public static void main(String[] args) throws Exception {
        new JsonStringProbe().columnDefinitionJson();
        new JsonStringProbe().stringJson();
    }
}
