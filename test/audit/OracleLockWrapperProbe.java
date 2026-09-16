package audit;

import jakarta.persistence.*;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import support.SqlCaptureInspector;

import java.util.List;

/**
 * OracleDialect 가 페이징+락에서 만들어내는 locking wrapper SQL 을 뽑아
 * 그 형태를 Tibero 가 받아들이는지 확인하는 수동 프로브.
 * (OracleDialect 로 부팅하되 접속은 Tibero — SQL 생성 형태만 보려는 목적)
 */
public class OracleLockWrapperProbe {

    @Entity(name = "OW_Item")
    @Table(name = "OW_ITEM")
    public static class Item {
        @Id Long id;
        String name;
    }

    private SessionFactory boot(String dialect) {
        var reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", dialect)
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url",
                        System.getProperty("hibernate.connection.url", "jdbc:tibero:thin:@localhost:9999:tibero"))
                .applySetting("hibernate.connection.username", "tibero")
                .applySetting("hibernate.connection.password", "tmax")
                .applySetting("hibernate.hbm2ddl.auto", "create-drop")
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", "false")
                .applySetting("hibernate.session_factory.statement_inspector", "support.SqlCaptureInspector")
                .build();
        return new MetadataSources(reg).addAnnotatedClass(Item.class)
                .buildMetadata().buildSessionFactory();
    }

    void oracleShape() {
        System.out.println("@@ ===== OracleDialect 로 같은 쿼리를 만들어 본다 =====");
        try (SessionFactory sf = boot("org.hibernate.dialect.OracleDialect")) {
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                for (long i = 1; i <= 5; i++) { Item it = new Item(); it.id = i; it.name = "n" + i; s.persist(it); }
                tx.commit();
            }
            SqlCaptureInspector.clear();
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                List<?> rs = s.createQuery("from OW_Item e order by e.id", Item.class)
                        .setMaxResults(3)
                        .setLockMode(LockModeType.PESSIMISTIC_WRITE)
                        .getResultList();
                tx.commit();
                System.out.println("@@ Oracle 형태 실행 성공 — 행 " + rs.size());
            } catch (Exception e) {
                System.out.println("@@ Oracle 형태 실행 실패 — " + rootMsg(e));
            }
            for (String sql : SqlCaptureInspector.getSqls()) System.out.println("@@   SQL: " + sql);
        } catch (Throwable t) {
            System.out.println("@@ Oracle dialect 부팅 실패 — " + rootMsg(t));
            for (String sql : SqlCaptureInspector.getSqls()) System.out.println("@@   SQL: " + sql);
        }
    }

    void tiberoAcceptsWrapperShape() {
        System.out.println("@@ ===== 래퍼 형태를 Tibero 에 직접 던져 본다 =====");
        try (SessionFactory sf = boot("com.tmax.tibero.hibernate.dialect.TiberoDialect")) {
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                for (long i = 1; i <= 5; i++) { Item it = new Item(); it.id = i; it.name = "n" + i; s.persist(it); }
                tx.commit();
            }
            probe(sf, "래퍼형 (offset/fetch 는 서브쿼리, for update 는 바깥)",
                    "select i.id, i.name from OW_ITEM i where i.id in "
                  + "(select id from (select id from OW_ITEM order by id) where rownum <= 3) for update");
            probe(sf, "래퍼형 · ANSI fetch 를 서브쿼리에",
                    "select i.id, i.name from OW_ITEM i where i.id in "
                  + "(select id from OW_ITEM order by id fetch first 3 rows only) for update");
            probe(sf, "(대조) 평면형 — 현재 거부되는 형태",
                    "select id, name from OW_ITEM order by id fetch first 3 rows only for update");
        } catch (Throwable t) {
            System.out.println("@@ FAIL " + rootMsg(t));
        }
    }

    private void probe(SessionFactory sf, String label, String sql) {
        try (var s = sf.openSession()) {
            var tx = s.beginTransaction();
            List<?> rs = s.createNativeQuery(sql, Object[].class).getResultList();
            tx.commit();
            System.out.println("@@ OK    " + label + "  → 행 " + rs.size());
        } catch (Exception e) {
            System.out.println("@@ FAIL  " + label + "  → " + rootMsg(e));
        }
    }

    private static String rootMsg(Throwable t) {
        Throwable c = t;
        while (c.getCause() != null) c = c.getCause();
        return c.getClass().getSimpleName() + ": " + String.valueOf(c.getMessage()).replaceAll("\\s+", " ");
    }

    /**
     * 수동 프로브 진입점.
     *
     * <p>audit 프로브는 DB 부하가 크고 판단 근거를 눈으로 보려는 것이라 자동 실행
     * 대상이 아니다. JUnit 애노테이션을 쓰면 {@code build.gradle} 의 실행 엔진 설정에
     * 따라 조용히 실행되거나 조용히 빠지므로, 13개 전부 {@code main()} 으로 통일한다.
     */
    public static void main(String[] args) throws Exception {
        new OracleLockWrapperProbe().oracleShape();
        new OracleLockWrapperProbe().tiberoAcceptsWrapperShape();
    }
}
