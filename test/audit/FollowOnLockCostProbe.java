package audit;

import jakarta.persistence.*;
import jakarta.persistence.LockModeType;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import support.SqlCaptureInspector;

import java.util.List;

/** 페이징 + 비관적 락에서 follow-on locking 이 실제로 몇 번 쿼리하는지 재는 수동 프로브. */
public class FollowOnLockCostProbe {

    @Entity(name = "FL_Item")
    @Table(name = "FL_ITEM")
    public static class Item {
        @Id Long id;
        String name;
        @Version long ver;          // 버전 유무에 따른 차이도 보기 위해
    }

    @Entity(name = "FL_Plain")
    @Table(name = "FL_PLAIN")
    public static class Plain {
        @Id Long id;
        String name;                // @Version 없음
    }

    private SessionFactory boot(Class<?>... cs) {
        var reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", "com.tmax.tibero.hibernate.dialect.TiberoDialect")
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url",
                        System.getProperty("hibernate.connection.url", "jdbc:tibero:thin:@localhost:9999:tibero"))
                .applySetting("hibernate.connection.username", "tibero")
                .applySetting("hibernate.connection.password", "tmax")
                .applySetting("hibernate.hbm2ddl.auto", "create-drop")
                .applySetting("hibernate.session_factory.statement_inspector",
                        "support.SqlCaptureInspector")
                .build();
        var ms = new MetadataSources(reg);
        for (Class<?> c : cs) ms.addAnnotatedClass(c);
        return ms.buildMetadata().buildSessionFactory();
    }

    void measure() {
        try (SessionFactory sf = boot(Item.class, Plain.class)) {
            try (var s = sf.openSession()) {
                var tx = s.beginTransaction();
                for (long i = 1; i <= 10; i++) {
                    Item it = new Item(); it.id = i; it.name = "n" + i; s.persist(it);
                    Plain p = new Plain(); p.id = i; p.name = "n" + i; s.persist(p);
                }
                tx.commit();
            }

            run(sf, "① 락 없이 페이징 3건", false, 3, Item.class);
            run(sf, "② 페이징 3건 + PESSIMISTIC_WRITE (@Version 있음)", true, 3, Item.class);
            run(sf, "③ 페이징 5건 + PESSIMISTIC_WRITE (@Version 있음)", true, 5, Item.class);
            run(sf, "④ 페이징 3건 + PESSIMISTIC_WRITE (@Version 없음)", true, 3, Plain.class);
            run(sf, "⑤ 페이징 없이 전체 + PESSIMISTIC_WRITE", true, -1, Item.class);
        } catch (Throwable t) {
            System.out.println("@@ FAIL " + t.getClass().getName() + " : " + t.getMessage());
        }
    }

    private void run(SessionFactory sf, String label, boolean lock, int max, Class<?> entity) {
        SqlCaptureInspector.clear();
        try (var s = sf.openSession()) {
            var tx = s.beginTransaction();
            var q = s.createQuery("from " + (entity == Item.class ? "FL_Item" : "FL_Plain") + " e order by e.id", entity);
            if (max > 0) q.setMaxResults(max);
            if (lock) q.setLockMode(LockModeType.PESSIMISTIC_WRITE);
            List<?> rs = q.getResultList();
            tx.commit();
            List<String> sqls = SqlCaptureInspector.getSqls();
            System.out.println("@@ " + label + "  → 행 " + rs.size() + " / SQL " + sqls.size() + "회");
            for (String sql : sqls) System.out.println("@@     " + sql);
        } catch (Exception e) {
            System.out.println("@@ " + label + "  → 예외 " + e.getClass().getSimpleName() + " : " + e.getMessage());
        }
    }

    /**
     * 수동 프로브 진입점.
     *
     * <p>audit 프로브는 DB 부하가 크고 판단 근거를 눈으로 보려는 것이라 자동 실행
     * 대상이 아니다. JUnit 애노테이션을 쓰면 {@code build.gradle} 의 실행 엔진 설정에
     * 따라 조용히 실행되거나 조용히 빠지므로, 13개 전부 {@code main()} 으로 통일한다.
     */
    public static void main(String[] args) throws Exception {
        new FollowOnLockCostProbe().measure();
    }
}
