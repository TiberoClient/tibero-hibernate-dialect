package audit;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.SessionFactory;
import org.hibernate.annotations.Struct;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * {@code @Struct} 와 {@code @Embedded} 가 실제로 어떻게 다른지 ps06 에서 확인한다.
 *
 * <p>{@code docs/dialect-decisions.md} 는 "Spring 앱은 대부분 {@code @Embedded} 로 충분하다"
 * 고 적어두었는데, 이것이 <b>완전 대체</b>를 뜻하는지 확인이 필요했다.
 * 결론부터 — 두 어노테이션은 <b>DB 스키마 모양 자체가 다르다</b>.
 *
 * <pre>실행: java -cp &lt;test runtime classpath&gt; audit.StructVsEmbeddedProbe</pre>
 */
public class StructVsEmbeddedProbe {

    static final String URL = "jdbc:tibero:thin:@localhost:9999:tibero";
    static final String USER = "tibero";
    static final String PASS = "tmax";

    @Embeddable
    public static class PlainAddress {
        @Column(name = "STREET") public String street;
        @Column(name = "CITY")   public String city;
    }

    @Embeddable
    @Struct(name = "SVE_ADDR_TYPE")
    public static class StructAddress {
        public String street;
        public String city;
    }

    @Entity(name = "SveEmbedded") @Table(name = "SVE_EMBEDDED")
    public static class EmbeddedEntity {
        @Id public Long id;
        @Embedded public PlainAddress addr;
    }

    @Entity(name = "SveStruct") @Table(name = "SVE_STRUCT")
    public static class StructEntity {
        @Id public Long id;
        public StructAddress addr;
    }

    public static void main(String[] args) throws Exception {
        cleanup();

        System.out.println("=== [1] @Embedded — 지금 되는가 ===");
        boot(EmbeddedEntity.class);
        showColumns("SVE_EMBEDDED");

        System.out.println();
        System.out.println("=== [2] @Struct — 지금 되는가 ===");
        boot(StructEntity.class);
        showColumns("SVE_STRUCT");

        System.out.println();
        System.out.println("=== [3] DB 자체는 object UDT 를 받는가 (dialect 와 별개) ===");
        try (Connection c = DriverManager.getConnection(URL, USER, PASS)) {
            exec(c, "create or replace type SVE_ADDR_TYPE as object (street varchar2(255), city varchar2(255))");
            exec(c, "create table SVE_MANUAL (id number primary key, addr SVE_ADDR_TYPE)");
            exec(c, "insert into SVE_MANUAL values (1, SVE_ADDR_TYPE('Gangnam-daero', 'Seoul'))");
            dump(c, "select id, a.addr.street, a.addr.city from SVE_MANUAL a");
        }
        showColumns("SVE_MANUAL");

        cleanup();
    }

    static void boot(Class<?> entity) {
        StandardServiceRegistry registry = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", "com.tmax.tibero.hibernate.dialect.TiberoDialect")
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url", URL)
                .applySetting("hibernate.connection.username", USER)
                .applySetting("hibernate.connection.password", PASS)
                .applySetting("hibernate.hbm2ddl.auto", "create")
                .applySetting("hibernate.show_sql", "true")
                .applySetting("hibernate.format_sql", "false")
                .build();
        try (SessionFactory sf = new MetadataSources(registry)
                .addAnnotatedClass(entity).buildMetadata().buildSessionFactory()) {
            System.out.println("  부팅 OK — " + entity.getSimpleName());
        } catch (RuntimeException e) {
            Throwable root = e;
            while (root.getCause() != null) root = root.getCause();
            System.out.println("  부팅 FAIL — " + root.getClass().getName());
            System.out.println("            " + String.valueOf(root.getMessage()).trim().split("\n")[0]);
            StandardServiceRegistryBuilder.destroy(registry);
        }
    }

    static void showColumns(String table) {
        try (Connection c = DriverManager.getConnection(URL, USER, PASS)) {
            dump(c, "select column_name, data_type from user_tab_columns "
                    + "where table_name='" + table + "' order by column_id");
        } catch (SQLException e) {
            System.out.println("        FAIL " + e.getMessage().trim().split("\n")[0]);
        }
    }

    static void cleanup() throws SQLException {
        try (Connection c = DriverManager.getConnection(URL, USER, PASS)) {
            for (String t : new String[]{"SVE_EMBEDDED", "SVE_STRUCT", "SVE_MANUAL"}) {
                try (Statement s = c.createStatement()) { s.execute("drop table " + t); } catch (SQLException ignore) {}
            }
            try (Statement s = c.createStatement()) { s.execute("drop type SVE_ADDR_TYPE"); } catch (SQLException ignore) {}
        }
    }

    static void exec(Connection c, String sql) {
        try (Statement s = c.createStatement()) { s.execute(sql); System.out.println("  OK    " + sql); }
        catch (SQLException e) { System.out.println("  FAIL  " + sql + "\n        -> " + e.getMessage().trim().split("\n")[0]); }
    }

    static void dump(Connection c, String sql) {
        try (Statement s = c.createStatement(); ResultSet r = s.executeQuery(sql)) {
            int n = r.getMetaData().getColumnCount();
            boolean any = false;
            while (r.next()) {
                any = true;
                StringBuilder b = new StringBuilder("        ");
                for (int i = 1; i <= n; i++) b.append(r.getMetaData().getColumnLabel(i)).append('=').append(r.getString(i)).append("  ");
                System.out.println(b);
            }
            if (!any) System.out.println("        (없음)");
        } catch (SQLException e) { System.out.println("        FAIL " + e.getMessage().trim().split("\n")[0]); }
    }
}
