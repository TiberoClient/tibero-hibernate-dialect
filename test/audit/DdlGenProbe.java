package audit;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.SessionFactory;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;

/**
 * 전수조사 보조: 실제 엔티티 필드가 Tibero 에 어떤 컬럼 타입으로 생성되는지 확인한다.
 * (DdlTypeRegistry 단위 단언과 실제 매핑 경로가 다를 수 있으므로 DB 에 직접 만들어 확인)
 */
public class DdlGenProbe {

    public static void main(String[] args) {
        StandardServiceRegistry reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", TiberoDialect.class.getName())
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url", "jdbc:tibero:thin:@localhost:8888:tibero")
                .applySetting("hibernate.connection.username", "tibero")
                .applySetting("hibernate.connection.password", "tmax")
                .applySetting("hibernate.hbm2ddl.auto", "create")
                .applySetting("hibernate.show_sql", "true")
                .applySetting("hibernate.format_sql", "false")
                .build();

        try (SessionFactory sf = new MetadataSources(reg)
                .addAnnotatedClass(NumEntity.class)
                .buildMetadata()
                .buildSessionFactory()) {

            sf.inSession(s -> {
                System.out.println();
                System.out.println("=== NUM_ENTITY 실제 컬럼 타입 (Tibero) ===");
                s.doWork(c -> {
                    try (var st = c.createStatement();
                         var rs = st.executeQuery(
                                 "select column_name, data_type, data_precision, data_scale, data_length " +
                                         "from user_tab_columns where table_name='NUM_ENTITY' order by column_id")) {
                        System.out.printf("  %-10s %-16s %-6s %-6s %s%n", "COLUMN", "TYPE", "PREC", "SCALE", "LEN");
                        while (rs.next()) {
                            System.out.printf("  %-10s %-16s %-6s %-6s %s%n",
                                    rs.getString(1), rs.getString(2), rs.getObject(3), rs.getObject(4), rs.getObject(5));
                        }
                    }
                });
            });
        }
        reg.close();
    }

    @Entity(name = "NumEntity")
    @Table(name = "NUM_ENTITY")
    public static class NumEntity {
        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "D_PRIM")
        public double dPrim;

        @Column(name = "D_WRAP")
        public Double dWrap;

        @Column(name = "F_PRIM")
        public float fPrim;

        @Column(name = "F_WRAP")
        public Float fWrap;

        @Column(name = "BD_VAL", precision = 20, scale = 4)
        public java.math.BigDecimal bdVal;
    }
}
