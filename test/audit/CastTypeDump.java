package audit;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.boot.spi.MetadataImplementor;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.sql.DdlType;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;

import java.lang.reflect.Field;

/**
 * 전수조사 보조: DDL 타입명과 CAST 대상 타입명이 어떻게 다른지 덤프한다.
 */
public class CastTypeDump {

    public static void main(String[] args) {
        StandardServiceRegistry reg = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", TiberoDialect.class.getName())
                .applySetting("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver")
                .applySetting("hibernate.connection.url", "jdbc:tibero:thin:@localhost:8888:tibero")
                .applySetting("hibernate.connection.username", "tibero")
                .applySetting("hibernate.connection.password", "tmax")
                .build();

        MetadataImplementor md = (MetadataImplementor) new MetadataSources(reg).buildMetadata();
        DdlTypeRegistry ddl = md.getTypeConfiguration().getDdlTypeRegistry();

        int[] codes = {
                SqlTypes.BOOLEAN, SqlTypes.TINYINT, SqlTypes.SMALLINT, SqlTypes.INTEGER, SqlTypes.BIGINT,
                SqlTypes.REAL, SqlTypes.FLOAT, SqlTypes.DOUBLE, SqlTypes.NUMERIC, SqlTypes.DECIMAL,
                SqlTypes.CHAR, SqlTypes.VARCHAR, SqlTypes.NVARCHAR, SqlTypes.CLOB,
                SqlTypes.BINARY, SqlTypes.VARBINARY, SqlTypes.BLOB,
                SqlTypes.DATE, SqlTypes.TIME, SqlTypes.TIMESTAMP,
                SqlTypes.TIME_WITH_TIMEZONE, SqlTypes.TIMESTAMP_WITH_TIMEZONE
        };

        System.out.printf("%-28s %-30s %s%n", "SqlType", "DDL typeName", "CAST typeName");
        System.out.println("-".repeat(90));
        for (int c : codes) {
            DdlType t = ddl.getDescriptor(c);
            String name = t == null ? "(none)" : safe(() -> t.getTypeName(Size.nil(), null, ddl));
            String cast = t == null ? "(none)" : safe(() -> t.getCastTypeName(Size.nil(), null, ddl));
            System.out.printf("%-28s %-30s %s%n", nameOf(c), name, cast);
        }

        reg.close();
    }

    interface S { String get(); }

    static String safe(S s) {
        try { return s.get(); } catch (Throwable e) { return "EX:" + e.getClass().getSimpleName() + ":" + e.getMessage(); }
    }

    static String nameOf(int code) {
        for (Field f : SqlTypes.class.getFields()) {
            try {
                if (f.getType() == int.class && f.getInt(null) == code) return f.getName() + "(" + code + ")";
            } catch (Exception ignored) {}
        }
        return String.valueOf(code);
    }
}
