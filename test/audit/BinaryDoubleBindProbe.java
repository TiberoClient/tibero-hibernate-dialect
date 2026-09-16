package audit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.Types;

/**
 * 전수조사 보조: BINARY_DOUBLE 컬럼에 IEEE 극값을 바인딩하는 경로별 동작 확인.
 * (Double.MAX_VALUE 가 binary_double 컬럼에도 overflow 로 거부되는 원인 추적)
 */
public class BinaryDoubleBindProbe {

    public static void main(String[] args) throws Exception {
        Class.forName("com.tmax.tibero.jdbc.TbDriver");
        try (Connection c = DriverManager.getConnection("jdbc:tibero:thin:@localhost:8888:tibero", "tibero", "tmax")) {
            try (Statement s = c.createStatement()) {
                try { s.execute("drop table BD_T"); } catch (Exception ignored) {}
                s.execute("create table BD_T (id number(10), bd binary_double)");
            }

            double[] vals = {Math.PI, 1e300, Double.MAX_VALUE, Double.MIN_NORMAL, Double.MIN_VALUE};

            System.out.println("=== [A] setDouble ===");
            for (double v : vals) System.out.printf("  %-26s : %s%n", v, bind(c, v, "setDouble"));

            System.out.println();
            System.out.println("=== [B] setObject(v, Types.DOUBLE) ===");
            for (double v : vals) System.out.printf("  %-26s : %s%n", v, bind(c, v, "setObjectTyped"));

            System.out.println();
            System.out.println("=== [C] setObject(v) ===");
            for (double v : vals) System.out.printf("  %-26s : %s%n", v, bind(c, v, "setObject"));

            System.out.println();
            System.out.println("=== [D] SQL 리터럴 + 접미사 ===");
            for (String lit : new String[]{"1.7976931348623157E308", "1.7976931348623157E308d",
                    "1.7976931348623157E+308", "binary_double_infinity", "1e300"}) {
                try (Statement s = c.createStatement()) {
                    s.executeUpdate("delete from BD_T");
                    s.executeUpdate("insert into BD_T (id,bd) values (1," + lit + ")");
                    try (ResultSet rs = s.executeQuery("select bd from BD_T where id=1")) {
                        rs.next();
                        System.out.printf("  %-30s : OK -> %s%n", lit, rs.getDouble(1));
                    }
                } catch (Exception e) {
                    System.out.printf("  %-30s : FAIL %s%n", lit, e.getMessage().replaceAll("\\s+", " "));
                }
            }

            try (Statement s = c.createStatement()) { s.execute("drop table BD_T"); }
        }
    }

    static String bind(Connection c, double v, String how) {
        try (Statement s = c.createStatement()) {
            s.executeUpdate("delete from BD_T");
        } catch (Exception ignored) {}

        try (PreparedStatement ps = c.prepareStatement("insert into BD_T (id,bd) values (1,?)")) {
            switch (how) {
                case "setDouble":
                    ps.setDouble(1, v);
                    break;
                case "setObjectTyped":
                    ps.setObject(1, v, Types.DOUBLE);
                    break;
                default:
                    ps.setObject(1, v);
                    break;
            }
            ps.executeUpdate();
        } catch (Exception e) {
            return "FAIL " + e.getMessage().replaceAll("\\s+", " ");
        }

        try (PreparedStatement ps = c.prepareStatement("select bd from BD_T where id=1");
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            double got = rs.getDouble(1);
            return (got == v ? "OK 무손실 -> " : "OK 손실 -> ") + got;
        } catch (Exception e) {
            return "READ FAIL " + e.getMessage();
        }
    }
}
