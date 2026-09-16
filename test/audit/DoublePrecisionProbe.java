package audit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 전수조사 보조: float(53) 컬럼과 binary_double 컬럼의 double 왕복 정밀도 비교.
 *
 * Tibero 에서 float(53) 은 NUMBER(15) 로 저장되므로 IEEE double 을 온전히 담지 못한다.
 */
public class DoublePrecisionProbe {

    public static void main(String[] args) throws Exception {
        Class.forName("com.tmax.tibero.jdbc.TbDriver");
        try (Connection c = DriverManager.getConnection("jdbc:tibero:thin:@localhost:8888:tibero", "tibero", "tmax")) {

            try (Statement s = c.createStatement()) {
                try { s.execute("drop table DP_T"); } catch (Exception ignored) {}
                s.execute("create table DP_T (id number(10), f53 float(53), bd binary_double)");
            }

            double[] samples = {
                    Math.PI,
                    1.0 / 3.0,
                    0.1,
                    123456789.12345678,
                    1.2345678901234567E10,
                    Double.MAX_VALUE,
                    Double.MIN_NORMAL,
                    1.7976931348623157E308
            };

            System.out.printf("%-26s %-26s %-26s %-6s %-6s%n", "원본 double", "float(53) 왕복", "binary_double 왕복", "f53", "bd");
            System.out.println("-".repeat(100));

            int f53Bad = 0, bdBad = 0;
            for (int i = 0; i < samples.length; i++) {
                double v = samples[i];
                String f53r, bdr;
                try (PreparedStatement ps = c.prepareStatement("insert into DP_T (id,f53,bd) values (?,?,?)")) {
                    ps.setInt(1, i);
                    ps.setDouble(2, v);
                    ps.setDouble(3, v);
                    ps.executeUpdate();
                    f53r = null; bdr = null;
                } catch (Exception e) {
                    System.out.printf("%-26s INSERT FAIL: %s%n", v, e.getMessage().replaceAll("\\s+", " "));
                    continue;
                }

                try (PreparedStatement ps = c.prepareStatement("select f53, bd from DP_T where id=?")) {
                    ps.setInt(1, i);
                    try (ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        double a = rs.getDouble(1);
                        double b = rs.getDouble(2);
                        f53r = String.valueOf(a);
                        bdr = String.valueOf(b);
                        boolean okA = a == v, okB = b == v;
                        if (!okA) f53Bad++;
                        if (!okB) bdBad++;
                        System.out.printf("%-26s %-26s %-26s %-6s %-6s%n",
                                v, f53r, bdr, okA ? "동일" : "손실", okB ? "동일" : "손실");
                    }
                }
            }

            System.out.println();
            System.out.println("float(53) 정밀도 손실 " + f53Bad + "/" + samples.length
                    + " , binary_double 손실 " + bdBad + "/" + samples.length);

            try (Statement s = c.createStatement()) { s.execute("drop table DP_T"); }
        }
    }
}
