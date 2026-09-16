package audit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * 전수조사 보조: Tibero 의 float(p) 허용 범위와 cast 대상 타입 실측.
 *
 * 배경: Hibernate 의 DoubleJdbcType.getDdlTypeCode() == FLOAT 이므로
 * java double/Double 은 columnType(DOUBLE)(=binary_double) 이 아니라
 * columnType(FLOAT)(= base "float($p)", p=getDoublePrecision()=53) 으로 렌더링된다.
 */
public class FloatPrecisionProbe {

    static Connection conn;

    public static void main(String[] args) throws Exception {
        Class.forName("com.tmax.tibero.jdbc.TbDriver");
        conn = DriverManager.getConnection("jdbc:tibero:thin:@localhost:8888:tibero", "tibero", "tmax");

        System.out.println("=== [A] cast 대상 타입 ===");
        for (String t : new String[]{"float(53)", "float(38)", "float(24)", "float(126)",
                "binary_double", "binary_float", "double precision", "number"}) {
            System.out.printf("  cast('1.5' as %-18s) : %s%n", t, sel("select cast('1.5' as " + t + ") from dual"));
        }

        System.out.println();
        System.out.println("=== [B] DDL float(p) 허용 범위 (이분탐색) ===");
        System.out.println("  최대 허용 p = " + maxDdlPrecision());

        System.out.println();
        System.out.println("=== [C] DDL 개별 확인 ===");
        for (String t : new String[]{"float(53)", "float(38)", "float(24)", "binary_double", "binary_float", "double precision"}) {
            System.out.printf("  create table (c %-18s) : %s%n", t, ddl(t));
        }

        System.out.println();
        System.out.println("=== [D] binary_double 정밀도 보존 확인 ===");
        System.out.println("  " + sel("select cast(1.7976931348623157E308 as binary_double) from dual"));
        System.out.println("  " + sel("select cast(2.2250738585072014E-308 as binary_double) from dual"));

        conn.close();
    }

    static String sel(String sql) {
        try (Statement s = conn.createStatement(); var rs = s.executeQuery(sql)) {
            return rs.next() ? "OK -> " + rs.getObject(1) : "OK (no row)";
        } catch (Exception e) {
            return "FAIL: " + e.getMessage().replaceAll("\\s+", " ").trim();
        }
    }

    static String ddl(String type) {
        try (Statement s = conn.createStatement()) {
            s.execute("create table FP_T (c " + type + ")");
            String actual = "?";
            try (var rs = s.executeQuery(
                    "select data_type, data_precision, data_scale from user_tab_columns where table_name='FP_T'")) {
                if (rs.next()) actual = rs.getString(1) + "(" + rs.getObject(2) + "," + rs.getObject(3) + ")";
            }
            s.execute("drop table FP_T");
            return "OK -> 실제 저장타입 " + actual;
        } catch (Exception e) {
            try (Statement s2 = conn.createStatement()) { s2.execute("drop table FP_T"); } catch (Exception ignored) {}
            return "FAIL: " + e.getMessage().replaceAll("\\s+", " ").trim();
        }
    }

    static int maxDdlPrecision() {
        int lo = 1, hi = 200, best = 0;
        while (lo <= hi) {
            int mid = (lo + hi) / 2;
            if (ddl("float(" + mid + ")").startsWith("OK")) { best = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        return best;
    }
}
