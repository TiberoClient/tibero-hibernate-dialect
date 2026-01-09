import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.spi.JdbcServices;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.junit.Test;


import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.sql.*;
import java.util.Locale;


import static org.junit.Assert.*;


public class E2ETest extends AbstractTiberoDialectTestBase {


    // =========================================================================
    // Dialect / Registries
    // =========================================================================


    private TiberoDialect dialect() {
        final SessionFactoryImplementor sfi = (SessionFactoryImplementor) sessionFactory();
        final JdbcServices jdbcServices = sfi.getJdbcServices();
        final Dialect d = jdbcServices.getDialect();
        assertTrue("Dialect is not TiberoDialect: " + d.getClass(), d instanceof TiberoDialect);
        return (TiberoDialect) d;
    }


    private JdbcTypeRegistry jdbcTypeRegistry() {
        return ((SessionFactoryImplementor) sessionFactory())
                .getTypeConfiguration()
                .getJdbcTypeRegistry();
    }


    private DdlTypeRegistry ddlTypeRegistry() {
        return ((SessionFactoryImplementor) sessionFactory())
                .getTypeConfiguration()
                .getDdlTypeRegistry();
    }


    // =========================================================================
    // 1) columnType() tests (reflection invoke)
    // =========================================================================


    @Test
    public void test_columnType_mappings() throws Exception {
        TiberoDialect d = dialect();

        assertEquals("number(1,0)",  invokeProtectedColumnType(d, SqlTypes.BOOLEAN));
        assertEquals("number(3,0)",  invokeProtectedColumnType(d, SqlTypes.TINYINT));
        assertEquals("number(5,0)",  invokeProtectedColumnType(d, SqlTypes.SMALLINT));
        assertEquals("number(10,0)", invokeProtectedColumnType(d, SqlTypes.INTEGER));
        assertEquals("number(19,0)", invokeProtectedColumnType(d, SqlTypes.BIGINT));

        assertEquals("binary_float",  invokeProtectedColumnType(d, SqlTypes.REAL));
        assertEquals("binary_double", invokeProtectedColumnType(d, SqlTypes.DOUBLE));

        assertEquals("number($p,$s)", invokeProtectedColumnType(d, SqlTypes.NUMERIC));
        assertEquals("number($p,$s)", invokeProtectedColumnType(d, SqlTypes.DECIMAL));

        assertEquals("timestamp($p) with time zone", invokeProtectedColumnType(d, SqlTypes.TIME_WITH_TIMEZONE));

        assertEquals("char($l char)",     invokeProtectedColumnType(d, SqlTypes.CHAR));
        assertEquals("varchar2($l char)", invokeProtectedColumnType(d, SqlTypes.VARCHAR));
        assertEquals("nvarchar2($l)",     invokeProtectedColumnType(d, SqlTypes.NVARCHAR));

        assertEquals("raw($l)", invokeProtectedColumnType(d, SqlTypes.BINARY));
        assertEquals("raw($l)", invokeProtectedColumnType(d, SqlTypes.VARBINARY));
    }


    // =========================================================================
    // 2) registerColumnTypes() tests
    //    - validate DdlTypeRegistry got your custom descriptors
    // =========================================================================


    @Test
    public void test_registerColumnTypes_ddlTypeRegistry_has_custom_descriptors() throws Exception {
        DdlTypeRegistry reg = ddlTypeRegistry();

        Object jsonDdl = reg.getDescriptor(SqlTypes.JSON);
        Object geomDdl = reg.getDescriptor(SqlTypes.GEOMETRY);
        Object xmlDdl  = reg.getDescriptor(SqlTypes.SQLXML);
        Object intDdl  = reg.getDescriptor(SqlTypes.INTERVAL_SECOND);

        assertNotNull("DDL type for JSON not registered", jsonDdl);
        assertNotNull("DDL type for GEOMETRY not registered", geomDdl);
        assertNotNull("DDL type for SQLXML not registered", xmlDdl);
        assertNotNull("DDL type for INTERVAL_SECOND not registered", intDdl);

        // raw type name가 API마다 노출 방식이 달라서 안전하게 리플렉션으로 추출
        assertEquals("json",                    extractRawDdlTypeName(jsonDdl).toLowerCase(Locale.ROOT));
        assertEquals("geometry",                extractRawDdlTypeName(geomDdl).toLowerCase(Locale.ROOT));
        assertEquals("xmltype",                 extractRawDdlTypeName(xmlDdl).toLowerCase(Locale.ROOT));
        assertEquals("interval day to second",  extractRawDdlTypeName(intDdl).toLowerCase(Locale.ROOT));
    }


    // =========================================================================
    // 3) resolveSqlTypeDescriptor() end-to-end tests
    //    - create table (DDL), insert, select
    //    - read ResultSetMetaData
    //    - call dialect.resolveSqlTypeDescriptor(...)
    // =========================================================================


    @Test
    public void test_resolveSqlTypeDescriptor_end_to_end() {
        final String table = uniqueObjectName("tb_types");


        // DDL: use types that your resolveSqlTypeDescriptor handles:
        // - json (Types.BLOB + typeName json)
        // - geometry (Types.GEOMERTY + typeName geometry)
        // - interval day to second (Types.OTHER + typeName interval day to second)
        // - binary_float / binary_double (Types.OTHER + typeName binary_float/binary_double)
        // - number(p,0) (Types.NUMERIC + precision p scale 0) => integer reverse mapping
        final String ddl =
                "create table " + table + " ( " +
                        " id number(19,0), " +
                        " j json, " +
                        " g geometry, " +
                        " i interval day to second, " +
                        " bf binary_float, " +
                        " bd binary_double, " +
                        " b number(1,0), " +
                        " t number(3,0), " +
                        " s number(5,0), " +
                        " n number(10,0), " +
                        " l number(19,0), " +
                        " x number(8,2) " +   // fallback NUMERIC
                        ")";


        try {
            // create
            inTransaction(session -> session.createNativeMutationQuery(ddl).executeUpdate());


            // insert (값은 최소)
            inTransaction(session -> session.createNativeMutationQuery(
                    "insert into " + table +
                            " (id, j, bf, bd, b, t, s, n, l, x) values " +
                            " (1, '{\"a\":1}', 1.25, 2.5, 1, 7, 11, 123, 123456789, 12.34)"
            ).executeUpdate());


            // select + metadata + resolve
            final Meta meta = inTransactionReturning(session ->
                    session.doReturningWork(conn -> readMeta(conn,
                            "select j, g, i, bf, bd, b, t, s, n, l, x from " + table + " where id = 1",
                            table))
            );

            TiberoDialect d = dialect();
            JdbcTypeRegistry jreg = jdbcTypeRegistry();

            // 0:j(json) 1:g(geometry) 2:i(interval) 3:bf 4:bd 5:b 6:t 7:s 8:n 9:l 10:x

            assertResolvedDefaultSqlType(d, jreg, meta.cols[0], SqlTypes.JSON);
            // hibernate에서 geometry 타입에 대해 VarbinaryJdbcType을 구현체로 사용
            assertResolvedDefaultSqlType(d, jreg, meta.cols[1], SqlTypes.VARBINARY);
            assertResolvedDefaultSqlType(d, jreg, meta.cols[2], SqlTypes.INTERVAL_SECOND);


            // binary_float/binary_double are returned as Types.OTHER with typeName "binary_float"/"binary_double"
            assertResolvedDefaultSqlType(d, jreg, meta.cols[3], SqlTypes.REAL);
            assertResolvedDefaultSqlType(d, jreg, meta.cols[4], SqlTypes.DOUBLE);


            // number(p,0) => reverse mapping to integer family
            assertResolvedDefaultSqlType(d, jreg, meta.cols[5], SqlTypes.BOOLEAN);
            assertResolvedDefaultSqlType(d, jreg, meta.cols[6], SqlTypes.TINYINT);
            assertResolvedDefaultSqlType(d, jreg, meta.cols[7], SqlTypes.SMALLINT);
            assertResolvedDefaultSqlType(d, jreg, meta.cols[8], SqlTypes.INTEGER);
            assertResolvedDefaultSqlType(d, jreg, meta.cols[9], SqlTypes.BIGINT);


            // fallback numeric
            assertResolvedDefaultSqlType(d, jreg, meta.cols[10], SqlTypes.NUMERIC);
        }
        finally {
            dropTableWithRetry(table);
        }
    }


    // =========================================================================
    // Helpers: invoke protected columnType(), extract DDL raw type name,
    //          JDBC meta reader, assertion helpers
    // =========================================================================


    private static String invokeProtectedColumnType(Dialect dialect, int sqlTypeCode) throws Exception {
        Method m = null;
        Class<?> c = dialect.getClass();
        while (c != null && m == null) {
            try {
                m = c.getDeclaredMethod("columnType", int.class);
            } catch (NoSuchMethodException ignored) {
                c = c.getSuperclass();
            }
        }
        assertNotNull("Could not find Dialect#columnType(int) via reflection", m);
        m.setAccessible(true);
        return (String) m.invoke(dialect, sqlTypeCode);
    }


    /**
     * DdlTypeImpl 내부 raw type name을 최대한 안전하게 뽑아낸다.
     * (Hibernate minor 버전 따라 public API가 다를 수 있어서 reflection fallback)
     */
    private static String extractRawDdlTypeName(Object ddlType) throws Exception {
        // 1) public/protected method 후보
        for (String methodName : new String[]{"getRawTypeName", "getTypeName", "getTypeNamePattern"}) {
            try {
                Method m = ddlType.getClass().getMethod(methodName);
                Object v = m.invoke(ddlType);
                if (v instanceof String) {
                    return (String) v;
                }
            } catch (NoSuchMethodException ignored) {}
        }


        // 2) field 후보
        for (String fieldName : new String[]{"rawTypeName", "typeName", "typeNamePattern", "typeNameTemplate"}) {
            Field f = findField(ddlType.getClass(), fieldName);
            if (f != null) {
                f.setAccessible(true);
                Object v = f.get(ddlType);
                if (v instanceof String) {
                    return (String) v;
                }
            }
        }


        // 3) 마지막 fallback
        return ddlType.toString();
    }


    private static Field findField(Class<?> type, String name) {
        Class<?> c = type;
        while (c != null) {
            try {
                return c.getDeclaredField(name);
            } catch (NoSuchFieldException ignored) {
                c = c.getSuperclass();
            }
        }
        return null;
    }


    private static void assertResolvedDefaultSqlType(
            Dialect dialect,
            JdbcTypeRegistry jdbcTypeRegistry,
            ColMeta col,
            int expectedSqlTypeCode
    ) {
        JdbcType resolved = dialect.resolveSqlTypeDescriptor(
                col.typeName,
                col.jdbcTypeCode,
                col.precision,
                col.scale,
                jdbcTypeRegistry
        );
        assertNotNull("Resolved JdbcType is null for " + col, resolved);
        assertEquals(
                "defaultSqlTypeCode mismatch for " + col + " resolved=" + resolved.getClass().getName()
                        + " jdbcTypeCode=" + resolved.getJdbcTypeCode(),
                expectedSqlTypeCode,
                resolved.getDefaultSqlTypeCode()
        );
    }


    private static class Meta {
        final ColMeta[] cols;
        Meta(ColMeta[] cols) { this.cols = cols; }
    }


    private static class ColMeta {
        final String label;
        final String typeName;
        final int jdbcTypeCode;
        final int precision;
        final int scale;


        ColMeta(String label, String typeName, int jdbcTypeCode, int precision, int scale) {
            this.label = label;
            this.typeName = typeName;
            this.jdbcTypeCode = jdbcTypeCode;
            this.precision = precision;
            this.scale = scale;
        }


        @Override
        public String toString() {
            return "label=" + label +
                    ", typeName=" + typeName +
                    ", jdbcType=" + jdbcTypeCode +
                    ", p=" + precision +
                    ", s=" + scale;
        }
    }


    private static Meta readMeta(Connection conn, String sql, String table) throws SQLException {
        try (PreparedStatement ps = conn.prepareStatement(sql);
             ResultSet rs = ps.executeQuery()) {

            ResultSetMetaData md = rs.getMetaData();
            int n = md.getColumnCount();

            ColMeta[] cols = new ColMeta[n];
            for (int i = 1; i <= n; i++) {
                cols[i - 1] = new ColMeta(
                        md.getColumnLabel(i),
                        md.getColumnTypeName(i),
                        md.getColumnType(i),     // java.sql.Types.*
                        md.getPrecision(i),
                        md.getScale(i)
                );
            }
            return new Meta(cols);
        }
    }
}





