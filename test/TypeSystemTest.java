import com.tmax.tibero.hibernate.dialect.TiberoTypes;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.mapping.Table;
import org.hibernate.tool.schema.internal.StandardTableExporter;
import org.hibernate.tool.schema.spi.Exporter;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.ValueExtractor;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.JdbcType;
import org.hibernate.type.descriptor.jdbc.JsonAsStringJdbcType;
import org.hibernate.type.descriptor.jdbc.SqlTypedJdbcType;
import org.hibernate.type.descriptor.jdbc.spi.JdbcTypeRegistry;
import org.hibernate.type.descriptor.sql.DdlType;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.hibernate.type.spi.TypeConfiguration;
import org.junit.Before;
import org.junit.Test;

import java.sql.Types;

import static org.junit.Assert.*;

/**
 * TiberoDialectTypeSystemTest
 *
 * 목적:
 *  1) columnType(sqlTypeCode) / registerColumnTypes() 로 등록된 타입 매핑이
 *     실제 DDL 렌더링 결과로 올바르게 반영되는지 검증
 *
 *  2) supportsBitType(), default interval second scale contract 검증
 *
 *  3) resolveSqlTypeDescriptor() 역매핑 로직(OTHER/NUMERIC/ARRAY/STRUCT) 전체 분기 검증
 *
 * Hibernate 6.6 + JUnit4 기준.
 */
public class TypeSystemTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;
    private TypeConfiguration typeConfig;
    private DdlTypeRegistry ddlTypeRegistry;
    private JdbcTypeRegistry jdbcTypeRegistry;
    private Exporter<Table> exporter;

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "none");
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        typeConfig = sfi.getTypeConfiguration();
        ddlTypeRegistry = typeConfig.getDdlTypeRegistry();
        jdbcTypeRegistry = typeConfig.getJdbcTypeRegistry();

        exporter = new StandardTableExporter(dialect);

        assertNotNull(dialect);
        assertNotNull(typeConfig);
        assertNotNull(ddlTypeRegistry);
        assertNotNull(jdbcTypeRegistry);
    }

    // =========================================================================
    // 1) columnType + registerColumnTypes: DDL 렌더링 검증 (Exporter 기반)
    // =========================================================================

    @Test
    public void ddl_columnType_numericAndStringAndBinaryMappings_shouldRenderCorrectly() {
        // BOOLEAN/TINYINT/SMALLINT/INTEGER/BIGINT -> number(p,0)
        assertEquals("number(1,0)", resolveDdlTypeName(SqlTypes.BOOLEAN, 0, 0, 0));
        assertEquals("number(3,0)", resolveDdlTypeName(SqlTypes.TINYINT, 0, 0, 0));
        assertEquals("number(5,0)", resolveDdlTypeName(SqlTypes.SMALLINT, 0, 0, 0));
        assertEquals("number(10,0)", resolveDdlTypeName(SqlTypes.INTEGER, 0, 0, 0));
        assertEquals("number(19,0)", resolveDdlTypeName(SqlTypes.BIGINT, 0, 0, 0));

        // REAL/DOUBLE -> binary_float/binary_double
        assertEquals("binary_float", resolveDdlTypeName(SqlTypes.REAL, 0, 0, 0));
        assertEquals("binary_double", resolveDdlTypeName(SqlTypes.DOUBLE, 0, 0, 0));

        // DECIMAL/NUMERIC -> number(p,s)
        assertEquals("number(30,10)", resolveDdlTypeName(SqlTypes.NUMERIC, 0, 30, 10));
        assertEquals("number(12,2)", resolveDdlTypeName(SqlTypes.DECIMAL, 0, 12, 2));

        // CHAR/VARCHAR/NVARCHAR -> length 치환
        assertEquals("char(10 char)", resolveDdlTypeName(SqlTypes.CHAR, 10, 0, 0));
        assertEquals("varchar2(10 char)", resolveDdlTypeName(SqlTypes.VARCHAR, 10, 0, 0));
        assertEquals("nvarchar2(10)", resolveDdlTypeName(SqlTypes.NVARCHAR, 10, 0, 0));

        // BINARY/VARBINARY -> raw(length)
        assertEquals("raw(10)", resolveDdlTypeName(SqlTypes.BINARY, 10, 0, 0));
        assertEquals("raw(12)", resolveDdlTypeName(SqlTypes.VARBINARY, 12, 0, 0));
    }

    @Test
    public void ddl_registerColumnTypes_customDescriptors_shouldRenderCorrectly() {
        // xmltype: SQLXML
        assertEquals("xmltype", resolveDdlTypeName(SqlTypes.SQLXML, 0, 0, 0));

        // geometry/json/interval day to second : SqlTypes 확장 타입 코드
        assertEquals("geometry", resolveDdlTypeName(SqlTypes.GEOMETRY, 0, 0, 0));
        assertEquals("json", resolveDdlTypeName(SqlTypes.JSON, 0, 0, 0));
        assertEquals("interval day to second", resolveDdlTypeName(SqlTypes.INTERVAL_SECOND, 0, 0, 0));
    }

    // =========================================================================
    // 2) supports flags
    // =========================================================================

    @Test
    public void supportsBitType_shouldBeFalse() {
        assertFalse(dialect.supportsBitType());
    }

    @Test
    public void defaultIntervalSecondScale_shouldBe6() {
        assertEquals(6, dialect.getDefaultIntervalSecondScale());
    }

    // =========================================================================
    // 3) resolveSqlTypeDescriptor(): 역매핑 로직 전체 분기 테스트
    // =========================================================================

    @Test
    public void resolve_OTHER_json_shouldReturnJsonType() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("json", Types.BLOB, 0, 0, jdbcTypeRegistry);
        assertNotNull(t);
        // registry에 등록된 JSON descriptor도 같이 확인
        JdbcType jsonDesc = jdbcTypeRegistry.getDescriptor(SqlTypes.JSON);


        System.out.println("=== resolved JdbcType ===");
        System.out.println("class                = " + t.getClass().getName());
        System.out.println("jdbcTypeCode         = " + t.getJdbcTypeCode() + " (java.sql.Types)");
        System.out.println("defaultSqlTypeCode   = " + t.getDefaultSqlTypeCode() + " (org.hibernate.type.SqlTypes)");
        System.out.println("ddlTypeCode          = " + t.getDdlTypeCode());
        System.out.println("sameAsRegistryJson?  = " + (t == jsonDesc));
        System.out.println("registryJson.class   = " + jsonDesc.getClass().getName());
        System.out.println("registryJson.jdbc    = " + jsonDesc.getJdbcTypeCode());
        System.out.println("registryJson.default = " + jsonDesc.getDefaultSqlTypeCode());
        System.out.println("registryJson.ddl     = " + jsonDesc.getDdlTypeCode());


        // “JSON으로 인식” 여부는 이걸로 검증
        assertEquals(SqlTypes.JSON, t.getDefaultSqlTypeCode());
    }

    @Test
    public void sanity_check_jsonJdbcTypeDescriptorExists() {
        JdbcType json = jdbcTypeRegistry.findDescriptor(SqlTypes.JSON);
        System.out.println("json descriptor = " + json);
        if (json != null) {
            System.out.println("json jdbcTypeCode=" + json.getJdbcTypeCode());
            System.out.println("json ddlTypeCode=" + json.getDdlTypeCode());
            System.out.println("json class=" + json.getClass().getName());
        }
        assertNotNull("JdbcTypeRegistry must contain JSON descriptor", json);
        assertTrue(json instanceof JsonAsStringJdbcType);
    }

    @Test
    public void resolve_OTHER_geometry_shouldReturnGeometryDdlType() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("geometry", TiberoTypes.GEOMETRY, 0, 0, jdbcTypeRegistry);
        assertNotNull(t);

        // geometry는 JDBC 바인딩이 보통 VARBINARY
        assertEquals(SqlTypes.VARBINARY, t.getDefaultSqlTypeCode());

        // hibernate에서 geometry 타입에 대해 VarbinaryJdbcType을 구현체로 사용
        assertEquals(SqlTypes.VARBINARY, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_OTHER_intervalDayToSecond_shouldReturnIntervalSecondType() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("interval day(2) to second(6)", SqlTypes.OTHER, 0, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.INTERVAL_SECOND, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_NUMERIC_scale0_precision1_shouldReturnBoolean() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("number", SqlTypes.NUMERIC, 1, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.BOOLEAN, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_NUMERIC_scale0_precision3_shouldReturnTinyint() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("number", SqlTypes.NUMERIC, 3, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.TINYINT, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_NUMERIC_scale0_precision5_shouldReturnSmallint() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("number", SqlTypes.NUMERIC, 5, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.SMALLINT, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_NUMERIC_scale0_precision10_shouldReturnInteger() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("number", SqlTypes.NUMERIC, 10, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.INTEGER, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_NUMERIC_scale0_precision19_shouldReturnBigint() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("number", SqlTypes.NUMERIC, 19, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.BIGINT, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_NUMERIC_scale0_precision38_shouldFallbackToNumeric() {
        JdbcType t = dialect.resolveSqlTypeDescriptor("number", SqlTypes.NUMERIC, 38, 0, jdbcTypeRegistry);
        assertNotNull(t);
        assertEquals(SqlTypes.NUMERIC, t.getJdbcTypeCode());
    }

    @Test
    public void resolve_ARRAY_shouldUseSqlTypedDescriptor_withoutSchemaPrefix() {
        SqlTypedJdbcType dummy = new DummySqlTypedJdbcType("MY_ARRAY_TYPE", SqlTypes.ARRAY);
        jdbcTypeRegistry.addDescriptor(dummy);

        JdbcType resolved = dialect.resolveSqlTypeDescriptor("SCHEMA.MY_ARRAY_TYPE", SqlTypes.ARRAY, 0, 0, jdbcTypeRegistry);
        assertNotNull(resolved);
        assertSame(dummy, resolved);
    }

    @Test
    public void resolve_STRUCT_shouldUseSqlTypedDescriptor_withoutSchemaPrefix() {
        SqlTypedJdbcType dummy = new DummySqlTypedJdbcType("MY_OBJ_TYPE", SqlTypes.STRUCT);
        jdbcTypeRegistry.addDescriptor(dummy);

        JdbcType resolved = dialect.resolveSqlTypeDescriptor("SCHEMA.MY_OBJ_TYPE", SqlTypes.STRUCT, 0, 0, jdbcTypeRegistry);
        assertNotNull(resolved);
        assertSame(dummy, resolved);
    }

    // =========================================================================
    // Helper methods
    // =========================================================================

    private String resolveDdlTypeName(int jdbcTypeCode, long length, int precision, int scale) {
        DdlType ddlType = ddlTypeRegistry.getDescriptor(jdbcTypeCode);
        assertNotNull("No DdlType registered for jdbcType=" + jdbcTypeCode, ddlType);

        // Hibernate 6.6 Size 생성
        Size size = Size.nil();
        if (length > 0) size.setLength(length);
        if (precision > 0) size.setPrecision(precision);
        if (scale > 0) size.setScale(scale);

        // 중요: registry를 같이 넘겨야 치환됨
        String typeName = ddlType.getTypeName(size, null, ddlTypeRegistry);

        return typeName;
    }

    // =========================================================================
    // Dummy SqlTypedJdbcType (ARRAY/STRUCT reverse mapping용)
    // =========================================================================

    public static class DummySqlTypedJdbcType implements SqlTypedJdbcType {
        private final String sqlTypeName;
        private final int jdbcCode;

        public DummySqlTypedJdbcType(String sqlTypeName, int jdbcCode) {
            this.sqlTypeName = sqlTypeName;
            this.jdbcCode = jdbcCode;
        }

        @Override
        public String getSqlTypeName() {
            return sqlTypeName;
        }

        @Override
        public int getJdbcTypeCode() {
            return jdbcCode;
        }

        @Override
        public <X> ValueBinder<X> getBinder(JavaType<X> javaType) {
            return null;
        }

        @Override
        public <X> ValueExtractor<X> getExtractor(JavaType<X> javaType) {
            return null;
        }
    }
}
