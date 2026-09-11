package semantics;

import support.AbstractTiberoDialectTestBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.Size;
import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.sql.DdlType;
import org.hibernate.type.descriptor.sql.spi.DdlTypeRegistry;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * 부동소수 타입이 Tibero의 IEEE 타입(binary_float/binary_double)으로 매핑되는지 검증한다.
 *
 * 회귀 배경:
 * Hibernate는 Float/Double을 모두 FLOAT DDL 코드로 보낸다(DoubleJdbcType.getDdlTypeCode()==FLOAT).
 * 기본 매핑 float($p)를 그대로 쓰면 Tibero에서 float(53) -> NUMBER(15)로 저장되어
 *   - double이 15자리로 잘리고 (3.141592653589793 -> 3.14159265358979)
 *   - Double.MAX_VALUE는 overflow로 insert 자체가 실패하며
 *   - cast(x as float(53))은 NUMBER 정밀도 상한(38) 초과로 JDBC-5077 실패
 * 하는 문제가 있었다.
 */
public class FloatingPointPrecisionTest extends AbstractTiberoDialectTestBase {

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{FpEntity.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    // ------------------------------------------------------------------------
    // Contract: DDL 타입명
    // ------------------------------------------------------------------------

    @Test
    public void floatDdlCode_mapsToBinaryFloatOrDouble_byPrecision() {
        DdlTypeRegistry reg = sessionFactory().getTypeConfiguration().getDdlTypeRegistry();
        DdlType t = reg.getDescriptor(SqlTypes.FLOAT);
        assertNotNull("FLOAT DDL 타입이 등록되어 있어야 한다", t);

        assertEquals("double precision(53)은 binary_double 이어야 한다",
                "binary_double", t.getTypeName(null, 53, null));
        assertEquals("float precision(24)은 binary_float 이어야 한다",
                "binary_float", t.getTypeName(null, 24, null));

        // float(53) 이 그대로 나가면 Tibero에서 NUMBER(15)로 잘린다
        assertFalse("float($p) 패턴이 남아 있으면 안 된다",
                t.getTypeName(null, 53, null).startsWith("float"));
    }

    @Test
    public void doubleAndRealDdlCodes_mapToTiberoIeeeTypes() {
        DdlTypeRegistry reg = sessionFactory().getTypeConfiguration().getDdlTypeRegistry();
        assertEquals("binary_double", reg.getDescriptor(SqlTypes.DOUBLE).getTypeName(Size.nil(), null, reg));
        assertEquals("binary_float", reg.getDescriptor(SqlTypes.REAL).getTypeName(Size.nil(), null, reg));
    }

    // ------------------------------------------------------------------------
    // Behaviour: 값 왕복
    // ------------------------------------------------------------------------

    @Test
    public void doubleRoundTrip_isLossless() {
        double[] samples = {
                Math.PI,
                1.0 / 3.0,
                0.1,
                123456789.12345678,
                1.2345678901234567E10,
                -1.2345678901234567E10,
                9.007199254740993E15,
                1.0E100,
                Double.MAX_VALUE,
                -Double.MAX_VALUE,
                Double.MIN_NORMAL,
                Double.MIN_VALUE
        };

        for (int i = 0; i < samples.length; i++) {
            final long id = 100 + i;
            final double v = samples[i];

            inTransaction(session -> {
                FpEntity e = new FpEntity();
                e.id = id;
                e.dVal = v;
                e.fVal = 1.0f;
                session.persist(e);
            });

            Double read = inTransactionReturning(session ->
                    session.get(FpEntity.class, id).dVal);

            assertNotNull("id=" + id + " 조회 실패", read);
            assertEquals("double 왕복에서 정밀도가 손실되면 안 된다 (원본 " + v + ")",
                    v, read, 0.0);
        }
    }

    /**
     * NaN / Infinity는 NUMBER로 표현할 수 없으므로 확장 바인딩이 동작해야만 통과한다.
     */
    @Test
    public void nonFiniteDoubles_roundTrip() {
        double[] samples = {Double.NaN, Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY};

        for (int i = 0; i < samples.length; i++) {
            final long id = 900 + i;
            final double v = samples[i];

            inTransaction(session -> {
                FpEntity e = new FpEntity();
                e.id = id;
                e.dVal = v;
                e.fVal = 1.0f;
                session.persist(e);
            });

            Double read = inTransactionReturning(session -> session.get(FpEntity.class, id).dVal);
            assertNotNull(read);
            assertEquals("비유한값 왕복 실패 (원본 " + v + ")",
                    Double.doubleToLongBits(v), Double.doubleToLongBits(read));
        }
    }

    @Test
    public void binderUsesDriverExtension_notNumberBinding() {
        // 확장 바인딩이 빠지면 NUMBER를 경유하며 1.0E100 이 9.999999999999998E99 로 어긋난다
        final double v = 1.0E100;
        inTransaction(session -> {
            FpEntity e = new FpEntity();
            e.id = 950L;
            e.dVal = v;
            e.fVal = 1.0f;
            session.persist(e);
        });
        Double read = inTransactionReturning(session -> session.get(FpEntity.class, 950L).dVal);
        assertEquals("NUMBER 경유 바인딩으로 되돌아갔다", v, read, 0.0);
    }

    @Test
    public void floatRoundTrip_isLossless() {
        float[] samples = {
                (float) Math.PI, 1.0f / 3.0f, 0.1f, 1.2345678E20f,
                Float.MAX_VALUE, -Float.MAX_VALUE, Float.MIN_NORMAL, Float.MIN_VALUE};

        for (int i = 0; i < samples.length; i++) {
            final long id = 200 + i;
            final float v = samples[i];

            inTransaction(session -> {
                FpEntity e = new FpEntity();
                e.id = id;
                e.dVal = 1.0;
                e.fVal = v;
                session.persist(e);
            });

            Float read = inTransactionReturning(session ->
                    session.get(FpEntity.class, id).fVal);

            assertNotNull("id=" + id + " 조회 실패", read);
            assertEquals("float 왕복에서 정밀도가 손실되면 안 된다 (원본 " + v + ")",
                    v, read, 0.0f);
        }
    }

    // ------------------------------------------------------------------------
    // Behaviour: HQL cast
    // ------------------------------------------------------------------------

    @Test
    public void hqlCastToDoubleAndFloat_executeOnTibero() {
        inTransaction(session -> {
            FpEntity e = new FpEntity();
            e.id = 300L;
            e.dVal = 2.5;
            e.fVal = 2.5f;
            session.persist(e);
        });

        Object d = inTransactionReturning(session -> session
                .createQuery("select cast('1.5' as Double) from FpEntity e where e.id=300", Object.class)
                .getSingleResult());
        assertEquals(1.5, ((Number) d).doubleValue(), 0.0);

        Object f = inTransactionReturning(session -> session
                .createQuery("select cast('1.5' as Float) from FpEntity e where e.id=300", Object.class)
                .getSingleResult());
        assertEquals(1.5f, ((Number) f).floatValue(), 0.0f);

        Object fromCol = inTransactionReturning(session -> session
                .createQuery("select cast(e.dVal as String) from FpEntity e where e.id=300", Object.class)
                .getSingleResult());
        assertNotNull(fromCol);
    }

    @Entity(name = "FpEntity")
    @Table(name = "FP_ENTITY")
    public static class FpEntity {

        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "D_VAL")
        public Double dVal;

        @Column(name = "F_VAL")
        public Float fVal;
    }
}
