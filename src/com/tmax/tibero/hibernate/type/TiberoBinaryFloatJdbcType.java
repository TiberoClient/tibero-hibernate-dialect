package com.tmax.tibero.hibernate.type;

import org.hibernate.type.SqlTypes;
import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.FloatJdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * float을 Tibero의 binary_float로 무손실 바인딩하는 JdbcType.
 *
 * 표준 {@link FloatJdbcType}은 st.setFloat()을 쓰는데, tbjdbc는 이를 NUMBER로 바인딩한다.
 *
 * FLOAT과 REAL 두 타입 코드에 각각 등록한다.
 * (Hibernate는 java Float을 FLOAT으로 매핑하고, TiberoDialect는 REAL도 binary_float으로 매핑한다)
 */
public class TiberoBinaryFloatJdbcType extends FloatJdbcType {

    public static final TiberoBinaryFloatJdbcType FLOAT_INSTANCE = new TiberoBinaryFloatJdbcType(SqlTypes.FLOAT);
    public static final TiberoBinaryFloatJdbcType REAL_INSTANCE = new TiberoBinaryFloatJdbcType(SqlTypes.REAL);

    private final int jdbcTypeCode;

    private TiberoBinaryFloatJdbcType(int jdbcTypeCode) {
        this.jdbcTypeCode = jdbcTypeCode;
    }

    @Override
    public int getJdbcTypeCode() {
        return jdbcTypeCode;
    }

    @Override
    public String getFriendlyName() {
        return "BINARY_FLOAT";
    }

    @Override
    public String toString() {
        return "TiberoBinaryFloatJdbcType(" + jdbcTypeCode + ")";
    }

    @Override
    public <X> ValueBinder<X> getBinder(final JavaType<X> javaType) {
        return new BasicBinder<>(javaType, this) {
            @Override
            protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
                    throws SQLException {
                TiberoBinaryFloatingPointBinder.setFloat(
                        st, index, javaType.unwrap(value, Float.class, options));
            }

            @Override
            protected void doBind(CallableStatement st, X value, String name, WrapperOptions options)
                    throws SQLException {
                // tbjdbc의 확장 setter는 인덱스 바인딩만 제공한다
                st.setFloat(name, javaType.unwrap(value, Float.class, options));
            }
        };
    }
}
