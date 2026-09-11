package com.tmax.tibero.hibernate.type;

import org.hibernate.type.descriptor.ValueBinder;
import org.hibernate.type.descriptor.WrapperOptions;
import org.hibernate.type.descriptor.java.JavaType;
import org.hibernate.type.descriptor.jdbc.BasicBinder;
import org.hibernate.type.descriptor.jdbc.DoubleJdbcType;

import java.sql.CallableStatement;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * double을 Tibero의 binary_double로 무손실 바인딩하는 JdbcType.
 *
 * 표준 {@link DoubleJdbcType}은 st.setDouble()을 쓰는데, tbjdbc는 이를 NUMBER로 바인딩하기 때문에
 * 컬럼이 binary_double이어도 IEEE 극값이 거부되거나 큰 지수에서 왕복 오차가 생긴다.
 *
 * 읽기(getDouble)는 손실이 없으므로 바인딩만 교체한다.
 */
public class TiberoBinaryDoubleJdbcType extends DoubleJdbcType {

    public static final TiberoBinaryDoubleJdbcType INSTANCE = new TiberoBinaryDoubleJdbcType();

    @Override
    public String getFriendlyName() {
        return "BINARY_DOUBLE";
    }

    @Override
    public String toString() {
        return "TiberoBinaryDoubleJdbcType";
    }

    @Override
    public <X> ValueBinder<X> getBinder(final JavaType<X> javaType) {
        return new BasicBinder<>(javaType, this) {
            @Override
            protected void doBind(PreparedStatement st, X value, int index, WrapperOptions options)
                    throws SQLException {
                TiberoBinaryFloatingPointBinder.setDouble(
                        st, index, javaType.unwrap(value, Double.class, options));
            }

            @Override
            protected void doBind(CallableStatement st, X value, String name, WrapperOptions options)
                    throws SQLException {
                // tbjdbc의 확장 setter는 인덱스 바인딩만 제공한다
                st.setDouble(name, javaType.unwrap(value, Double.class, options));
            }
        };
    }
}
