package com.tmax.tibero.hibernate.type;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Optional;

/**
 * tbjdbc의 setBinaryDouble/setBinaryFloat 확장 API로 IEEE 값을 바인딩한다.
 *
 * 표준 setDouble/setFloat는 대상 컬럼이 binary_double/binary_float이어도 값을 NUMBER로 보낸다.
 * 그래서 NUMBER 표현 범위(약 ±1e126)를 벗어나는 값이 JDBC-90653/90654로 거부되고,
 * 범위 안이어도 지수가 크면 이진↔십진 왕복 오차가 남는다 (1.0E100 → 9.999999999999998E99).
 * 확장 API로 바인딩하면 NaN/Infinity를 포함해 무손실이다.
 *
 * 확장 API가 없는 드라이버나 이를 노출하지 않는 커넥션 풀 프록시에서는 표준 경로로 폴백한다.
 */
public final class TiberoBinaryFloatingPointBinder {

    private static final ClassValue<Optional<Method>> BINARY_DOUBLE_SETTER = setterCache("setBinaryDouble", double.class);
    private static final ClassValue<Optional<Method>> BINARY_FLOAT_SETTER = setterCache("setBinaryFloat", float.class);

    private TiberoBinaryFloatingPointBinder() {
    }

    public static void setDouble(PreparedStatement st, int index, double value) throws SQLException {
        if (!bind(BINARY_DOUBLE_SETTER, st, index, value)) {
            st.setDouble(index, value);
        }
    }

    public static void setFloat(PreparedStatement st, int index, float value) throws SQLException {
        if (!bind(BINARY_FLOAT_SETTER, st, index, value)) {
            st.setFloat(index, value);
        }
    }

    /**
     * @return 확장 API로 바인딩했으면 true, 확장 API를 찾지 못해 호출자가 표준 경로를 써야 하면 false
     */
    private static boolean bind(ClassValue<Optional<Method>> cache, PreparedStatement st, int index, Object value)
            throws SQLException {

        PreparedStatement target = st;
        Method setter = cache.get(target.getClass()).orElse(null);

        if (setter == null) {
            target = unwrap(st);
            setter = target == st ? null : cache.get(target.getClass()).orElse(null);
        }
        if (setter == null) {
            return false;
        }

        try {
            setter.invoke(target, index, value);
            return true;
        }
        catch (InvocationTargetException e) {
            // 드라이버가 실제로 거부한 경우는 표준 경로로 재시도하지 않고 그대로 알린다
            final Throwable cause = e.getCause();
            if (cause instanceof SQLException) {
                throw (SQLException) cause;
            }
            if (cause instanceof RuntimeException) {
                throw (RuntimeException) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new SQLException("Tibero 확장 바인딩 호출에 실패했습니다: " + setter.getName(), cause);
        }
        catch (IllegalAccessException e) {
            return false;
        }
    }

    private static PreparedStatement unwrap(PreparedStatement st) {
        try {
            if (st.isWrapperFor(PreparedStatement.class)) {
                final PreparedStatement unwrapped = st.unwrap(PreparedStatement.class);
                if (unwrapped != null) {
                    return unwrapped;
                }
            }
        }
        catch (SQLException | RuntimeException ignored) {
            // 폴백
        }
        return st;
    }

    private static ClassValue<Optional<Method>> setterCache(String name, Class<?> valueType) {
        return new ClassValue<>() {
            @Override
            protected Optional<Method> computeValue(Class<?> statementType) {
                try {
                    final Method m = statementType.getMethod(name, int.class, valueType);
                    return m.trySetAccessible() ? Optional.of(m) : Optional.empty();
                }
                catch (NoSuchMethodException | RuntimeException e) {
                    return Optional.empty();
                }
            }
        };
    }
}
