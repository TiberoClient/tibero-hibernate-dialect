package com.tmax.tibero.hibernate.dialect;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;

/**
 * JDBC 메타데이터를 보고 Tibero 를 알아내어 {@link TiberoDialect} 를 물려준다.
 */
public class TiberoDialectResolver implements DialectResolver {

    /** {@code DatabaseMetaData.getDatabaseProductName()} 실측값 (ps06 / tibero7-jdbc-11). */
    private static final String PRODUCT_NAME = "Tibero";

    /**
     * {@code getDriverName()} 의 접두사.
     *
     * <p>실측값은 {@code "Tibero Tibero_7.2.9afaec48_JDBC_11_release_20260902"} 로
     * 빌드 번호와 날짜가 붙는다. 그래서 전체 일치가 아니라 접두사로 본다.
     */
    private static final String DRIVER_NAME_PREFIX = "Tibero";

    /**
     * {@inheritDoc}
     *
     * <p><b>내 DB 가 아니면 반드시 {@code null} 을 돌려준다 — 예외를 던지면 안 된다.</b>
     * {@code DialectResolverSet} 은 resolver 가 던진 예외를 INFO 로그 한 줄로 삼키고
     * 다음 resolver 로 넘어간다. 예외로 실패를 알리려 하면 아무 데도 안 남는다.
     */
    @Override
    public Dialect resolveDialect(DialectResolutionInfo info) {
        return matches(info) ? new TiberoDialect(info) : null;
    }

    /**
     * 제품명 정확 일치, 실패하면 드라이버명 접두사.
     *
     * <p>{@code null} 방어가 형식적인 게 아니다 — 여기서 NPE 가 나면
     * <b>Tibero 가 아닌 모든 DB 의 부팅</b>이 이 resolver 를 지나갈 때마다 예외를
     * 던지게 되고, 그 예외는 위에 적은 대로 조용히 삼켜져 추적이 불가능해진다.
     *
     * <p>{@code trim()} 은 이 드라이버가 {@code getDriverVersion()} 에 실제로
     * 끝 공백을 흘리는 것을 보고 넣었다({@code "7.2. "}). 제품명에도 같은 일이
     * 생길 수 있다고 보는 편이 안전하다.
     */
    private static boolean matches(DialectResolutionInfo info) {
        final String databaseName = info.getDatabaseName();
        if (databaseName != null && PRODUCT_NAME.equalsIgnoreCase(databaseName.trim())) {
            return true;
        }
        final String driverName = info.getDriverName();
        return driverName != null && driverName.trim().startsWith(DRIVER_NAME_PREFIX);
    }
}
