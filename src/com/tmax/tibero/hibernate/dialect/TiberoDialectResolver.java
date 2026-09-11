package com.tmax.tibero.hibernate.dialect;

import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;

/**
 * JDBC 메타데이터를 보고 Tibero 를 알아내어 {@link TiberoDialect} 를 물려준다.
 *
 * <h2>무슨 문제였나</h2>
 * 이게 없으면 사용자가 {@code hibernate.dialect} 를 손으로 적어야 한다. 안 적으면
 * 부팅이 이렇게 죽는다.
 *
 * <pre>
 * HibernateException: Unable to determine Dialect for Tibero 7.0
 *     (please set 'hibernate.dialect' or register a Dialect resolver)
 * </pre>
 *
 * 메시지가 알려주는 그 "Dialect resolver" 가 이 클래스다.
 *
 * <h2>등록 방법 — 클래스만 있으면 아무 일도 안 일어난다</h2>
 * 이 클래스는 {@link java.util.ServiceLoader} 로 발견된다. jar 안에
 * {@code META-INF/services/org.hibernate.engine.jdbc.dialect.spi.DialectResolver}
 * 파일이 있고 거기에 이 클래스의 FQN 이 적혀 있어야 한다.
 *
 * <p>그 파일은 {@code resources/} 아래에 있고, {@code build.gradle} 의
 * {@code sourceSets.main.resources.srcDirs} 로 패키징된다. <b>그 선언이 없으면
 * 서비스 파일이 jar 에 안 들어가고 자동 인식이 통째로 죽는다</b> — 클래스는 멀쩡한데
 * 기능만 사라지는 조용한 고장이라 {@code contract.DialectResolverRegistrationTest}
 * 가 그 회귀를 지킨다.
 *
 * <h2>왜 제품명만 보지 않는가</h2>
 * tbjdbc 는 {@code databaseProductName} <b>커넥션 프로퍼티로 제품명을 덮어쓸 수
 * 있다</b>(실측). Oracle 전용 도구를 통과시키려고 이 값을 {@code "Oracle"} 로
 * 박아둔 배포가 실제로 있을 수 있다.
 *
 * <pre>
 * 프로퍼티 없음              productName=[Tibero]  driverName=[Tibero Tibero_7.2....]
 * databaseProductName=Oracle  productName=[Oracle]  driverName=[Tibero Tibero_7.2....]
 *                                        ^^^^^^                 ^^^^^^ 여긴 안 덮인다
 * </pre>
 *
 * 제품명만 보면 이 경우 {@code null} 을 돌려주게 되고, 그러면 뒤에 있는 Hibernate
 * 내장 {@code StandardDialectResolver} 의 ORACLE 항목이 물어서 <b>Tibero 서버에
 * {@code OracleDialect} 가 조용히 붙는다.</b> 부팅은 성공하고 나중에 SQL 이 깨진다.
 *
 * <p>그래서 드라이버명으로 한 번 더 본다. Hibernate 자신도 같은 상황을 같은 방식으로
 * 처리한다 — {@code Database.MARIADB.matchesResolutionInfo} 가 제품명이 MySQL 로
 * 위장된 MariaDB 를 드라이버명으로 잡아낸다.
 *
 * <h2>기존 사용자에게 영향이 없는 이유</h2>
 * {@code hibernate.dialect} 가 설정돼 있으면 Hibernate 는 resolver 를 <b>아예 부르지
 * 않는다</b>({@code DialectFactoryImpl.buildDialect} 가 설정값 유무로 먼저 갈린다).
 * 명시 설정이 항상 이기므로 자동 인식이 기존 동작을 덮어쓸 일이 없고, 문제가 생기면
 * {@code hibernate.dialect} 를 적는 것이 확실한 탈출구다.
 *
 * @see TiberoDialectSelector {@code hibernate.dialect=Tibero} 짧은 이름 지원
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
