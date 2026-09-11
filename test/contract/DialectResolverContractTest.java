package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import com.tmax.tibero.hibernate.dialect.TiberoDialectResolver;
import com.tmax.tibero.hibernate.dialect.TiberoDialectSelector;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * {@code hibernate.dialect} 없이도 Tibero 를 알아내는 resolver 의 계약 (DB 불필요).
 *
 * <h2>무엇을 지키는가</h2>
 * resolver 는 <b>판정 하나</b>가 전부다 — "이 JDBC 메타데이터가 Tibero 인가". 그 판정이
 * 틀리면 두 방향으로 사고가 난다.
 *
 * <ul>
 *   <li><b>너무 좁으면</b> Tibero 를 못 알아보고, 뒤에 있는 Hibernate 내장 resolver 가
 *       엉뚱한 dialect 를 물린다. 부팅은 성공하고 나중에 SQL 이 깨진다.</li>
 *   <li><b>너무 넓으면</b> 남의 DB 에 TiberoDialect 를 붙인다. 더 나쁘다.</li>
 * </ul>
 *
 * <p>게다가 여기서 <b>예외를 던지면 아무 데도 안 남는다</b> —
 * {@code DialectResolverSet} 이 resolver 의 예외를 INFO 로그 한 줄로 삼키고 다음으로
 * 넘어간다. 그래서 {@code null} 방어까지 테스트로 고정한다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialectResolver
 * @see contract.DialectResolverRegistrationTest 서비스 파일 등록 여부
 */
public class DialectResolverContractTest {

    private final TiberoDialectResolver resolver = new TiberoDialectResolver();

    private Dialect resolve(String databaseName, String driverName) {
        return resolver.resolveDialect(info(databaseName, driverName));
    }

    // ------------------------------------------------------------------
    // 잡아야 하는 것
    // ------------------------------------------------------------------

    /** ps06 실측값 그대로. */
    @Test
    public void measuredMetadata_resolvesToTibero() {
        final Dialect d = resolve("Tibero", "Tibero Tibero_7.2.9afaec48_JDBC_11_release_20260902");
        assertTrue("실측 메타데이터는 TiberoDialect 로 풀려야 함: " + d, d instanceof TiberoDialect);
    }

    /** 제품명이 어떤 대소문자로 오든 잡는다. */
    @Test
    public void productName_isCaseInsensitive() {
        for (String name : new String[]{"Tibero", "tibero", "TIBERO", "TiBeRo"}) {
            assertTrue("대소문자와 무관해야 함: " + name,
                    resolve(name, null) instanceof TiberoDialect);
        }
    }

    /**
     * 앞뒤 공백을 허용한다.
     *
     * <p>형식적인 방어가 아니다 — 이 드라이버는 {@code getDriverVersion()} 에 실제로
     * 끝 공백을 흘린다({@code "7.2. "}). 제품명에도 같은 일이 생길 수 있다.
     */
    @Test
    public void productName_isTrimmed() {
        for (String name : new String[]{" Tibero", "Tibero ", " Tibero ", "Tibero\t"}) {
            assertTrue("공백이 붙어도 잡아야 함: [" + name + "]",
                    resolve(name, null) instanceof TiberoDialect);
        }
    }

    /**
     * ⚠️ 제품명이 {@code Oracle} 로 덮여 있어도 드라이버명으로 잡는다.
     *
     * <p>tbjdbc 는 {@code databaseProductName} 커넥션 프로퍼티로 제품명을 덮어쓸 수
     * 있다(실측 확인). Oracle 전용 도구를 통과시키려고 그렇게 박아둔 배포가 있을 수 있는데,
     * 제품명만 보면 <b>Tibero 서버에 OracleDialect 가 조용히 붙는다.</b>
     *
     * <p>Hibernate 자신도 같은 상황을 같은 방식으로 푼다 — MySQL 로 위장된 MariaDB 를
     * 드라이버명으로 잡아낸다.
     */
    @Test
    public void maskedProductName_isRecoveredByDriverName() {
        final Dialect d = resolve("Oracle", "Tibero Tibero_7.2.9afaec48_JDBC_11_release_20260902");
        assertTrue("제품명이 덮여도 드라이버명으로 복구돼야 함: " + d, d instanceof TiberoDialect);
    }

    // ------------------------------------------------------------------
    // 잡으면 안 되는 것
    // ------------------------------------------------------------------

    /** 남의 DB 는 건드리지 않는다. 여기서 잘못 물면 그쪽 부팅이 통째로 망가진다. */
    @Test
    public void otherDatabases_areNotClaimed() {
        assertNull(resolve("Oracle", "Oracle JDBC driver"));
        assertNull(resolve("PostgreSQL", "PostgreSQL JDBC Driver"));
        assertNull(resolve("MySQL", "MySQL Connector/J"));
        assertNull(resolve("H2", "H2 JDBC Driver"));
        assertNull(resolve("Microsoft SQL Server", "Microsoft JDBC Driver"));
    }

    /**
     * 유사한 이름에 넘어가지 않는다.
     *
     * <p>{@code startsWith} 로 짰다면 {@code "TiberoXX"} 를 물었을 것이다. 제품명은
     * 정확 일치로 본다 — Tibero 는 제품명 변종이 없어 느슨하게 볼 이유가 없다.
     */
    @Test
    public void similarProductNames_areNotClaimed() {
        assertNull("제품명은 정확 일치", resolve("TiberoXX", "SomeOther JDBC"));
        assertNull(resolve("NotTibero", "SomeOther JDBC"));
    }

    // ------------------------------------------------------------------
    // 절대 터지면 안 되는 것
    // ------------------------------------------------------------------

    /**
     * {@code null} 메타데이터에 NPE 를 내지 않는다.
     *
     * <p>여기서 NPE 가 나면 <b>Tibero 가 아닌 모든 DB 의 부팅</b>이 이 resolver 를
     * 지날 때마다 예외를 던지고, 그 예외는 {@code DialectResolverSet} 이 조용히
     * 삼켜서 원인 추적이 불가능해진다.
     */
    @Test
    public void nullMetadata_returnsNullNotNpe() {
        assertNull(resolve(null, null));
        assertNull(resolve(null, "Some Driver"));
        assertNull(resolve("SomeDb", null));
        assertNull(resolve("", ""));
    }

    /** 어떤 입력에도 예외를 던지지 않는다 — 예외는 삼켜지므로 던져봐야 소용없다. */
    @Test
    public void neverThrows() {
        final String[] weird = {null, "", " ", "\t\n", "Tibero", "오라클", "x".repeat(500)};
        for (String a : weird) {
            for (String b : weird) {
                try {
                    resolver.resolveDialect(info(a, b));
                }
                catch (RuntimeException e) {
                    fail("resolveDialect 가 예외를 던지면 안 됨 (" + a + " / " + b + "): " + e);
                }
            }
        }
    }

    // ------------------------------------------------------------------
    // 만들어진 dialect
    // ------------------------------------------------------------------

    /**
     * {@code DialectResolutionInfo} 생성자로 만든다.
     *
     * <p>무인자 생성자를 쓰면 버전이 {@code make(7)} 로 박제되고, 드라이버가 보고하는
     * 예약어({@code increment} · {@code pctfree})도 등록되지 않는다. 더 나쁜 건
     * <b>명시 설정 경로와 다른 인스턴스가 만들어진다</b>는 점이다 —
     * {@code DialectFactoryImpl} 은 사용자가 {@code hibernate.dialect} 를 적었을 때도
     * info 생성자를 먼저 시도한다. 두 경로가 갈리면 재현 안 되는 버그가 된다.
     */
    @Test
    public void resolvedDialect_usesTheInfoConstructor() {
        final Dialect viaResolver = resolve("Tibero", "Tibero Tibero_7.2...");
        final Dialect viaNoArg = new TiberoDialect();

        assertTrue(viaResolver.getKeywords().size() >= viaNoArg.getKeywords().size());
        assertEquals("기본 프로퍼티는 두 경로가 같아야 함",
                viaNoArg.getDefaultProperties(), viaResolver.getDefaultProperties());
    }

    // ------------------------------------------------------------------
    // 짧은 이름
    // ------------------------------------------------------------------

    /** {@code hibernate.dialect=Tibero} 를 풀어준다. */
    @Test
    public void selector_resolvesShortName() {
        final TiberoDialectSelector selector = new TiberoDialectSelector();
        assertEquals(TiberoDialect.class, selector.resolve("Tibero"));
        assertEquals(TiberoDialect.class, selector.resolve("tibero"));
        assertNull("남의 이름은 안 받는다", selector.resolve("Oracle"));
        assertNull(selector.resolve(""));
    }

    // ------------------------------------------------------------------

    /** 필요한 두 값만 실어 나르는 최소 구현. */
    private static DialectResolutionInfo info(String databaseName, String driverName) {
        return new DialectResolutionInfo() {
            @Override public String getDatabaseName() { return databaseName; }
            @Override public String getDatabaseVersion() { return "7"; }
            @Override public int getDatabaseMajorVersion() { return 7; }
            @Override public int getDatabaseMinorVersion() { return 0; }
            @Override public String getDriverName() { return driverName; }
            @Override public int getDriverMajorVersion() { return 7; }
            @Override public int getDriverMinorVersion() { return 2; }
            @Override public String getSQLKeywords() { return ""; }
        };
    }
}
