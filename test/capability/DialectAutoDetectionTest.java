package capability;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.junit.Test;

import java.util.LinkedHashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * {@code hibernate.dialect} 없이 실제 Tibero 에 붙어 dialect 가 자동으로 정해지는지 본다.
 *
 * <h2>무엇이 달라지나</h2>
 * 이전에는 이 설정을 빠뜨리면 부팅이 이렇게 죽었다.
 *
 * <pre>
 * HibernateException: Unable to determine Dialect for Tibero 7.0
 *     (please set 'hibernate.dialect' or register a Dialect resolver)
 * </pre>
 *
 * <p>여기서는 <b>끝까지 붙여 보는 것</b>이 목적이다. resolver 의 판정 로직은
 * {@code contract.DialectResolverContractTest} 가, 서비스 파일 등록은
 * {@code contract.DialectResolverRegistrationTest} 가 이미 본다. 이 테스트가
 * 추가로 잡는 것은 <b>그 셋이 실제 커넥션 위에서 맞물리는가</b>이다.
 *
 * <p>기존 테스트들과 달리 {@code hibernate.properties} 를 쓰지 않는다 — 거기에
 * {@code hibernate.dialect} 가 박혀 있어서 자동 인식을 시험할 수 없기 때문이다.
 * 접속 정보는 {@code -D} 로 넘어온 값을 직접 읽는다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialectResolver
 */
public class DialectAutoDetectionTest {

    /**
     * {@code init-test.gradle} 이 전달하는 {@code -D} 값을 쓴다.
     *
     * <p>기본값을 ps06(9999)로 두었다. {@code hibernate.properties} 의 기본값은
     * 8888(ps05)이라 그걸 쓰면 엉뚱한 인스턴스로 나간다.
     */
    private static Map<String, Object> settings() {
        final Map<String, Object> m = new LinkedHashMap<>();
        m.put("hibernate.connection.driver_class", "com.tmax.tibero.jdbc.TbDriver");
        m.put("hibernate.connection.url", System.getProperty("hibernate.connection.url",
                "jdbc:tibero:thin:@localhost:9999:tibero"));
        m.put("hibernate.connection.username",
                System.getProperty("hibernate.connection.username", "tibero"));
        m.put("hibernate.connection.password",
                System.getProperty("hibernate.connection.password", "tmax"));
        m.put("hibernate.hbm2ddl.auto", "none");
        return m;
    }

    /** 주어진 추가 설정으로 레지스트리를 띄워 결정된 dialect 를 돌려준다. */
    private static Dialect dialectFor(Map<String, Object> extra) {
        final StandardServiceRegistryBuilder b = new StandardServiceRegistryBuilder()
                .applySettings(settings());
        extra.forEach(b::applySetting);
        final StandardServiceRegistry registry = b.build();
        try {
            return registry.getService(JdbcEnvironment.class).getDialect();
        }
        finally {
            StandardServiceRegistryBuilder.destroy(registry);
        }
    }

    // ------------------------------------------------------------------

    /** 설정을 안 줘도 Tibero 로 붙는다 — 이 작업의 본론. */
    @Test
    public void withoutDialectSetting_tiberoIsDetected() {
        final Dialect d = dialectFor(Map.of());
        assertTrue("hibernate.dialect 없이도 TiberoDialect 여야 함: " + d.getClass().getName(),
                d instanceof TiberoDialect);
    }

    /**
     * 명시 설정이 항상 이긴다 — 기존 사용자 영향 없음.
     *
     * <p>Hibernate 는 {@code hibernate.dialect} 가 있으면 resolver 를 <b>아예 부르지
     * 않는다</b>. 자동 인식이 남의 설정을 덮어쓰지 않음을 고정한다. 문제가 생겼을 때
     * 사용자가 쓸 수 있는 탈출구이기도 하다.
     */
    @Test
    public void explicitSetting_alwaysWins() {
        final Dialect d = dialectFor(Map.of("hibernate.dialect",
                OracleDialect.class.getName()));
        assertEquals("명시 설정이 자동 인식을 이겨야 함",
                OracleDialect.class, d.getClass());
    }

    /** FQN 을 그대로 적는 기존 방식도 계속 동작한다. */
    @Test
    public void explicitFullyQualifiedName_stillWorks() {
        final Dialect d = dialectFor(Map.of("hibernate.dialect",
                TiberoDialect.class.getName()));
        assertTrue(d instanceof TiberoDialect);
    }

    /** {@code hibernate.dialect=Tibero} 짧은 이름이 풀린다. */
    @Test
    public void shortName_isResolved() {
        final Dialect d = dialectFor(Map.of("hibernate.dialect", "Tibero"));
        assertTrue("짧은 이름이 TiberoDialect 로 풀려야 함: " + d.getClass().getName(),
                d instanceof TiberoDialect);
    }

    /**
     * 자동 인식으로 얻은 dialect 가 명시 지정한 것과 같은 설정을 갖는지.
     *
     * <p>resolver 가 무인자 생성자를 쓰면 여기서 갈린다 — 버전이 박제되고 드라이버가
     * 보고하는 예약어가 빠진다. <b>같은 DB 인데 설정 방법에 따라 동작이 달라지는</b>
     * 상황을 막는다.
     */
    @Test
    public void autoDetected_matchesExplicitlyConfigured() {
        final Dialect auto = dialectFor(Map.of());
        final Dialect explicit = dialectFor(Map.of("hibernate.dialect",
                TiberoDialect.class.getName()));

        assertEquals("기본 프로퍼티가 같아야 함",
                explicit.getDefaultProperties(), auto.getDefaultProperties());
        assertEquals("예약어 목록이 같아야 함",
                explicit.getKeywords(), auto.getKeywords());
        // SimpleDatabaseVersion 은 equals 를 구현하지 않아 문자열로 비교한다
        assertEquals("버전이 같아야 함",
                String.valueOf(explicit.getVersion()), String.valueOf(auto.getVersion()));
    }

    /**
     * 자동 인식 경로에서도 Tibero 고유 예약어가 등록되는지.
     *
     * <p>예약어 등록은 {@code registerDefaultKeywords()} 에 넣었고 그건 두 생성자
     * 모두에서 불린다. 이 테스트는 <b>resolver 가 만든 실제 인스턴스</b>에서도
     * 그게 지켜지는지를 본다 — 두 작업이 맞물리는 지점이다.
     */
    @Test
    public void autoDetected_hasTiberoKeywords() {
        final Dialect d = dialectFor(Map.of());
        for (String word : new String[]{"size", "number", "least"}) {
            assertTrue("자동 인식 경로에서도 Tibero 예약어가 있어야 함: " + word,
                    d.getKeywords().contains(word));
        }
    }
}
