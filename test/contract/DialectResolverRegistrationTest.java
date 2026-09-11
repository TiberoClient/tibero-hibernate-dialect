package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialectResolver;
import com.tmax.tibero.hibernate.dialect.TiberoDialectSelector;
import org.hibernate.boot.registry.BootstrapServiceRegistry;
import org.hibernate.boot.registry.BootstrapServiceRegistryBuilder;
import org.hibernate.boot.registry.classloading.spi.ClassLoaderService;
import org.hibernate.boot.registry.selector.spi.DialectSelector;
import org.hibernate.engine.jdbc.dialect.spi.DialectResolver;
import org.junit.Test;

import java.util.Collection;
import java.util.ServiceLoader;

import static org.junit.Assert.*;

/**
 * resolver 가 <b>실제로 발견되는지</b> 확인한다 (DB 불필요).
 *
 * <h2>왜 이 테스트가 따로 필요한가</h2>
 * {@link TiberoDialectResolver} 는 코드가 아무리 정확해도 <b>{@code META-INF/services}
 * 파일이 jar 에 없으면 존재하지 않는 것과 같다.</b> 클래스는 멀쩡히 컴파일되고 단위
 * 테스트도 다 통과하는데 기능만 조용히 사라지는, 알아차리기 가장 어려운 부류의 고장이다.
 *
 * <p>실제로 그 위험이 있었다. 이 프로젝트의 {@code build.gradle} 은 원래
 * {@code sourceSets.main.resources} 를 선언하지 않아 Gradle 기본값
 * {@code src/main/resources} 를 보고 있었고, 그 디렉터리는 존재하지 않았다. 배포된
 * {@code tibero-hibernate-dialect-6.6.0.jar} 를 열어보면 {@code META-INF} 아래에
 * {@code MANIFEST.MF} 밖에 없다 — 서비스 파일을 넣었어도 패키징되지 않았을 것이다.
 *
 * <p>그래서 이 테스트는 <b>빌드 설정의 회귀</b>를 지킨다. 누가
 * {@code resources { srcDirs = ['resources'] }} 를 지우면 여기서 걸린다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialectResolver
 */
public class DialectResolverRegistrationTest {

    /** 순수 JDK {@link ServiceLoader} 로 보이는가 — 서비스 파일 자체의 존재 확인. */
    @Test
    public void resolverIsDiscoverableByServiceLoader() {
        boolean found = false;
        for (DialectResolver r : ServiceLoader.load(DialectResolver.class)) {
            if (r instanceof TiberoDialectResolver) {
                found = true;
                break;
            }
        }
        assertTrue("META-INF/services/org.hibernate.engine.jdbc.dialect.spi.DialectResolver 가 "
                + "classpath 에 없다. build.gradle 의 sourceSets.main.resources 선언을 확인할 것", found);
    }

    /**
     * Hibernate 가 실제로 쓰는 경로로도 보이는가.
     *
     * <p>Hibernate 는 {@code ServiceLoader} 를 직접 쓰지 않고
     * {@code ClassLoaderService.loadJavaServices()} 를 거친다. 그쪽은 classpath 와
     * module path 를 함께 훑으므로 결과가 다를 수 있어 따로 확인한다.
     */
    @Test
    public void resolverIsDiscoverableByHibernate() {
        final BootstrapServiceRegistry registry = new BootstrapServiceRegistryBuilder().build();
        try {
            final Collection<DialectResolver> resolvers = registry
                    .getService(ClassLoaderService.class)
                    .loadJavaServices(DialectResolver.class);
            assertTrue("Hibernate 의 서비스 로딩 경로에서 TiberoDialectResolver 가 보여야 함: " + resolvers,
                    resolvers.stream().anyMatch(r -> r instanceof TiberoDialectResolver));
        }
        finally {
            registry.close();
        }
    }

    /** 짧은 이름 selector 도 같은 방식으로 등록돼 있는가. */
    @Test
    public void selectorIsDiscoverable() {
        final BootstrapServiceRegistry registry = new BootstrapServiceRegistryBuilder().build();
        try {
            final Collection<DialectSelector> selectors = registry
                    .getService(ClassLoaderService.class)
                    .loadJavaServices(DialectSelector.class);
            assertTrue("TiberoDialectSelector 가 보여야 함: " + selectors,
                    selectors.stream().anyMatch(s -> s instanceof TiberoDialectSelector));
        }
        finally {
            registry.close();
        }
    }

    /**
     * ⚠️ 테스트 전용 서비스 파일이 배포물에 섞이지 않았는지.
     *
     * <p>저장소 루트에 {@code META-INF/services/org.jboss.logging.LoggerProvider} 가 있고
     * 내용이 {@code org.hibernate.testing.logger.TestableLoggerProvider} — <b>hibernate-testing
     * 전용</b>이다. 리소스 경로를 {@code '.'} 로 잡는 실수를 하면 이게 배포 jar 에 들어가
     * 운영 환경 로깅이 깨진다.
     *
     * <p>우리 리소스 디렉터리에는 dialect 관련 서비스 파일만 있어야 한다.
     */
    @Test
    public void noTestOnlyLoggerProviderIsOnTheMainResourcePath() {
        // 파일 경로가 아니라 클래스패스로 본다 — 작업 디렉터리에 의존하지 않게.
        // 우리 서비스 파일이 있는 디렉터리를 찾아 그 옆에 LoggerProvider 가 있는지 확인한다.
        final java.net.URL ours = getClass().getClassLoader()
                .getResource("META-INF/services/org.hibernate.engine.jdbc.dialect.spi.DialectResolver");
        assertNotNull("우리 서비스 파일이 클래스패스에 있어야 함", ours);

        final java.io.File servicesDir =
                new java.io.File(ours.getPath()).getParentFile();
        final String[] names = servicesDir.list();
        assertNotNull(names);
        for (String name : names) {
            assertFalse("테스트 전용 LoggerProvider 가 배포 리소스 디렉터리에 들어오면 안 됨: "
                    + servicesDir + "/" + name, name.contains("LoggerProvider"));
        }
    }
}
