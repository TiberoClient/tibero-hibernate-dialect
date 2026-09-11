package support;

import jakarta.persistence.EntityManagerFactory;
import jakarta.persistence.LockModeType;
import org.hibernate.LockMode;
import org.hibernate.LockOptions;
import org.hibernate.boot.MetadataSources;
import org.hibernate.boot.registry.StandardServiceRegistry;
import org.hibernate.boot.registry.StandardServiceRegistryBuilder;
import org.hibernate.engine.jdbc.connections.spi.ConnectionProvider;
import org.hibernate.engine.spi.LoadQueryInfluencers;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.spi.Limit;
import org.hibernate.query.spi.QueryOptions;
import org.hibernate.query.spi.QueryOptionsAdapter;
import org.hibernate.query.spi.QueryParameterBindings;
import org.hibernate.query.sqm.internal.DomainParameterXref;
import org.hibernate.query.sqm.sql.SqmTranslator;
import org.hibernate.query.sqm.tree.SqmStatement;
import org.hibernate.query.sqm.tree.select.SqmSelectStatement;
import org.hibernate.sql.ast.tree.select.SelectStatement;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.util.Map;

/**
 * DB 없이 Hibernate 산출물(SQL·DDL)을 뽑는 하네스.
 *
 * <p>더미 {@link ConnectionProvider} 와 {@code allow_jdbc_metadata_access=false} 로
 * SessionFactory 를 띄우면 커넥션 없이도 SQM → SQL AST → SQL 문자열 경로가 끝까지 돈다.
 * 렌더 단계에서만 드러나는 회귀(문법 조합, 에뮬레이션 선택)를 CI에서 잡기 위한 것이다.
 */
public final class RenderSupport implements AutoCloseable {

    /** 커넥션을 절대 내주지 않는 provider — 부팅은 되지만 DB 접근은 불가하다. */
    public static class NoConnectionProvider implements ConnectionProvider {
        @Override public Connection getConnection() { throw new UnsupportedOperationException("DB 미사용 하네스"); }
        @Override public void closeConnection(Connection conn) { }
        @Override public boolean supportsAggressiveRelease() { return false; }
        @Override public boolean isUnwrappableAs(Class<?> unwrapType) { return false; }
        @Override public <T> T unwrap(Class<T> unwrapType) { return null; }
    }

    private final SessionFactoryImplementor sf;

    private RenderSupport(SessionFactoryImplementor sf) { this.sf = sf; }

    public static RenderSupport withEntities(Class<?>... entities) {
        return build(null, java.util.Collections.emptyMap(), entities);
    }

    /**
     * 옵트인 설정을 켜고 부팅한다.
     *
     * <p>{@code hibernate.query.hql.portable_integer_division} 처럼 <b>켜야만 특정 AST 노드가
     * 생성되는</b> 설정이 있어서, 그 경로의 렌더를 고정하려면 설정을 실어 부팅해야 한다.
     */
    public static RenderSupport withSettings(Map<String, String> settings, Class<?>... entities) {
        return build(null, settings, entities);
    }

    /** DDL 스크립트를 파일로 뽑고 싶을 때 사용한다. */
    public static RenderSupport withDdlScript(Path target, Class<?>... entities) {
        return build(target, java.util.Collections.emptyMap(), entities);
    }

    private static RenderSupport build(Path ddlTarget, Map<String, String> settings, Class<?>... entities) {
        final StandardServiceRegistryBuilder b = new StandardServiceRegistryBuilder()
                .applySetting("hibernate.dialect", "com.tmax.tibero.hibernate.dialect.TiberoDialect")
                .applySetting("hibernate.boot.allow_jdbc_metadata_access", "false")
                .applySetting("hibernate.connection.provider_class", NoConnectionProvider.class.getName())
                .applySetting("hibernate.hbm2ddl.auto", "none");
        settings.forEach(b::applySetting);
        if (ddlTarget != null) {
            b.applySetting("jakarta.persistence.schema-generation.scripts.action", "create")
             .applySetting("jakarta.persistence.schema-generation.scripts.create-target", ddlTarget.toString())
             .applySetting("hibernate.hbm2ddl.delimiter", ";");
        }
        final StandardServiceRegistry sr = b.build();
        final MetadataSources ms = new MetadataSources(sr);
        for (Class<?> e : entities) {
            ms.addAnnotatedClass(e);
        }
        return new RenderSupport((SessionFactoryImplementor) ms.buildMetadata().buildSessionFactory());
    }

    /** HQL 을 최종 JDBC SQL 문자열까지 렌더한다. */
    public String sql(String hql) {
        return sql(hql, QueryOptions.NONE);
    }

    /**
     * 락 모드와 페이징을 얹어 렌더한다.
     *
     * <p>페이징 + 락 조합은 dialect 마다 전혀 다른 SQL 로 갈라지는데
     * ({@code for update} 를 그대로 붙일지, 서브쿼리로 감쌀지, 별도 쿼리로 뺄지)
     * 그 갈림은 {@link QueryOptions} 를 통해서만 전달되므로 여기서 실어 준다.
     *
     * @param firstResult 0 이하면 offset 없음
     * @param maxResults  0 이하면 limit 없음
     */
    public String sqlWithLock(String hql, LockModeType lockMode, int firstResult, int maxResults) {
        final Limit limit = new Limit();
        if (firstResult > 0) {
            limit.setFirstRow(firstResult);
        }
        if (maxResults > 0) {
            limit.setMaxRows(maxResults);
        }
        final LockOptions lockOptions = new LockOptions(LockMode.fromJpaLockMode(lockMode));
        return sql(hql, new QueryOptionsAdapter() {
            @Override public Limit getLimit() { return limit; }
            @Override public LockOptions getLockOptions() { return lockOptions; }
        });
    }

    private String sql(String hql, QueryOptions options) {
        final SqmStatement<?> sqm = sf.getQueryEngine().getHqlTranslator().translate(hql, null);
        final SqmTranslator<SelectStatement> translator = sf.getQueryEngine().getSqmTranslatorFactory()
                .createSelectTranslator(
                        (SqmSelectStatement<?>) sqm,
                        options,
                        DomainParameterXref.EMPTY,
                        QueryParameterBindings.NO_PARAM_BINDINGS,
                        new LoadQueryInfluencers(sf),
                        sf,
                        true);
        return sf.getJdbcServices().getJdbcEnvironment().getSqlAstTranslatorFactory()
                .buildSelectTranslator(sf, translator.translate().getSqlAst())
                .translate(null, options)
                .getSqlString();
    }

    public static String readDdl(Path target) throws IOException {
        return new String(Files.readAllBytes(target)).trim();
    }

    public SessionFactoryImplementor sessionFactory() { return sf; }

    @Override public void close() { sf.close(); }
}
