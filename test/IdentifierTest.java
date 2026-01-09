import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.testing.junit4.BaseCoreFunctionalTestCase;

import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.*;

public class IdentifierTest extends BaseCoreFunctionalTestCase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    /**
     * StatementInspector 등록
     */
    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.session_factory.statement_inspector", SqlCaptureInspector.class.getName());

        // 필요하면 디버깅을 위해 활성화
        // cfg.setProperty("hibernate.show_sql", "true");
        // cfg.setProperty("hibernate.format_sql", "true");
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] { AutoIdEntity.class };
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();

        assertNotNull(dialect);
        SqlCaptureInspector.clear();
    }

    /**
     * Contract Test:
     * TiberoDialect가 native(AUTO) 전략으로 sequence를 선택하도록 지정했는지 검증
     */
    @Test
    public void testNativeIdentifierGeneratorStrategyIsSequence() {
        String strategy = dialect.getNativeIdentifierGeneratorStrategy();
        assertNotNull(strategy);
        assertEquals("sequence", strategy);
    }

    /**
     * Integration Test:
     * @GeneratedValue(strategy = GenerationType.AUTO)가 실제 Tibero에서 정상적으로 ID를 생성하는지 검증
     */
    @Test
    public void testGeneratedValueAutoGeneratesId() {
        inTransaction(session -> {
            AutoIdEntity entity = new AutoIdEntity();
            session.persist(entity);

            assertNotNull("ID should be generated for AUTO strategy", entity.getId());
            assertTrue("Generated ID should be positive", entity.getId() > 0);
        });
    }

    /**
     * Inspection Test:
     * AUTO ID 생성 시 실제로 sequence nextval SQL이 호출되는지 검증
     *
     * - TiberoDialect가 native strategy를 "sequence"로 지정했으므로
     * - persist 시 sequence.nextval 쿼리가 실행되는 것이 기대된다.
     */
    @Test
    public void testGeneratedValueAutoTriggersSequenceNextValSql() {
        SqlCaptureInspector.clear();

        inTransaction(session -> {
            AutoIdEntity entity = new AutoIdEntity();
            session.persist(entity);

            assertNotNull(entity.getId());
        });

        // debug dump (실패 시 원인 확인에 도움)
        System.out.println("==== Captured SQLs ====");
        SqlCaptureInspector.getSqls().forEach(sql -> System.out.println("[SQL] " + sql));

        boolean usesSequenceNextval =
                SqlCaptureInspector.getSqls().stream()
                        .map(sql -> sql == null ? "" : sql.toLowerCase(Locale.ROOT))
                        .anyMatch(sql ->
                                sql.contains(".nextval")
                                        // nextval이 단독 select로 실행되는 패턴도 커버
                                        || (sql.contains("nextval") && sql.contains("select"))
                        );

        assertTrue("AUTO id generation should trigger sequence nextval", usesSequenceNextval);
    }

    // =========================================================================
    // Test Entity
    // =========================================================================

    @Entity(name = "AutoIdEntity")
    @Table(name = "AUTO_ID_ENTITY")
    public static class AutoIdEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        public Long getId() {
            return id;
        }
    }
}
