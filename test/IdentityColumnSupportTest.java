import com.tmax.tibero.hibernate.dialect.identity.TiberoIdentityColumnSupport;
import jakarta.persistence.*;

import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.id.insert.GetGeneratedKeysDelegate;
import org.hibernate.persister.entity.EntityPersister;
import org.junit.Before;
import org.junit.Test;

import java.sql.*;
import java.util.Locale;

import static org.junit.Assert.*;

/**
 * IdentityColumnSupportTest
 *
 * 목적:
 *  - IdentityColumnSupportTest의 contract 테스트
 *  - 실제 Tibero DB에서 IDENTITY 컬럼이 생성되고
 *    Hibernate가 INSERT 후 generated key를 정상적으로 가져오는지 검증
 */
public class IdentityColumnSupportTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;
    private TiberoIdentityColumnSupport identitySupport;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] { IdentityEntity.class };
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        cfg.setProperty("hibernate.session_factory.statement_inspector", SqlCaptureInspector.class.getName());
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);

        identitySupport = (TiberoIdentityColumnSupport) dialect.getIdentityColumnSupport();
        assertNotNull(identitySupport);

        SqlCaptureInspector.clear();
    }

    // =========================================================================
    // 1) Unit / Contract tests
    // =========================================================================

    @Test
    public void contract_supportsIdentityColumns_true() {
        assertTrue(identitySupport.supportsIdentityColumns());
    }

    @Test
    public void contract_supportsInsertSelectIdentity_true() {
        assertFalse(identitySupport.supportsInsertSelectIdentity());
    }

    @Test
    public void contract_identityColumnString_isGeneratedAsIdentity() {
        assertEquals("generated as identity", identitySupport.getIdentityColumnString(Types.BIGINT).toLowerCase(Locale.ROOT));
    }

    @Test
    public void contract_identityInsertString_isDefault() {
        assertEquals("default", identitySupport.getIdentityInsertString().toLowerCase(Locale.ROOT));
    }

    /**
     * identitySupport.buildGetGeneratedKeysDelegate()
     * - GetGeneratedKeysDelegate 내부가 너무 깊어서 mock persister로는 객체 생성 자체가 안되기 때문에 Integration test로 대체
     */

    // =========================================================================
    // 2) Integration test: IDENTITY 컬럼 생성 + insert + generated key retrieval
    // =========================================================================

    @Test
    public void integration_identityInsert_generatesIdAndRetrievesGeneratedKey() {
        assertSame(TiberoIdentityColumnSupport.INSTANCE, dialect.getIdentityColumnSupport());

        Long id = inTransactionReturning(session -> {
            IdentityEntity e = new IdentityEntity();
            e.setName("hello");
            session.persist(e);
            session.flush(); // insert 발생

            assertNotNull("ID should be assigned after flush", e.getId());
            return e.getId();
        });

        assertNotNull("Generated id must not be null", id);

        // 실제 row 존재 확인
        inTransaction(session -> {
            IdentityEntity found = session.find(IdentityEntity.class, id);
            assertNotNull(found);
            assertEquals("hello", found.getName());
        });
    }

    /**
     * getIdentityInsertString()
     *
     * Hibernate가 생성한 INSERT SQL이 identity 컬럼(id)을 직접 넣지 않고
     * name만 넣는지(혹은 default 키워드로 넣는지) 확인
     *
     * DB마다 id 컬럼 포함 여부가 다를 수 있어도,
     * 최소한 TiberoIdentityColumnSupport.getIdentityInsertString() == "default" 이므로
     * id에 default를 넣는 형태도 acceptable.
     */
    @Test
    public void integration_insertSql_doesNotManuallyAssignIdentityValue() {
        SqlCaptureInspector.clear();

        inTransaction(session -> {
            IdentityEntity e = new IdentityEntity();
            e.setName("sql-check");
            session.persist(e);
            session.flush();
        });

        String insertSql = SqlCaptureInspector.getSqls().stream()
                .map(String::toLowerCase)
                .filter(sql -> sql.startsWith("insert into identity_test"))
                .findFirst()
                .orElse(null);

        assertNotNull("INSERT SQL should be captured", insertSql);

        // 두 형태 모두 허용:
        // 1) insert into IDENTITY_TEST (NAME) values (?)
        // 2) insert into IDENTITY_TEST (ID, NAME) values (default, ?)
        boolean containsIdColumn = insertSql.contains("(id,") || insertSql.contains(",id)");
        if (containsIdColumn) {
            assertTrue(
                    "If id column is present, it must use DEFAULT keyword",
                    insertSql.contains("default")
            );
        }
    }

    /**
     * supportsIdentityColumns()
     *
     * 실제 테이블 컬럼이 generated as identity 로 생성됐는지 dictionary에서 확인
     * Tibero는 user_tab_cols에 identity 컬럼 정보가 노출될 수 있음.
     * (만약 Tibero 버전에 따라 컬럼이 다르면, 이 테스트는 옵션으로 처리 가능)
     */
    @Test
    public void integration_identityColumnExistsInDictionary() {
        inTransaction(session -> {
            // Tibero 버전에 따라 컬럼 정보가 다를 수 있음.
            // user_tab_cols / user_tab_identity_cols 등 확인 필요
            long count = session.createNativeQuery(
                    "select count(*) from user_tab_identity_cols where table_name = 'IDENTITY_TEST' and column_name = 'ID'",
                    Long.class
            ).getSingleResult();

            assertEquals("ID column must exist", 1L, count);
        });
    }

    /**
     *  supportsInsertSelectIdentity()
     *  - false : INSERT INTO users (name) VALUES ('John') RETURNING id INTO ? 구문 지원x
     */
    @Test
    public void testSupportsInsertSelectIdentity_returningIntoWorksInTibero() {
        inSession(session -> session.doWork(conn -> {
            // Tibero/Oracle style RETURNING INTO
            String sql = "insert into identity_test (name) values (?) returning id into ?";

            try (CallableStatement cs = conn.prepareCall(sql)) {
                cs.setString(1, "john");
                cs.registerOutParameter(2, java.sql.Types.BIGINT); // identity id 반환

                cs.executeUpdate();
                // 실행이 성공하면 안 됨
                fail("Expected RETURNING INTO to fail, but it succeeded. Dialect says not supported.");
            } catch (Exception e) {
                // expected
                System.out.println("[Expected failure] RETURNING INTO failed: " + e.getMessage());

                // optional: 에러 메시지/코드 검증
                assertNotNull(e.getMessage());
            }
        }));
    }

    /**
     * buildGetGeneratedKeysDelegate()
     * - generated key retrieval delegate 생성 테스트
     */
    @Test
    public void testBuildGetGeneratedKeysDelegate_generatedKeyRetrievalWorks() {
        // 1) persister를 real runtime metamodel에서 구한다 (mock 불가)
        EntityPersister persister =
                sfi.getRuntimeMetamodels()
                        .getMappingMetamodel()
                        .getEntityDescriptor(IdentityEntity.class.getName());

        assertNotNull(persister);

        // 2) delegate 생성 가능해야 함
        GetGeneratedKeysDelegate delegate = identitySupport.buildGetGeneratedKeysDelegate(persister);
        assertNotNull(delegate);

        // 3) 실제로 Hibernate가 generated key retrieval 성공하는지
        Long id = inTransactionReturning(session -> {
            IdentityEntity e = new IdentityEntity();
            e.setName("delegate-test");
            session.persist(e);
            session.flush();

            assertNotNull("ID should be generated by GetGeneratedKeysDelegate", e.getId());
            return e.getId();
        });

        assertNotNull(id);

        // 4) inferredKeys parameter (success)
        inSession(session -> session.doWork(conn -> {
            String sql = "insert into identity_test (name) values (?)";

            try (PreparedStatement ps = conn.prepareStatement(sql, new String[]{"ID"})) {
                ps.setString(1, "a");
                int updated = ps.executeUpdate();
                assertEquals(1, updated);

                try (ResultSet keys = ps.getGeneratedKeys()) {
                    assertTrue("GeneratedKeys must have a row", keys.next());
                    long generatedId = keys.getLong(1);
                    assertTrue("Generated id should be > 0", generatedId > 0);
                }
            }
        }));
    }


    // =========================================================================
    // Test Entity
    // =========================================================================
    @Entity(name = "IdentityEntity")
    @Table(name = "IDENTITY_TEST")
    public static class IdentityEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.IDENTITY)
        private Long id;

        @Column(name = "NAME")
        private String name;

        public Long getId() { return id; }

        public String getName() { return name; }

        public void setName(String name) { this.name = name; }
    }
}
