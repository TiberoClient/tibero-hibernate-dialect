import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Lob;
import jakarta.persistence.Table;

import org.hibernate.Session;
import org.hibernate.cfg.Configuration;
import org.hibernate.dialect.Dialect;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.junit.Before;
import org.junit.Test;

import java.util.Locale;

import static org.junit.Assert.*;

/**
 * TiberoDialectLobOrderingTest
 *
 * 목적:
 *  - dialect.forceLobAsLastValue() == true 일 때
 *  - Hibernate가 생성하는 INSERT / UPDATE SQL에서
 *    LOB 컬럼이 일반 컬럼보다 뒤에 배치되는지 검증
 *
 * 방법:
 *  - StatementInspector로 생성 SQL 캡처
 *  - INSERT / UPDATE SQL에서 컬럼/SET 순서 파싱
 *
 * 주의:
 *  - "LOB이 절대 마지막 컬럼"까지 강제하지 않고,
 *    "일반 컬럼보다 뒤에 오는지"를 기준으로 검증
 *    (ID 컬럼 위치는 전략에 따라 달라질 수 있음)
 *  - 또한, tibero가 강제하는 규칙을 검증하는 것이
 *    아니라 hibernate가 안전하게 처리하도록 하는 힌트임
 */
public class LobOrderingTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private Dialect dialect;

    // 엔티티 테스트에서는 unique table name 사용x
    private static final String tableName = "LOB_ORDER_TEST";

    private final String ddl = "create table " + tableName + " (" +
            "id number primary key," +
            "name varchar2(100)," +
            "lob_data clob" +
            ")";

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] { LobOrderEntity.class };
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        // SQL 캡처용 StatementInspector 등록
        cfg.setProperty("hibernate.session_factory.statement_inspector", SqlCaptureInspector.class.getName());
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        dialect = sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);

        /**
         * 두 테스트가 같은 테이블을 사용하기 때문에 매 테스트 전에 테이블 생성 및 truncate 상태를 보장
         */
        ensureTableExists(tableName, ddl);      // 없으면 create
        truncateTable(tableName);          // 데이터 초기화

        SqlCaptureInspector.clear();
    }

    /**
     * INSERT SQL에서 LOB 컬럼이 일반 컬럼보다 뒤에 오는지 검증
     */
    @Test
    public void testForceLobAsLastValue_InsertSqlOrder() {
        assertTrue("Dialect should force LOB as last value", dialect.forceLobAsLastValue());

        try {
            inTransaction(session -> {
                LobOrderEntity e = new LobOrderEntity();
                e.setTable(tableName);
                e.setName("name1");
                e.setLobData("lob-data-1");
                session.persist(e);
            });

            String insertSql = findFirstSqlStartingWith("insert into " + tableName);

            assertNotNull("INSERT SQL should be captured", insertSql);
            assertTrue("INSERT SQL should contain column list", insertSql.contains("("));

            // INSERT 컬럼 목록에서 NAME < LOB_DATA 인지 확인
            String columnsPart = extractInsertColumnsPart(insertSql);
            assertNotNull("Could not extract columns part from INSERT", columnsPart);

            assertContainsColumn(columnsPart, "NAME");
            assertContainsColumn(columnsPart, "LOB_DATA");

            int nameIdx = indexOfColumn(columnsPart, "NAME");
            int lobIdx = indexOfColumn(columnsPart, "LOB_DATA");

            assertTrue(
                    "LOB_DATA should appear after NAME in INSERT column list.\nColumns: " + columnsPart,
                    lobIdx > nameIdx
            );

        } finally {
            dropTableWithRetry(tableName);
        }
    }

    /**
     * UPDATE SQL에서 LOB 컬럼이 일반 컬럼보다 뒤에 오는지 검증
     */
    @Test
    public void testForceLobAsLastValue_UpdateSqlOrder() {
        assertTrue("Dialect should force LOB as last value", dialect.forceLobAsLastValue());

        Long id;
        try {
            // 1) insert first
            id = inTransactionReturning(session -> {
                LobOrderEntity e = new LobOrderEntity();
                e.setTable(tableName);
                e.setName("name1");
                e.setLobData("lob-data-1");
                session.persist(e);
                return e.getId();
            });

            SqlCaptureInspector.clear();

            // 2) update both columns (NAME + LOB_DATA)
            inTransaction(session -> {
                LobOrderEntity e = session.find(LobOrderEntity.class, id);
                e.setName("name2");
                e.setLobData("lob-data-2");
            });

            String updateSql = findFirstSqlStartingWith("update " + tableName);
            assertNotNull("UPDATE SQL should be captured", updateSql);

            // UPDATE SET 절에서 NAME < LOB_DATA 인지 확인
            String setPart = extractUpdateSetPart(updateSql);
            assertNotNull("Could not extract SET part from UPDATE", setPart);

            assertContainsAssignment(setPart, "NAME");
            assertContainsAssignment(setPart, "LOB_DATA");

            int nameIdx = indexOfAssignment(setPart, "NAME");
            int lobIdx = indexOfAssignment(setPart, "LOB_DATA");

            assertTrue(
                    "LOB_DATA should appear after NAME in UPDATE SET clause.\nSET: " + setPart,
                    lobIdx > nameIdx
            );

        } finally {
            dropTableWithRetry(tableName);
        }
    }

    // =========================================================================
    // Helpers
    // =========================================================================

    /**
     * insert SQL에서 "insert into TABLE ( ... ) values ..." 의 (...) 부분만 추출
     */
    private String extractInsertColumnsPart(String sql) {
        // normalize
        String lower = sql.toLowerCase(Locale.ROOT);
        int firstParen = lower.indexOf('(');
        if (firstParen < 0) return null;

        // INSERT 컬럼 리스트 닫는 ')'는 values 직전 괄호
        int valuesIdx = lower.indexOf(" values", firstParen);
        if (valuesIdx < 0) return null;

        int lastParenBeforeValues = lower.lastIndexOf(')', valuesIdx);
        if (lastParenBeforeValues < 0) return null;

        return sql.substring(firstParen + 1, lastParenBeforeValues);
    }

    /**
     * update SQL에서 "update TABLE set ... where ..." 의 set ... 부분만 추출
     */
    private String extractUpdateSetPart(String sql) {
        String lower = sql.toLowerCase(Locale.ROOT);
        int setIdx = lower.indexOf(" set ");
        if (setIdx < 0) return null;

        int whereIdx = lower.indexOf(" where ", setIdx);
        if (whereIdx < 0) {
            // where가 없을 수도 있으니 끝까지
            return sql.substring(setIdx + 5);
        }
        return sql.substring(setIdx + 5, whereIdx);
    }

    private void assertContainsColumn(String columnsPart, String column) {
        String normalized = normalize(columnsPart);
        assertTrue(
                "Column list should contain '" + column + "'. Columns=" + columnsPart,
                normalized.contains(column.toLowerCase(Locale.ROOT))
        );
    }

    private int indexOfColumn(String columnsPart, String column) {
        return normalize(columnsPart).indexOf(column.toLowerCase(Locale.ROOT));
    }

    private void assertContainsAssignment(String setPart, String column) {
        String normalized = normalize(setPart);
        assertTrue(
                "SET clause should contain assignment for '" + column + "'. SET=" + setPart,
                normalized.contains(column.toLowerCase(Locale.ROOT) + "=")
        );
    }

    private int indexOfAssignment(String setPart, String column) {
        return normalize(setPart).indexOf(column.toLowerCase(Locale.ROOT) + "=");
    }

    private String normalize(String s) {
        // 공백 제거, 따옴표 제거(quoted identifier 대응 최소화)
        return s.replace(" ", "")
                .replace("\"", "")
                .toLowerCase(Locale.ROOT);
    }

    private String findFirstSqlStartingWith(String prefix) {
        String lowerPrefix = prefix.toLowerCase(Locale.ROOT);
        return SqlCaptureInspector.getSqls().stream()
                .map(String::trim)
                .filter(sql -> sql.toLowerCase(Locale.ROOT).startsWith(lowerPrefix))
                .findFirst()
                .orElse(null);
    }


    // =========================================================================
    // Test Entity
    // =========================================================================

    /**
     * 테이블명을 동적으로 사용하기 위해 @Table을 고정하지 않고,
     * Hibernate PhysicalNamingStrategy나 동적 테이블명을 적용하는 방식은 복잡하므로,
     * 여기서는 @Table(name="LOB_ORDER_TEST") 고정 + create/drop은 BaseCoreFunctionalTestCase가 해주지 않기 때문에,
     * 동적 테이블명을 반영하려면 insert/update SQL capture에서 실제 table name이 고정되어야 한다.
     *
     * 따라서 이 엔티티는 실제 테이블명이 @Table에 의해 고정되며,
     * setTable(tableName)은 테스트에서 SQL prefix matching용으로만 사용한다.
     *
     * 만약 진짜로 테이블명을 동적으로 만들고 싶다면:
     *  - 별도 엔티티를 테스트마다 생성하거나
     *  - Hibernate 매핑을 런타임에 바꾸는 방식이 필요해서
     *  - 일반적으로 권장되지 않는다.
     *
     * 결론: 이 테스트는 "LOB 컬럼이 마지막으로 배치되는 SQL 생성"을 보기 위함이므로
     *       테이블명은 고정하는 것이 더 안정적이다.
     */
    @Entity(name = "LobOrderEntity")
    @Table(name = "LOB_ORDER_TEST")
    public static class LobOrderEntity {

        @Id
        @GeneratedValue(strategy = GenerationType.AUTO)
        private Long id;

        @Column(name = "NAME")
        private String name;

        @Lob
        @Column(name = "LOB_DATA")
        private String lobData;

        // 테스트 편의를 위해 사용 (실제 매핑엔 영향 없음)
        @jakarta.persistence.Transient
        private String table;

        public Long getId() { return id; }

        public String getName() { return name; }
        public String getLobData() { return lobData; }

        public void setName(String name) { this.name = name; }
        public void setLobData(String lobData) { this.lobData = lobData; }

        public void setTable(String table) { this.table = table; }
    }
}
