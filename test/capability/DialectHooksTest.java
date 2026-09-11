package capability;

import support.AbstractTiberoDialectTestBase;
import support.SqlCaptureInspector;
import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.PrimaryKeyJoinColumn;
import jakarta.persistence.SecondaryTable;
import jakarta.persistence.Table;
import jakarta.persistence.TemporalType;
import org.hibernate.annotations.RowId;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.cfg.BatchSettings;
import org.hibernate.cfg.Configuration;
import org.hibernate.engine.jdbc.env.spi.JdbcEnvironment;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.junit.Before;
import org.junit.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

import static org.junit.Assert.*;

/**
 * 전수조사에서 테스트 참조가 없었던 Dialect 훅 5개를 계약 + 실행으로 덮는다.
 *
 * - appendDateTimeLiteral
 * - buildIdentifierHelper (autoQuoteInitialUnderscore)
 * - createOptionalTableUpdateOperation (SecondaryTable → MERGE)
 * - rowId (@RowId)
 * - initDefaultProperties / registerDefaultProperties
 */
public class DialectHooksTest extends AbstractTiberoDialectTestBase {

    private TiberoDialect dialect;
    private SessionFactoryImplementor sfi;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{SecEntity.class, RowIdEntity.class, UnderscoreEntity.class};
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
        dialect = (TiberoDialect) sfi.getJdbcServices().getDialect();
        assertNotNull(dialect);
        SqlCaptureInspector.clear();
    }

    // ------------------------------------------------------------------------
    // initDefaultProperties / registerDefaultProperties
    // ------------------------------------------------------------------------

    @Test
    public void initDefaultProperties_setsBatchDefaults() {
        assertEquals(15, dialect.getDefaultStatementBatchSize());
        assertTrue(dialect.getDefaultUseGetGeneratedKeys());

        Properties defaults = dialect.getDefaultProperties();
        assertEquals("false", defaults.getProperty(BatchSettings.BATCH_VERSIONED_DATA));

        // SessionFactory에도 dialect 기본값이 반영되어야 함 (hibernate.properties 미지정)
        assertEquals(15, sfi.getSessionFactoryOptions().getJdbcBatchSize());
        assertTrue(sfi.getSessionFactoryOptions().isGetGeneratedKeysEnabled());
        assertFalse(sfi.getSessionFactoryOptions().isJdbcBatchVersionedData());
    }

    // ------------------------------------------------------------------------
    // rowId
    // ------------------------------------------------------------------------

    @Test
    public void rowId_returnsPseudoColumnName() {
        assertEquals("rowid", dialect.rowId("ignored"));
        assertEquals("rowid", dialect.rowId(null));
    }

    @Test
    public void rowId_annotation_usesRowidInUpdateSql() {
        inTransaction(session -> {
            RowIdEntity e = new RowIdEntity();
            e.id = 1L;
            e.v = "before";
            session.persist(e);
        });

        SqlCaptureInspector.clear();
        inTransaction(session -> {
            RowIdEntity e = session.get(RowIdEntity.class, 1L);
            e.v = "after";
        });

        List<String> sqls = SqlCaptureInspector.getSqls();
        boolean usedRowid = sqls.stream()
                .map(s -> s.toLowerCase(Locale.ROOT))
                .anyMatch(s -> s.contains("update") && s.contains("rowid"));
        assertTrue("UPDATE SQL에 rowid 가 포함되어야 함: " + sqls, usedRowid);

        String v = inTransactionReturning(session -> session.get(RowIdEntity.class, 1L).v);
        assertEquals("after", v);
    }

    // ------------------------------------------------------------------------
    // buildIdentifierHelper
    // ------------------------------------------------------------------------

    @Test
    public void buildIdentifierHelper_quotesInitialUnderscore() {
        JdbcEnvironment env = sfi.getJdbcServices().getJdbcEnvironment();
        Identifier id = env.getIdentifierHelper().toIdentifier("_hidden");
        assertTrue("선행 밑줄 식별자는 자동 인용되어야 함", id.isQuoted());

        Identifier normal = env.getIdentifierHelper().toIdentifier("normal_col");
        assertFalse("일반 식별자는 강제 인용되지 않아야 함", normal.isQuoted());
    }

    @Test
    public void underscoreColumn_roundTripOnTibero() {
        inTransaction(session -> {
            UnderscoreEntity e = new UnderscoreEntity();
            e.id = 1L;
            e.hidden = "x";
            session.persist(e);
        });
        String v = inTransactionReturning(session -> session.get(UnderscoreEntity.class, 1L).hidden);
        assertEquals("x", v);
    }

    // ------------------------------------------------------------------------
    // appendDateTimeLiteral
    // ------------------------------------------------------------------------

    @Test
    public void appendDateTimeLiteral_offsetTimestamp_usesTimestampLiteral() {
        StringBuilder sb = new StringBuilder();
        SqlAppender appender = fragment -> sb.append(fragment);
        OffsetDateTime odt = OffsetDateTime.of(2026, 8, 12, 13, 45, 56, 123_000_000, ZoneOffset.ofHours(9));

        dialect.appendDateTimeLiteral(appender, odt, TemporalType.TIMESTAMP, java.util.TimeZone.getTimeZone("UTC"));

        String lit = sb.toString().toLowerCase(Locale.ROOT);
        assertTrue("OFFSET 있는 TIMESTAMP는 timestamp '...' 리터럴이어야 함: " + sb, lit.startsWith("timestamp '"));
        assertTrue(lit.contains("2026"));
        assertFalse("date '...' 로 떨어지면 안 됨: " + sb, lit.startsWith("date '"));
    }

    @Test
    public void appendDateTimeLiteral_offsetTimestamp_executesOnTibero() {
        StringBuilder sb = new StringBuilder();
        SqlAppender appender = fragment -> sb.append(fragment);
        OffsetDateTime odt = OffsetDateTime.of(2026, 8, 12, 13, 45, 56, 0, ZoneOffset.ofHours(9));
        dialect.appendDateTimeLiteral(appender, odt, TemporalType.TIMESTAMP, java.util.TimeZone.getTimeZone("Asia/Seoul"));

        String literal = sb.toString();
        Object v = inTransactionReturning(session ->
                session.createNativeQuery("select " + literal + " from dual", Object.class).getSingleResult());
        assertNotNull("리터럴 실행 실패: " + literal, v);
    }

    // ------------------------------------------------------------------------
    // createOptionalTableUpdateOperation — SecondaryTable → MERGE
    // ------------------------------------------------------------------------

    @Test
    public void createOptionalTableUpdateOperation_secondaryTableUpdate_emitsMerge() {
        // 1) 주 테이블만 insert (secondary 행 없음)
        inTransaction(session -> {
            SecEntity e = new SecEntity();
            e.id = 1L;
            e.name = "main";
            session.persist(e);
        });

        // 2) secondary 컬럼을 나중에 채우면 optional table update → MERGE
        SqlCaptureInspector.clear();
        inTransaction(session -> {
            SecEntity e = session.get(SecEntity.class, 1L);
            e.detail = "filled";
        });

        List<String> sqls = SqlCaptureInspector.getSqls();
        boolean merge = sqls.stream()
                .map(s -> s.toLowerCase(Locale.ROOT))
                .anyMatch(s -> s.contains("merge"));
        assertTrue("SecondaryTable optional update는 MERGE를 써야 함: " + sqls, merge);

        String detail = inTransactionReturning(session -> session.get(SecEntity.class, 1L).detail);
        assertEquals("filled", detail);
    }

    // ------------------------------------------------------------------------
    // Entities
    // ------------------------------------------------------------------------

    @Entity(name = "SecEntity")
    @Table(name = "HOOK_SEC_MAIN")
    @SecondaryTable(name = "HOOK_SEC_DET", pkJoinColumns = @PrimaryKeyJoinColumn(name = "ID"))
    public static class SecEntity {
        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "NAME")
        public String name;

        @Column(name = "DETAIL", table = "HOOK_SEC_DET")
        public String detail;
    }

    @Entity(name = "RowIdEntity")
    @Table(name = "HOOK_ROWID")
    @RowId("rowid")
    public static class RowIdEntity {
        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "V")
        public String v;
    }

    @Entity(name = "UnderscoreEntity")
    @Table(name = "HOOK_USCORE")
    public static class UnderscoreEntity {
        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "_HIDDEN")
        public String hidden;
    }
}
