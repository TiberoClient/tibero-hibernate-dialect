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
 * - getMaxAliasLength           (§6.5 — 선언값만 있고 실제 생성 확인이 없었음)
 * - canDisableConstraints       (§6.5 — truncate 경로 확인이 없었음)
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

    // ------------------------------------------------------------------
    // §6.5 — 선언값은 있었지만 DB 동작 확인이 없던 두 훅
    // ------------------------------------------------------------------

    /**
     * {@code getMaxAliasLength() = 118} 이 실제 Tibero 한계와 맞는지.
     *
     * <p>기존 {@code LimitCapabilityTest} 는 이 값을 <b>선언값으로만</b> 확인했다
     * (그 파일 주석에 "contract-only" 라고 적혀 있다). 선언이 실제보다 크면 Hibernate 가
     * DB 가 못 받는 길이의 별칭을 만들어 낸다 — 조인이 깊어질 때만 터져서 찾기 어렵다.
     *
     * <p>Tibero 의 식별자 한계는 128 인데 별칭은 118 로 잡아 두었다. Hibernate 가 별칭 뒤에
     * 접미사를 붙일 여유를 남기는 관례로, Oracle dialect 도 같은 방식이다.
     */
    @Test
    public void maxAliasLength_declaredValueIsAcceptedByTibero() {
        final int max = new TiberoDialect().getMaxAliasLength();
        assertEquals(118, max);

        final String alias = "A".repeat(max);
        final Long v = inTransactionReturning(session -> ((Number) session
                .createNativeQuery("select 1 as " + alias + " from dual", Object.class)
                .getSingleResult()).longValue());
        assertEquals("118자 별칭은 받아야 함", Long.valueOf(1L), v);
    }

    /** 선언값이 실제 한계보다 <b>작은</b> 쪽인지 — 크면 위험하고 작으면 안전하다. */
    @Test
    public void maxAliasLength_isNotLargerThanTheIdentifierLimit() {
        final TiberoDialect dialect = new TiberoDialect();
        assertTrue("별칭 한계가 식별자 한계를 넘으면 안 됨",
                dialect.getMaxAliasLength() <= dialect.getMaxIdentifierLength());

        final String tooLong = "A".repeat(dialect.getMaxIdentifierLength() + 1);
        try {
            inTransactionReturning(session -> session
                    .createNativeQuery("select 1 as " + tooLong + " from dual", Object.class)
                    .getSingleResult());
            fail("식별자 한계를 넘는 별칭은 거부돼야 함 — 선언값의 전제가 무너진다");
        }
        catch (Exception expected) {
            // Tibero 가 거부하는 것이 정상
        }
    }

    /**
     * {@code canDisableConstraints() = true} 의 <b>쓰임새</b>인 truncate 경로가 실제로 되는지.
     *
     * <p>Hibernate 가 이 훅을 보는 이유는 스키마 정리 때문이다. 외래 키가 걸린 테이블은
     * 그냥 {@code truncate} 할 수 없으므로 <b>제약을 끄고 → 비우고 → 다시 켜는</b> 순서를 쓴다.
     * 기존 {@code DmlCapabilityTest} 는 끄고 켜는 문장만 확인했고 <b>그 사이에 truncate 가
     * 되는지</b>는 보지 않았다.
     *
     * <p>제약이 켜져 있을 때는 막히고 꺼져 있을 때는 통과하는 것까지 확인해야, 이 훅이
     * 약속하는 동작이 성립한다.
     */
    @Test
    public void canDisableConstraints_enablesTruncateOfReferencedTable() {
        final TiberoDialect dialect = new TiberoDialect();
        assertTrue(dialect.canDisableConstraints());

        final String parent = "HOOK_FK_PARENT";
        final String child = "HOOK_FK_CHILD";
        final String fk = "HOOK_FK_REF";
        dropTableWithRetry(child);
        dropTableWithRetry(parent);
        inTransaction(session -> {
            session.createNativeMutationQuery(
                    "create table " + parent + " (id number(19,0) primary key)").executeUpdate();
            session.createNativeMutationQuery(
                    "create table " + child + " (id number(19,0) primary key, pid number(19,0), "
                            + "constraint " + fk + " foreign key (pid) references " + parent + "(id))")
                    .executeUpdate();
            session.createNativeMutationQuery("insert into " + parent + " values (1)").executeUpdate();
            session.createNativeMutationQuery("insert into " + child + " values (1,1)").executeUpdate();
        });
        try {
            // ① 제약이 켜져 있으면 부모를 비울 수 없다
            try {
                inTransaction(session -> session
                        .createNativeMutationQuery("truncate table " + parent).executeUpdate());
                fail("외래 키가 살아 있는데 truncate 가 통과했다 — 이 훅의 전제가 무너진다");
            }
            catch (Exception expected) {
                // 막히는 것이 정상
            }

            // ② 제약을 끄면 비울 수 있다
            inTransaction(session -> {
                session.createNativeMutationQuery(
                        dialect.getDisableConstraintStatement(child, fk)).executeUpdate();
                session.createNativeMutationQuery("truncate table " + child).executeUpdate();
                session.createNativeMutationQuery("truncate table " + parent).executeUpdate();
                session.createNativeMutationQuery(
                        dialect.getEnableConstraintStatement(child, fk)).executeUpdate();
            });

            assertEquals("부모가 비워져야 함", 0L, countRows(parent));
            assertEquals("자식도 비워져야 함", 0L, countRows(child));

            // ③ 다시 켠 뒤에는 제약이 살아 있어야 한다
            try {
                inTransaction(session -> session
                        .createNativeMutationQuery("insert into " + child + " values (9,999)")
                        .executeUpdate());
                fail("제약을 다시 켰는데 없는 부모를 참조하는 행이 들어갔다");
            }
            catch (Exception expected) {
                // 막히는 것이 정상
            }
        }
        finally {
            dropTableWithRetry(child);
            dropTableWithRetry(parent);
        }
    }

    private long countRows(String table) {
        return inTransactionReturning(session -> ((Number) session
                .createNativeQuery("select count(*) from " + table, Object.class)
                .getSingleResult()).longValue());
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
