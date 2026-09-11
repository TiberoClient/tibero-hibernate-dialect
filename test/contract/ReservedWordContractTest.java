package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.dialect.Dialect;
import org.hibernate.dialect.OracleDialect;
import org.junit.Test;

import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Tibero 고유 예약어 등록의 계약 (DB 불필요).
 *
 * <h2>무엇을 지키는 테스트인가</h2>
 * 엔티티 필드 이름이 DB 예약어와 겹치면 Hibernate 가 자동으로 큰따옴표를 씌운다. 그 판단은
 * <b>dialect 가 등록해 둔 예약어 목록</b>에만 의존한다. 목록에서 단어가 하나 빠지면
 * 그 이름을 쓰는 엔티티의 {@code create table} 이 {@code JDBC-7001} 로 통째로 깨진다.
 *
 * <pre>
 * 목록에 있으면   create table ITEM (..., "size" number(10,0), ...)   OK
 * 목록에 없으면   create table ITEM (..., size number(10,0), ...)     JDBC-7001
 * </pre>
 *
 * <p>여기서는 <b>목록 자체</b>를 고정한다. 실제 DDL 이 나가는지는
 * {@code capability.ReservedWordDdlTest} 가 본다.
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialect#registerDefaultKeywords()
 */
public class ReservedWordContractTest {

    private final Dialect tibero = new TiberoDialect();
    private final Dialect oracle = new OracleDialect();

    /**
     * 업무에서 가장 흔히 부딪히는 이름들.
     *
     * <p>목록 전체를 나열하는 대신 <b>대표 표본</b>을 고정한다. 전체 목록의 정합성은
     * {@code capability.ReservedWordDdlTest} 가 실제 DDL 로 확인하므로, 여기서는
     * "누가 목록을 통째로 지우거나 되돌렸는가"를 싸게 잡는 것이 목적이다.
     */
    @Test
    public void commonColumnNames_areRegistered() {
        for (String word : new String[]{"size", "number", "comment", "level", "mode",
                "option", "share", "session", "access", "view", "index", "lock"}) {
            assertTrue("업무에서 흔한 컬럼명이라 반드시 인용돼야 함: " + word,
                    tibero.getKeywords().contains(word));
        }
    }

    /**
     * 카탈로그의 {@code RESERVED} 플래그만 믿었으면 놓쳤을 단어들.
     *
     * <p>이 단어들은 {@code V$RESERVED_WORDS} 에서 {@code RESERVED='N'} 으로 표시돼 있는데도
     * 컬럼명으로 쓰면 실제로 DDL 이 실패한다(ps06 실측). 목록을 <b>추측이 아니라 실측으로</b>
     * 만든 이유가 이 13개다 — 카탈로그를 신뢰해 자동 생성하면 이들이 빠진다.
     */
    @Test
    public void wordsMissedByTheCatalogFlag_areRegistered() {
        for (String word : new String[]{"least", "flashback", "hbase", "externally",
                "rebalance", "btip", "connect_by_root", "connect_by_isleaf",
                "connect_by_iscycle", "binary_double_nan", "binary_double_infinity",
                "binary_float_nan", "binary_float_infinity"}) {
            assertTrue("RESERVED='N' 이지만 실측에서 DDL 이 깨진 단어: " + word,
                    tibero.getKeywords().contains(word));
        }
    }

    /**
     * Oracle 과 갈라지는 지점임을 명시한다.
     *
     * <p>{@code OracleDialect} 는 추가 키워드를 하나도 등록하지 않는다. 그래서 이 항목은
     * "Oracle 이 하는데 우리가 안 한 것" 목록에는 절대 잡히지 않았고, 벤더 12종을
     * 기준선으로 훑고 나서야 드러났다 — MySQL·SQLServer·DB2·HSQL·HANA·Sybase·Derby
     * 7개 벤더가 채우는 자리다.
     */
    @Test
    public void tiberoRegistersMoreThanOracle() {
        assertEquals("Oracle 은 ANSI 기본만 등록한다", 243, oracle.getKeywords().size());
        assertTrue("Tibero 는 고유 예약어를 더 등록해야 함: " + tibero.getKeywords().size(),
                tibero.getKeywords().size() > oracle.getKeywords().size());

        final Set<String> added = new TreeSet<>(tibero.getKeywords());
        added.removeAll(oracle.getKeywords());
        assertEquals("실측으로 확정한 71개", 71, added.size());
    }

    /** 기본 ANSI 목록을 지우지 않고 <b>더한다</b>. {@code super} 호출이 빠지면 여기서 걸린다. */
    @Test
    public void ansiKeywords_areStillThere() {
        for (String word : new String[]{"select", "from", "where", "table", "order", "group"}) {
            assertTrue("ANSI 기본 키워드가 사라지면 super.registerDefaultKeywords() 가 빠진 것: " + word,
                    tibero.getKeywords().contains(word));
        }
    }

    /**
     * 등록은 <b>소문자</b>로 보관된다.
     *
     * <p>{@code Dialect.registerKeyword} 가 {@code toLowerCase(Locale.ROOT)} 를 거쳐 담기 때문이다.
     * 그래서 {@code getKeywords()} 를 직접 뒤질 때는 소문자로 봐야 한다 — 이 집합 자체는
     * 평범한 {@code HashSet} 이라 대소문자를 가린다.
     *
     * <p>사용자가 {@code @Column(name = "SIZE")} 처럼 대문자로 적어도 인용되는 이유는
     * <b>실제 인용 판단이 이 집합을 그대로 쓰지 않기 때문</b>이다.
     * {@code IdentifierHelperBuilder} 가 목록을
     * {@code new TreeSet<>(String.CASE_INSENSITIVE_ORDER)} 에 옮겨 담고
     * {@code NormalizingIdentifierHelperImpl.isReservedWord} 가 거기서 찾는다.
     *
     * <p>대문자 이름이 실제로 인용되는지는 {@code capability.ReservedWordDdlTest} 가
     * 진짜 DDL 로 확인한다 — 여기서 흉내 내면 계층을 잘못 짚게 된다.
     */
    @Test
    public void keywordsAreStoredInLowerCase() {
        for (String word : new String[]{"size", "number", "least"}) {
            assertTrue("소문자로 보관돼야 함: " + word, tibero.getKeywords().contains(word));
            assertFalse("이 집합 자체는 대소문자를 가린다 — 대문자로는 안 잡혀야 정상: " + word,
                    tibero.getKeywords().contains(word.toUpperCase(Locale.ROOT)));
        }
    }

    /**
     * {@code DialectResolutionInfo} 생성자 경로에서도 등록돼야 한다.
     *
     * <p>{@code Dialect} 는 두 생성자 모두에서 {@code registerDefaultKeywords()} 를 부른다.
     * 등록을 생성자 본문에 직접 넣었다면 한쪽 경로에서 빠질 수 있어, 그 실수를 막는다.
     * 운영에서는 이쪽 경로가 쓰인다.
     */
    @Test
    public void bothConstructorPaths_registerTheSameKeywords() {
        final Dialect viaInfo = new TiberoDialect(new StubResolutionInfo());
        assertEquals("두 생성자가 같은 목록을 가져야 함",
                new TreeSet<>(lower(tibero.getKeywords())),
                new TreeSet<>(lower(viaInfo.getKeywords())));
    }

    private static Set<String> lower(Set<String> in) {
        final Set<String> out = new TreeSet<>();
        for (String s : in) {
            out.add(s.toLowerCase(Locale.ROOT));
        }
        return out;
    }


    /**
     * ⚠️ <b>등록만으로는 아무 일도 일어나지 않는다</b> — 설정을 켜야 쓰인다.
     *
     * <p>{@code hibernate.auto_quote_keyword} 의 기본값은 {@code false} 다. 끈 상태에서는
     * Hibernate 가 예약어 목록을 <b>보지도 않는다</b>({@code IdentifierHelperBuilder} 가
     * 목록 적재 자체를 건너뛴다).
     *
     * <p>이 테스트는 그 전제를 문서화한다 — DDL 두 벌을 나란히 뽑아 비교한다.
     * 누군가 "예약어를 등록했는데 왜 안 되지" 하고 헤매는 것을 막는 것이 목적이다.
     *
     * <pre>
     * 설정 끔(기본)   create table ... (size number(10,0), ...)     → Tibero 가 JDBC-7001
     * 설정 켬         create table ... ("size" number(10,0), ...)   → OK
     * </pre>
     *
     * <p>설정을 dialect 가 임의로 켜지는 않는다. 사용자 설정이고, 켜면
     * 컬럼명 대소문자가 굳는 부작용이 따라오기 때문이다
     * ({@code capability.ReservedWordDdlTest#quotingFreezesTheColumnNameCase}).
     */
    @Test
    public void registrationOnlyTakesEffectWhenAutoQuotingIsEnabled() throws Exception {
        final String off = ddl(false);
        final String on = ddl(true);

        assertTrue("기본값에서는 인용되지 않는다 — 이게 Oracle 과 같은 기본 동작: " + off,
                off.contains("size number") && !off.contains("\"size\""));
        assertTrue("설정을 켜면 인용된다: " + on, on.contains("\"size\""));
    }


    /**
     * 컬럼명뿐 아니라 <b>테이블명 · 시퀀스명</b>에도 적용되는지.
     *
     * <p>Hibernate 는 같은 예약어 목록을 식별자 종류와 무관하게 쓰므로 원리상 당연하지만,
     * 실제로 확인하지 않으면 "컬럼만 되고 테이블은 안 되는" 반쪽 상태를 모르고 지나칠 수 있다.
     * 컬럼만 보고 끝냈다가 뒤늦게 발견하는 종류의 빈틈이다.
     */
    @Test
    public void tableAndSequenceNames_areQuotedToo() throws Exception {
        final String sql = ddl(true, TableProbe.class);
        assertTrue("테이블명이 인용돼야 함: " + sql, sql.contains("create table \"session\""));
        assertTrue("시퀀스명이 인용돼야 함: " + sql, sql.contains("create sequence \"level\""));
    }

    @jakarta.persistence.Entity(name = "RwTableProbe")
    @jakarta.persistence.Table(name = "session")
    public static class TableProbe {
        @jakarta.persistence.Id
        @jakarta.persistence.GeneratedValue(
                strategy = jakarta.persistence.GenerationType.SEQUENCE, generator = "rwSeq")
        @jakarta.persistence.SequenceGenerator(
                name = "rwSeq", sequenceName = "level", allocationSize = 1)
        public Long id;
        public String v;
    }

    /** {@code hibernate.auto_quote_keyword} 만 다르게 하여 DDL 을 뽑는다. */
    private static String ddl(boolean autoQuote) throws Exception {
        return ddl(autoQuote, Probe.class);
    }

    private static String ddl(boolean autoQuote, Class<?> entity) throws Exception {
        final java.nio.file.Path out = java.nio.file.Files.createTempFile("rw", ".sql");
        try {
            final org.hibernate.boot.registry.StandardServiceRegistryBuilder b =
                    new org.hibernate.boot.registry.StandardServiceRegistryBuilder()
                            .applySetting("hibernate.dialect",
                                    "com.tmax.tibero.hibernate.dialect.TiberoDialect")
                            .applySetting("hibernate.boot.allow_jdbc_metadata_access", "false")
                            .applySetting("hibernate.connection.provider_class",
                                    support.RenderSupport.NoConnectionProvider.class.getName())
                            .applySetting("hibernate.hbm2ddl.auto", "none")
                            .applySetting("jakarta.persistence.schema-generation.scripts.action", "create")
                            .applySetting("jakarta.persistence.schema-generation.scripts.create-target",
                                    out.toString())
                            .applySetting("hibernate.hbm2ddl.delimiter", ";");
            if (autoQuote) {
                b.applySetting("hibernate.auto_quote_keyword", "true");
            }
            final org.hibernate.boot.MetadataSources ms =
                    new org.hibernate.boot.MetadataSources(b.build());
            ms.addAnnotatedClass(entity);
            ms.buildMetadata().buildSessionFactory().close();
            return new String(java.nio.file.Files.readAllBytes(out)).trim();
        }
        finally {
            java.nio.file.Files.deleteIfExists(out);
        }
    }

    @jakarta.persistence.Entity(name = "RwProbe")
    @jakarta.persistence.Table(name = "RW_CONTRACT_PROBE")
    public static class Probe {
        @jakarta.persistence.Id public Long id;
        @jakarta.persistence.Column(name = "size") public Integer size;
    }

    /** {@code TiberoDialect(DialectResolutionInfo)} 를 부르기 위한 최소 구현. */
    private static class StubResolutionInfo
            implements org.hibernate.engine.jdbc.dialect.spi.DialectResolutionInfo {
        @Override public String getDatabaseName() { return "Tibero"; }
        @Override public String getDatabaseVersion() { return "7"; }
        @Override public int getDatabaseMajorVersion() { return 7; }
        @Override public int getDatabaseMinorVersion() { return 0; }
        @Override public String getDriverName() { return "tbJDBC"; }
        @Override public int getDriverMajorVersion() { return 7; }
        @Override public int getDriverMinorVersion() { return 2; }
        @Override public String getSQLKeywords() { return ""; }
    }
}
