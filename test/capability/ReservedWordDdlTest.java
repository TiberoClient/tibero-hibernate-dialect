package capability;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import org.hibernate.cfg.Configuration;
import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.junit.Test;
import support.AbstractTiberoDialectTestBase;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

import static org.junit.Assert.*;

/**
 * Tibero 예약어를 컬럼명으로 쓰는 엔티티가 <b>실제로 만들어지고 동작하는지</b> 본다.
 *
 * <h2>어떤 문제였나</h2>
 * {@code hibernate.auto_quote_keyword=true} 를 켠 사용자가 {@code @Column(name = "size")}
 * 를 쓰면 스키마 생성이 통째로 깨졌다. 설정을 켰으니 인용될 줄 알았는데, Hibernate 가
 * {@code size} 를 Tibero 예약어로 알지 못해 그냥 내보냈고 {@code JDBC-7001} 이 났다.
 * 오류 메시지가 어느 컬럼 때문인지 가리키지 않아 원인을 찾기도 어려웠다.
 *
 * <p><b>설정을 안 켠 기본 상태에서는 인용 자체가 일어나지 않는다</b> — 그건 Oracle 도
 * 같고 Hibernate 가 문서화해 둔 기본값이다. 그 동작도 아래에서 함께 고정한다.
 *
 * <h2>여기서 확인하는 것</h2>
 * <ol>
 *   <li>예약어 이름을 쓰는 엔티티의 {@code create table} 이 성공하는지</li>
 *   <li>그 컬럼에 값을 넣고 읽고 조건으로 쓰는 것까지 되는지 — DDL 만 되고 DML 이
 *       깨지면 반쪽이다</li>
 *   <li><b>등록 목록이 빠짐없는지</b> — {@code V$RESERVED_WORDS} 전량을 다시 돌려
 *       새로 생긴 예약어가 있으면 알려 준다</li>
 * </ol>
 *
 * @see com.tmax.tibero.hibernate.dialect.TiberoDialect#registerDefaultKeywords()
 * @see contract.ReservedWordContractTest 목록 자체의 계약 (DB 불필요)
 */
public class ReservedWordDdlTest extends AbstractTiberoDialectTestBase {

    /**
     * 업무에서 가장 흔히 부딪히는 예약어들을 한 엔티티에 모았다.
     *
     * <p>대문자 {@code SIZE} 를 일부러 섞었다 — 인용 판단이 대소문자를 가리지 않는지
     * 이 경로로 확인한다.
     */
    @Entity(name = "RwItem") @Table(name = "RW_ITEM")
    public static class Item {
        @Id public Long id;
        @Column(name = "SIZE") public Integer size;
        /** 같은 예약어를 소문자로도 둔다 — 인용이 대소문자를 어떻게 굳히는지 보려고. */
        @Column(name = "size") public Integer sizeLower;
        @Column(name = "number") public Integer number;
        @Column(name = "comment") public String comment;
        @Column(name = "level") public Integer level;
        @Column(name = "mode") public String mode;
        @Column(name = "session") public String session;
        @Column(name = "uid") public String uid;
        /** {@code RESERVED='N'} 인데도 실패하던 것 — 카탈로그만 믿었으면 빠졌을 단어. */
        @Column(name = "least") public Integer least;
    }

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[]{Item.class};
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
        // 이 설정이 없으면 예약어 인용 자체가 일어나지 않는다 (기본값 false).
        // 등록한 목록은 이 설정이 켜졌을 때만 쓰인다 — 클래스 주석 참고
        cfg.setProperty("hibernate.auto_quote_keyword", "true");
        // DDL 오류를 삼키지 않게 한다 — 이 테스트의 핵심은 create table 이 성공하는 것이라
        // 조용히 실패하면 뒤의 단언이 엉뚱한 이유로 깨진다
        cfg.setProperty("hibernate.hbm2ddl.halt_on_error", "true");
    }

    // ------------------------------------------------------------------
    // DDL
    // ------------------------------------------------------------------

    /**
     * 테이블이 실제로 만들어졌는지.
     *
     * <p>예약어 등록이 빠지면 {@code create table} 이 `JDBC-7001` 로 실패하고
     * ({@code halt_on_error=true} 라 부팅 단계에서 터진다) 이 클래스 전체가 못 뜬다.
     */
    @Test
    public void tableIsCreated() {
        assertEquals("RW_ITEM 이 만들어져야 함", 1L,
                count("select count(*) from user_tables where table_name='RW_ITEM'"));
    }

    /**
     * 예약어 컬럼이 전부 자리 잡았는지 — 하나라도 빠지면 매핑이 조용히 달라진 것이다.
     *
     * <p>이름을 <b>적은 그대로</b> 찾는다. 인용된 식별자는 대소문자가 보존되기 때문이다
     * ({@link #quotingFreezesTheColumnNameCase} 참고).
     */
    @Test
    public void allReservedColumnsExist() {
        for (String col : new String[]{"SIZE", "number", "comment", "level",
                "mode", "session", "uid", "least"}) {
            assertEquals("예약어 컬럼이 있어야 함: " + col, 1L, count(
                    "select count(*) from user_tab_columns "
                            + "where table_name='RW_ITEM' and column_name='" + col + "'"));
        }
    }

    /**
     * ⚠️ 인용의 부작용 — <b>컬럼명 대소문자가 그대로 굳는다.</b>
     *
     * <p>Tibero 는 인용하지 않은 식별자를 대문자로 접어 저장하고, 인용한 식별자는
     * 적힌 그대로 저장한다. 그래서 {@code auto_quote_keyword} 를 켜면 예약어 컬럼만
     * <b>소문자 이름으로 만들어지고</b>, 그 뒤로는 손으로 쓰는 SQL 에서도 계속
     * 인용해야 한다.
     *
     * <pre>
     * &#64;Column(name = "size")   →  카탈로그 이름 "size"   select size   → JDBC-8008
     *                                                        select "size" → OK
     * &#64;Column(name = "SIZE")   →  카탈로그 이름 "SIZE"   select SIZE   → OK
     * </pre>
     *
     * <p><b>사용자에게 줄 조언</b> — 이 설정을 켤 거라면 예약어 컬럼 이름을
     * <b>대문자로</b> 적는 편이 낫다. 그러면 물리 컬럼명이 평소와 같은 대문자로 남아
     * 기존 SQL·리포팅 도구가 그대로 동작한다.
     *
     * <p>이 테스트는 그 동작을 <b>고정</b>하는 것이지 바람직하다고 주장하는 게 아니다.
     * Hibernate 와 Tibero 양쪽의 표준 동작이라 dialect 가 바꿀 수 있는 것이 아니다.
     */
    @Test
    public void quotingFreezesTheColumnNameCase() {
        assertEquals("소문자로 적었으면 소문자로 만들어진다", 1L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='RW_ITEM' and column_name='size'"));
        assertEquals("대문자로 적은 SIZE 는 대문자로 남는다", 1L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='RW_ITEM' and column_name='SIZE'"));
        assertEquals("인용 안 한 ID 는 평소대로 대문자", 1L, count(
                "select count(*) from user_tab_columns "
                        + "where table_name='RW_ITEM' and column_name='ID'"));
    }

    // ------------------------------------------------------------------
    // DML — DDL 만 되고 값이 안 돌면 반쪽이다
    // ------------------------------------------------------------------

    /** 예약어 컬럼에 값을 넣고 읽는다. INSERT · SELECT 양쪽 렌더에서 인용이 유지돼야 한다. */
    @Test
    public void valuesRoundTrip() {
        inTransaction(s -> {
            Item i = new Item();
            i.id = 1L; i.size = 10; i.number = 20; i.comment = "c";
            i.level = 3; i.mode = "m"; i.session = "s"; i.uid = "u"; i.least = 1;
            s.persist(i);
        });
        inTransaction(s -> {
            Item i = s.find(Item.class, 1L);
            assertEquals(Integer.valueOf(10), i.size);
            assertEquals(Integer.valueOf(20), i.number);
            assertEquals("c", i.comment);
            assertEquals(Integer.valueOf(3), i.level);
            assertEquals("m", i.mode);
            assertEquals("s", i.session);
            assertEquals("u", i.uid);
            assertEquals(Integer.valueOf(1), i.least);
        });
    }

    /** 예약어 컬럼을 WHERE 조건과 ORDER BY 에 쓴다. */
    @Test
    public void reservedColumnsWorkInPredicates() {
        inTransaction(s -> {
            Item i = new Item();
            i.id = 2L; i.size = 99; i.level = 7; i.least = 5;
            s.persist(i);
        });
        inTransaction(s -> {
            List<Long> ids = s.createQuery(
                    "select i.id from RwItem i where i.size = :v order by i.level", Long.class)
                    .setParameter("v", 99).getResultList();
            assertEquals("예약어 컬럼이 조건·정렬에서도 동작해야 함", List.of(2L), ids);
        });
    }

    /** 예약어 컬럼을 UPDATE 의 SET 대상으로 쓴다 — 렌더 경로가 또 다르다. */
    @Test
    public void reservedColumnsWorkInUpdate() {
        inTransaction(s -> {
            Item i = new Item();
            i.id = 3L; i.size = 1;
            s.persist(i);
        });
        inTransaction(s -> assertEquals(1, s.createMutationQuery(
                "update RwItem i set i.size = 42 where i.id = 3").executeUpdate()));
        inTransaction(s -> assertEquals(Integer.valueOf(42), s.find(Item.class, 3L).size));
    }

    // ------------------------------------------------------------------
    // 목록 자체의 완전성
    // ------------------------------------------------------------------

    /**
     * 등록 목록에 <b>빠진 예약어가 없는지</b> 전수 확인한다.
     *
     * <p>{@code V$RESERVED_WORDS} 전량을 실제로 컬럼명으로 써 보고, 실패하는데
     * Hibernate 가 모르는 단어가 있으면 실패한다. 목록을 손으로 관리하는 이상
     * <b>서버 버전이 올라가 예약어가 늘면 조용히 뒤처지는데</b>, 그걸 잡는 장치다.
     *
     * <p>실패하면 메시지에 나온 단어를 {@code TiberoDialect.KEYWORDS} 에 추가하면 된다.
     *
     * <h2>왜 한 번에 묶어 던지나</h2>
     * 후보가 1,000개가 넘어서 단어마다 {@code create table} 을 돌리면 13초가 넘게 걸렸다
     * — 전체 스위트의 절반이다. 그래서 <b>한 덩어리씩 모아 컬럼을 한꺼번에</b> 만든다.
     * 덩어리가 통과하면 그 안의 단어는 전부 문제없는 것이므로 한 번으로 끝난다.
     *
     * <p>평소에는 빠진 단어가 없으니 덩어리마다 한 문장씩만 나가고, 어쩌다 실패한
     * 덩어리가 있을 때만 그 안을 단어별로 다시 훑어 범인을 찾는다. 실패는 드물고
     * 그때는 정확한 이름이 필요하므로 이 비대칭이 맞다.
     */
    @Test
    public void noReservedWordIsMissingFromTheRegisteredList() {
        final Set<String> known = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        known.addAll(new TiberoDialect().getKeywords());

        final List<String> candidates = new ArrayList<>();
        inTransaction(s -> s.doWork(conn -> {
            try (Statement st = conn.createStatement();
                 ResultSet r = st.executeQuery("select keyword from V$RESERVED_WORDS")) {
                while (r.next()) {
                    final String w = r.getString(1).toUpperCase(Locale.ROOT);
                    // 식별자 형태이면서 아직 모르는 단어만 후보
                    if (w.matches("[A-Z][A-Z0-9_]*") && w.length() <= 30 && !known.contains(w)) {
                        candidates.add(w);
                    }
                }
            }
        }));
        assertFalse("후보를 하나도 못 뽑았다면 V$RESERVED_WORDS 조회가 잘못된 것", candidates.isEmpty());

        final List<String> missing = new ArrayList<>();
        inTransaction(s -> s.doWork(conn -> {
            for (int from = 0; from < candidates.size(); from += CHUNK) {
                final List<String> chunk =
                        candidates.subList(from, Math.min(from + CHUNK, candidates.size()));
                if (!createTableWith(conn, chunk)) {
                    // 이 덩어리 안에 범인이 있다 — 여기서만 단어별로 좁힌다
                    for (String w : chunk) {
                        if (!createTableWith(conn, List.of(w))) {
                            missing.add(w);
                        }
                    }
                }
            }
            dropProbe(conn);
        }));

        assertTrue("컬럼명으로 못 쓰는데 TiberoDialect.KEYWORDS 에 없는 단어가 있음 — "
                        + "서버 버전이 올라 예약어가 늘었을 수 있다. 목록에 추가할 것: " + missing,
                missing.isEmpty());
    }

    /**
     * 한 번에 묶는 컬럼 수.
     *
     * <p>Tibero 의 테이블당 컬럼 상한(1,000)보다 넉넉히 작게 잡았다. 너무 크게 잡으면
     * 상한에 걸려 <b>예약어 때문이 아닌 이유로</b> 실패하고, 그러면 멀쩡한 단어가
     * 범인으로 지목된다.
     */
    private static final int CHUNK = 50;

    /** 주어진 단어들을 컬럼명으로 하는 테이블을 만들어 본다. 성공하면 {@code true}. */
    private static boolean createTableWith(java.sql.Connection conn, List<String> words) {
        dropProbe(conn);
        final StringBuilder ddl = new StringBuilder("create table RW_PROBE (");
        for (int i = 0; i < words.size(); i++) {
            if (i > 0) {
                ddl.append(", ");
            }
            ddl.append(words.get(i)).append(" number");
        }
        ddl.append(')');
        try (Statement st = conn.createStatement()) {
            st.execute(ddl.toString());
            return true;
        }
        catch (Exception rejected) {
            return false;
        }
    }

    private static void dropProbe(java.sql.Connection conn) {
        try (Statement st = conn.createStatement()) {
            st.execute("drop table RW_PROBE");
        }
        catch (Exception ignored) {
            // 아직 없으면 무시 — 첫 회차에는 항상 없다
        }
    }

    private long count(String sql) {
        return inTransactionReturning(s ->
                ((Number) s.createNativeQuery(sql, Object.class).getSingleResult()).longValue());
    }
}
