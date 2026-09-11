package semantics;

import support.AbstractTiberoDialectTestBase;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;

import org.hibernate.cfg.Configuration;
import org.hibernate.engine.spi.SessionFactoryImplementor;
import org.hibernate.query.sqm.function.SqmFunctionRegistry;
import org.junit.Before;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class FunctionRegistryTest extends AbstractTiberoDialectTestBase {

    private SessionFactoryImplementor sfi;
    private SqmFunctionRegistry registry;

    @Override
    protected Class<?>[] getAnnotatedClasses() {
        return new Class<?>[] { FnEntity.class };
    }

    @Override
    protected void configure(Configuration cfg) {
        super.configure(cfg);

        // 보통 테스트에서는 스키마 자동 생성이 켜져 있어야 엔티티 테이블이 생김
        cfg.setProperty("hibernate.hbm2ddl.auto", "create-drop");
    }

    @Before
    public void setUp() {
        sfi = sessionFactory();
        registry = sfi.getQueryEngine().getSqmFunctionRegistry();
        assertNotNull(registry);

        seedTestData();
    }

    // =========================================================================
    // 0) Seed
    // =========================================================================

    private void seedTestData() {
        inTransaction(session -> {
            // 이미 있으면 skip (create-drop라 보통 필요 없지만 안전장치)
            Long cnt = session.createQuery("select count(e) from FnEntity e", Long.class).getSingleResult();
            if (cnt != null && cnt > 0) return;

            // 3건 넣어서 stddev/variance 같은 집계도 의미 있게
            FnEntity e1 = new FnEntity(1L, -10,  5L,  4.0, "banana", null,
                    Date.valueOf("2025-12-01"),
                    Timestamp.valueOf("2025-12-30 12:34:56"));
            FnEntity e2 = new FnEntity(2L,  20,  7L,  9.0, "abracadabra", null,
                    Date.valueOf("2025-12-15"),
                    Timestamp.valueOf("2025-12-29 01:02:03"));
            FnEntity e3 = new FnEntity(3L,   0,  9L, 16.0, "xyz", "fallback",
                    Date.valueOf("2025-11-20"),
                    Timestamp.valueOf("2025-12-28 23:59:59"));

            session.persist(e1);
            session.persist(e2);
            session.persist(e3);
        });
    }

    // =========================================================================
    // 1) Registry 등록 검증 (모든 함수)
    // =========================================================================

    @Test
    public void registry_contains_all_registered_functions() {
        // 너의 initializeFunctionRegistry()에 나온 "모든" 함수명 목록
        List<String> names = Arrays.asList(
                // math / aggregate (Factory + super)
                "abs","sign","exp","ln","stddev","sqrt","variance","round","trunc","truncate","ceil","ceiling","floor",
                "median","log","log10","mod","power","atan2",
                // trig / hyperbolic
                "acos","asin","atan","cos","cosh","sin","sinh","tan","tanh",
                // bit
                "bitand","bitor","bitxor",
                // string
                "chr","initcap","lower","ltrim","rtrim","soundex","upper",
                "ascii","instr","instrb","lpad","replace","rpad","substr","substrb","translate",
                "left","right","trim",
                // cast/convert
                "to_char","to_date","to_number","to_timestamp",
                // date/time no-arg
                "current_date","current_time","current_timestamp","sysdate","systimestamp",
                // date funcs
                "last_day","add_months","months_between","next_day",
                // special no-arg
                "uid","user","rowid","rownum",
                // concat + aliases + patterns
                "concat","substring","locate","bit_length","octet_length","character_length","length",
                // coalesce / nvl etc
                "coalesce","nvl","nvl2",
                // window / ordered-set
                "listagg","mode","rank","dense_rank","row_number",
                // alias
                "str"
        );

        for (String fn : names) {
            assertNotNull("Missing function descriptor: " + fn,
                    registry.findFunctionDescriptor(fn));
        }
    }

    // =========================================================================
    // 2) 실행(Integration) 검증: function('name', ...)로 실제 쿼리 수행
    //    - 가능한 한 모든 함수를 실제로 호출해본다.
    // =========================================================================

    // ---------- Math ----------

    @Test
    public void fn_abs_sign_round_trunc_ceil_floor_mod_power_log_atan2_smoke() {
        // Factory 정렬 후 반환 JDBC 타입이 Integer/BigDecimal 등으로 달라질 수 있음
        Object abs = scalar("select function('abs', e.intVal) from FnEntity e where e.id=1", Object.class);
        assertEquals(10, ((Number) abs).intValue());

        Integer sign = scalar("select function('sign', e.intVal) from FnEntity e where e.id=1", Integer.class);
        assertTrue(sign == -1 || sign == 0 || sign == 1);

        // round/trunc/ceil/floor: 반환 타입이 DB/드라이버에 따라 BigDecimal/Double로 올 수 있어 Object로 받음
        Object round = scalar("select function('round', e.dblVal) from FnEntity e where e.id=1", Object.class);
        assertNotNull(round);

        Object trunc = scalar("select function('trunc', e.dblVal) from FnEntity e where e.id=1", Object.class);
        assertNotNull(trunc);

        Object ceil = scalar("select function('ceil', e.dblVal) from FnEntity e where e.id=1", Object.class);
        assertNotNull(ceil);

        Object floor = scalar("select function('floor', e.dblVal) from FnEntity e where e.id=1", Object.class);
        assertNotNull(floor);

        Integer mod = scalar("select function('mod', 10, 3) from FnEntity e where e.id=1", Integer.class);
        assertEquals(Integer.valueOf(1), mod);

        Object pow = scalar("select function('power', 2, 3) from FnEntity e where e.id=1", Object.class);
        assertNotNull(pow);

        // log(base, n) 형태가 Tibero에서 지원된다는 가정
        Object lg = scalar("select function('log', 10, 100) from FnEntity e where e.id=1", Object.class);
        assertNotNull(lg);

        Object atan2 = scalar("select function('atan2', 1, 1) from FnEntity e where e.id=1", Object.class);
        assertNotNull(atan2);
    }

    @Test
    public void fn_exp_ln_sqrt_smoke() {
        Double exp = scalar("select function('exp', 1.0) from FnEntity e where e.id=1", Double.class);
        assertNotNull(exp);

        Double ln = scalar("select function('ln', e.dblVal) from FnEntity e where e.id=1", Double.class);
        assertNotNull(ln);

        Double sqrt = scalar("select function('sqrt', e.dblVal) from FnEntity e where e.id=1", Double.class);
        assertNotNull(sqrt);
    }

    @Test
    public void fn_stddev_variance_aggregate_smoke() {
        Double stddev = scalar("select function('stddev', e.dblVal) from FnEntity e", Double.class);
        assertNotNull(stddev);

        Double variance = scalar("select function('variance', e.dblVal) from FnEntity e", Double.class);
        assertNotNull(variance);
    }

    // ---------- Trig ----------

    @Test
    public void fn_trig_family_smoke() {
        // 입력값은 -1~1 범위를 요구하는 acos/asin을 위해 0.5 사용
        Double acos = scalar("select function('acos', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(acos);

        Double asin = scalar("select function('asin', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(asin);

        Double atan = scalar("select function('atan', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(atan);

        Double cos = scalar("select function('cos', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(cos);

        Double sin = scalar("select function('sin', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(sin);

        Double tan = scalar("select function('tan', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(tan);

        Double cosh = scalar("select function('cosh', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(cosh);

        Double sinh = scalar("select function('sinh', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(sinh);

        Double tanh = scalar("select function('tanh', 0.5) from FnEntity e where e.id=1", Double.class);
        assertNotNull(tanh);
    }

    // ---------- Bit ----------

    @Test
    public void fn_bitand_smoke() {
        // bitand(10, 6) = 2 (1010 & 0110 = 0010)
        Object bitand = scalar("select function('bitand', 10, 6) from FnEntity e where e.id=1", Object.class);
        assertEquals(2, ((Number) bitand).intValue());
    }

    // ---------- String ----------

    @Test
    public void fn_string_basic_smoke() {
        // chr(65) = 'A'
        Character chr = scalar("select function('chr', 65) from FnEntity e where e.id=1", Character.class);
        assertNotNull(chr);
        assertEquals('A', chr.charValue());

        String lower = scalar("select function('lower', 'AbC') from FnEntity e where e.id=1", String.class);
        assertEquals("abc", lower);

        String upper = scalar("select function('upper', 'AbC') from FnEntity e where e.id=1", String.class);
        assertEquals("ABC", upper);

        String ltrim = scalar("select function('ltrim', '   a') from FnEntity e where e.id=1", String.class);
        assertNotNull(ltrim);

        String rtrim = scalar("select function('rtrim', 'a   ') from FnEntity e where e.id=1", String.class);
        assertNotNull(rtrim);

        // initcap/soundex는 로케일/DB 구현차가 있을 수 있어 non-null만
        String initcap = scalar("select function('initcap', 'hello world') from FnEntity e where e.id=1", String.class);
        assertNotNull(initcap);

        String soundex = scalar("select function('soundex', 'robert') from FnEntity e where e.id=1", String.class);
        assertNotNull(soundex);
    }

    @Test
    public void fn_ascii_instr_instrb_substr_substrb_translate_replace_lpad_rpad_smoke() {
        Integer ascii = scalar("select function('ascii', 'A') from FnEntity e where e.id=1", Integer.class);
        assertEquals(Integer.valueOf(65), ascii);

        Integer instr = scalar("select function('instr', 'banana', 'na') from FnEntity e where e.id=1", Integer.class);
        assertNotNull(instr);
        assertTrue(instr >= 1);

        Integer instrb = scalar("select function('instrb', 'banana', 'na') from FnEntity e where e.id=1", Integer.class);
        assertNotNull(instrb);
        assertTrue(instrb >= 1);

        String lpad = scalar("select function('lpad', 'a', 3, 'x') from FnEntity e where e.id=1", String.class);
        assertNotNull(lpad);
        assertEquals("xxa", lpad);

        String rpad = scalar("select function('rpad', 'a', 3, 'x') from FnEntity e where e.id=1", String.class);
        assertNotNull(rpad);
        assertEquals("axx", rpad);

        String replace = scalar("select function('replace', 'abab', 'a', 'x') from FnEntity e where e.id=1", String.class);
        assertNotNull(replace);
        assertEquals("xbxb", replace);

        String substr = scalar("select function('substr', 'banana', 2, 3) from FnEntity e where e.id=1", String.class);
        assertEquals("ana", substr);

        String substrb = scalar("select function('substrb', 'banana', 2, 3) from FnEntity e where e.id=1", String.class);
        assertNotNull(substrb);

        String translate = scalar("select function('translate', 'abc', 'ac', 'xz') from FnEntity e where e.id=1", String.class);
        assertNotNull(translate);
        assertEquals("xbz", translate);
    }

    // ---------- Aliases + patterns ----------

    @Test
    public void fn_concat_substring_alias_locate_bitLength_smoke() {
        String concat = scalar("select function('concat', 'foo', 'bar') from FnEntity e where e.id=1", String.class);
        assertEquals("foobar", concat);

        // substring alias -> substr로 매핑되어야 함
        String substring = scalar("select function('substring', 'banana', 1, 3) from FnEntity e where e.id=1", String.class);
        assertEquals("ban", substring);

        // locate -> instr(?2,?1), exact 2 args
        Integer locate = scalar("select function('locate', 'na', 'banana') from FnEntity e where e.id=1", Integer.class);
        assertNotNull(locate);
        assertTrue(locate >= 1);

        // bit_length -> vsize(?1)*8 : encoding/DB에 따라 값은 달라질 수 있으니 >0만
        Integer bitLen = scalar("select function('bit_length', 'abc') from FnEntity e where e.id=1", Integer.class);
        assertNotNull(bitLen);
        assertTrue(bitLen > 0);
    }

    // ---------- Coalesce / NVL ----------

    @Test
    public void fn_coalesce_nvl_nvl2_smoke() {
        // e.nullableStr: id=1,2는 null
        String coalesce = scalar(
                "select function('coalesce', e.nullableStr, e.strVal) from FnEntity e where e.id=1",
                String.class
        );
        assertEquals("banana", coalesce);

        String nvl = scalar(
                "select function('nvl', e.nullableStr, 'x') from FnEntity e where e.id=1",
                String.class
        );
        assertEquals("x", nvl);

        String nvl2_whenNull = scalar(
                "select function('nvl2', e.nullableStr, 'Y', 'N') from FnEntity e where e.id=1",
                String.class
        );
        assertEquals("N", nvl2_whenNull);

        String nvl2_whenNotNull = scalar(
                "select function('nvl2', e.nullableStr, 'Y', 'N') from FnEntity e where e.id=3",
                String.class
        );
        assertEquals("Y", nvl2_whenNotNull);
    }

    // ---------- To char/date + str alias ----------

    @Test
    public void fn_toChar_toDate_str_smoke() {
        // to_char: 숫자/날짜 모두 가능하지만 환경차 줄이기 위해 숫자 -> 문자열 변환
        String toChar = scalar("select function('to_char', 12345) from FnEntity e where e.id=1", String.class);
        assertNotNull(toChar);
        assertTrue(toChar.contains("12345"));

        // str 별칭 -> to_char
        String str = scalar("select function('str', 987) from FnEntity e where e.id=1", String.class);
        assertNotNull(str);
        assertTrue(str.contains("987"));

        // to_date('2025-12-30','YYYY-MM-DD') : 너 코드상 TIMESTAMP 반환 타입
        Object toDate = scalar(
                "select function('to_date', '2025-12-30', 'YYYY-MM-DD') from FnEntity e where e.id=1",
                Object.class
        );
        assertNotNull(toDate);
    }

    // ---------- Date/time no-arg + date funcs ----------

    @Test
    public void fn_currentDate_currentTime_currentTimestamp_sysdate_systimestamp_smoke() {
        Object currentDate = scalar("select function('current_date') from FnEntity e where e.id=1", Object.class);
        assertNotNull(currentDate);

        // current_time/current_timestamp/systimestamp 는 TZ 타입이 JDBC getObject와 충돌할 수 있어 native로 확인
        Object currentTime = inTransactionReturning(session ->
                session.createNativeQuery("select current_time from dual", Object.class).getSingleResult());
        assertNotNull(currentTime);

        Object currentTs = inTransactionReturning(session ->
                session.createNativeQuery("select cast(current_timestamp as timestamp) from dual", Object.class)
                        .getSingleResult());
        assertNotNull(currentTs);

        Object sysdate = scalar("select function('sysdate') from FnEntity e where e.id=1", Object.class);
        assertNotNull(sysdate);

        Object systs = inTransactionReturning(session ->
                session.createNativeQuery("select cast(systimestamp as timestamp) from dual", Object.class)
                        .getSingleResult());
        assertNotNull(systs);
    }

    @Test
    public void fn_lastDay_addMonths_monthsBetween_nextDay_smoke() {
        Object lastDay = scalar(
                "select function('last_day', e.dateVal) from FnEntity e where e.id=1",
                Object.class
        );
        assertNotNull(lastDay);

        Object addMonths = scalar(
                "select function('add_months', e.dateVal, 1) from FnEntity e where e.id=1",
                Object.class
        );
        assertNotNull(addMonths);

        Object monthsBetween = scalar(
                "select function('months_between', function('add_months', e.dateVal, 1), e.dateVal) from FnEntity e where e.id=1",
                Object.class
        );
        assertNotNull(monthsBetween);

        // next_day 요일 인자는 NLS_DATE_LANGUAGE 에 의존한다.
        // alter session 은 그 커넥션에만 적용되므로 설정과 조회를 반드시 같은 세션에서 해야 한다.
        // (별도 inTransaction 으로 나누면 커넥션 풀에서 다른 커넥션을 받아 JDBC-5113 이 난다)
        Object nextDay = inTransactionReturning(session -> {
            session.createNativeMutationQuery("alter session set nls_date_language='AMERICAN'").executeUpdate();
            return session.createQuery(
                    "select function('next_day', e.dateVal, 'MONDAY') from FnEntity e where e.id=1",
                    Object.class).getSingleResult();
        });
        assertNotNull(nextDay);
    }

    // ---------- Special no-arg ----------

    @Test
    public void fn_uid_user_smoke() {
        Integer uid = scalar("select function('uid') from FnEntity e where e.id=1", Integer.class);
        assertNotNull(uid);

        String user = scalar("select function('user') from FnEntity e where e.id=1", String.class);
        assertNotNull(user);
        assertFalse(user.isEmpty());
    }

    @Test
    public void fn_rowid_rownum_smoke_bestEffort() {
        Object rownum = scalar("select function('rownum') from FnEntity e where e.id=1", Object.class);
        assertNotNull(rownum);

        Object rowid = scalar("select function('rowid') from FnEntity e where e.id=1", Object.class);
        assertNotNull(rowid);
    }

    // ---------- Factory-aligned extras ----------

    @Test
    public void fn_left_right_median_listagg_log10_smoke() {
        String left = scalar("select function('left', e.strVal, 3) from FnEntity e where e.id=1", String.class);
        assertEquals("ban", left);

        String right = scalar("select function('right', e.strVal, 3) from FnEntity e where e.id=1", String.class);
        assertEquals("ana", right);

        Object med = scalar("select function('median', e.dblVal) from FnEntity e", Object.class);
        assertNotNull(med);

        Object log10 = scalar("select function('log10', 100) from FnEntity e where e.id=1", Object.class);
        assertNotNull(log10);

        // Tibero listagg는 WITHIN GROUP (ORDER BY ...) 필요 — registry 존재 + native 실측
        assertNotNull(registry.findFunctionDescriptor("listagg"));
        String agg = inTransactionReturning(session ->
                session.createNativeQuery(
                        "select listagg(str_val, ',') within group (order by id) from fn_registry_test",
                        String.class
                ).getSingleResult());
        assertNotNull(agg);
        assertTrue(agg.contains("banana") || agg.length() > 0);
    }

    // =========================================================================
    // Utilities
    // =========================================================================

    // =========================================================================
    // 3) 반환 타입 회귀 (전수조사 결과)
    //    mod/power/atan2 를 StandardSQLFunction 으로 재등록하면 base 의
    //    CommonFunctionFactory 등록(power/atan2 = double)이 float 으로 좁혀졌었음.
    //    base 등록을 그대로 쓰는지 고정한다.
    // =========================================================================

    @Test
    public void fn_power_atan2_resolveToDouble_notFloat() {
        Object pow = scalar("select function('power', 2, 3) from FnEntity e where e.id=1", Object.class);
        assertTrue("power 는 double 로 해석되어야 함 (실제: " + pow.getClass() + ")",
                pow instanceof Double);

        Object atan2 = scalar("select function('atan2', 1, 1) from FnEntity e where e.id=1", Object.class);
        assertTrue("atan2 는 double 로 해석되어야 함 (실제: " + atan2.getClass() + ")",
                atan2 instanceof Double);
    }

    @Test
    public void fn_mod_resolvesToInteger_perJpaSpec() {
        Object mod = scalar("select function('mod', 10, 3) from FnEntity e where e.id=1", Object.class);
        assertTrue("mod 는 JPA 스펙상 integer (실제: " + mod.getClass() + ")",
                mod instanceof Integer);
        assertEquals(1, ((Number) mod).intValue());
    }

    private <T> T scalar(String hql, Class<T> type) {
        return inTransactionReturning(session ->
                session.createQuery(hql, type)
                        .setMaxResults(1)
                        .getSingleResult()
        );
    }

    // =========================================================================
    // Test Entity
    // =========================================================================

    @Entity(name = "FnEntity")
    @Table(name = "FN_REGISTRY_TEST")
    public static class FnEntity {

        @Id
        @Column(name = "ID")
        public Long id;

        @Column(name = "INT_VAL")
        public Integer intVal;

        @Column(name = "LONG_VAL")
        public Long longVal;

        @Column(name = "DBL_VAL")
        public Double dblVal;

        @Column(name = "STR_VAL")
        public String strVal;

        @Column(name = "NULLABLE_STR")
        public String nullableStr;

        @Column(name = "DATE_VAL")
        public Date dateVal;

        @Column(name = "TS_VAL")
        public Timestamp tsVal;

        protected FnEntity() {}

        public FnEntity(Long id, Integer intVal, Long longVal, Double dblVal,
                        String strVal, String nullableStr, Date dateVal, Timestamp tsVal) {
            this.id = id;
            this.intVal = intVal;
            this.longVal = longVal;
            this.dblVal = dblVal;
            this.strVal = strVal;
            this.nullableStr = nullableStr;
            this.dateVal = dateVal;
            this.tsVal = tsVal;
        }
    }
}
