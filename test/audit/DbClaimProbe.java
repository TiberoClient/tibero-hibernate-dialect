package audit;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

/**
 * 전수조사 도구 (DB 실측): Dialect가 선언(또는 상속)한 값이 실제 Tibero에서 참인지 확인한다.
 *
 * 실행: java -cp <test runtime classpath> audit.DbClaimProbe
 *
 * 주의: 모든 statement에 queryTimeout(20s)을 건다. 바인드 파라미터 3만개 이상은
 * 서버 파싱이 20초를 넘겨 timeout 되므로 프로브 대상에서 제외했다.
 */
public class DbClaimProbe {

    static final String URL = "jdbc:tibero:thin:@localhost:8888:tibero";
    static Connection conn;
    static String lastError = "";

    public static void main(String[] args) throws Exception {
        conn = DriverManager.getConnection(URL, "tibero", "tmax");
        conn.setAutoCommit(true);

        var md = conn.getMetaData();
        System.out.println("product=" + md.getDatabaseProductName() + " " + md.getDatabaseProductVersion()
                + " / driver=" + md.getDriverVersion()
                + " / metadata.maxColumnNameLength=" + md.getMaxColumnNameLength());

        System.out.println();
        System.out.println("=== [A] 길이 상한 이분탐색 (getMaxVarcharLength / getMaxNVarcharLength / getMaxVarbinaryLength) ===");
        System.out.println("  varchar2(N)      max = " + bisect(n -> ddl("PRB_V", "varchar2(" + n + ")")));
        System.out.println("  varchar2(N char) max = " + bisect(n -> ddl("PRB_VC", "varchar2(" + n + " char)")));
        System.out.println("  nvarchar2(N)     max = " + bisect(n -> ddl("PRB_NV", "nvarchar2(" + n + ")")));
        System.out.println("  raw(N)           max = " + bisect(n -> ddl("PRB_R", "raw(" + n + ")")));
        System.out.println("  char(N)          max = " + bisect(n -> ddl("PRB_C", "char(" + n + ")")));
        System.out.println("  식별자 길이        max = " + bisect(DbClaimProbe::identOk, 1, 200)
                + "   (getMaxIdentifierLength)");

        System.out.println();
        System.out.println("=== [B] SQL 문법 주장 ===");
        setupTables();
        q("supportsExistsInSelect=false", "select exists(select 1 from dual) from dual");
        q("supportsDistinctFromPredicate=false", "select 1 from dual where 1 is distinct from 2");
        q("supportsTupleDistinctCounts=false", "select count(distinct (1,2)) from dual");
        q("supportsTupleCounts=false", "select count((1,2)) from dual");
        q("supportsValuesList=false : VALUES를 테이블 소스로", "select * from (values (1,2))");
        s("supportsValuesListForInsert=true : 다중행 INSERT",
                "insert into PRB_L1 (id) values (901),(902)");
        s("supportsFromClauseInUpdate : raw UPDATE..FROM (미지원, translator가 에뮬)",
                "update PRB_L1 t set t.id=1 from dual d where 1=1");
        s("UPDATE inline view 에뮬 (key-preserved join)",
                "update (select a.id ai, b.id bi from PRB_L1 a, PRB_L2 b where a.id=b.id) set ai=bi");
        q("supportsOrderByInSubquery", "select * from (select 1 as n from dual order by 1)");
        q("supportsUnionInSubquery", "select * from (select 1 as n from dual union select 2 from dual)");
        q("supportsNullPrecedence", "select 1 from dual order by 1 nulls first");
        q("supportsCaseInsensitiveLike=false (rows=0 이어야 함)", "select 1 from dual where 'A' like 'a'");

        System.out.println();
        System.out.println("=== [C] TiberoSqlAstTranslator 계약 ===");
        q("needsRecursiveKeywordInWithClause=false : WITH (키워드 없이)",
                "with t(n) as (select 1 from dual union all select n+1 from t where n<3) select max(n) from t");
        q("WITH RECURSIVE 키워드 사용 시 (실패해야 위 설정이 정당)",
                "with recursive t(n) as (select 1 from dual union all select n+1 from t where n<3) select max(n) from t");
        q("supportsRecursiveSearchClause=true",
                "with t(n) as (select 1 from dual union all select n+1 from t where n<3)"
                        + " search depth first by n set ord select max(n) from t");
        q("supportsRecursiveCycleClause=true",
                "with t(n) as (select 1 from dual union all select n+1 from t where n<3)"
                        + " cycle n set is_cycle to 'Y' default 'N' select max(n) from t");
        q("supportsWithClauseInSubquery=false (Tibero는 실제로 지원 — 보수적 선언)",
                "select * from (with t as (select 1 as n from dual) select * from t)");
        q("visitOver 보정 근거: 빈 OVER() 는 실패해야 함", "select row_number() over () from dual");
        q("visitOver 보정 결과: over(order by 1)", "select row_number() over (order by 1) from dual");
        // ON 절에 쓰인 컬럼은 갱신할 수 없으므로 비키 컬럼을 갱신한다
        s("MERGE (createOptionalTableUpdateOperation)",
                "merge into PRB_L1 t using (select 1 id, 'b' v from dual) s on (t.id=s.id)"
                        + " when matched then update set t.v=s.v"
                        + " when not matched then insert (id,v) values (s.id,s.v)");
        s("MERGE + update ... where (renderMergeUpdateClause)",
                "merge into PRB_L1 t using (select 1 id, 'c' v from dual) s on (t.id=s.id)"
                        + " when matched then update set t.v=s.v where s.v is not null"
                        + " when not matched then insert (id,v) values (s.id,s.v)");

        System.out.println();
        System.out.println("=== [D] 잠금 구문 (실테이블 기준 — DUAL은 잠글 수 없어 오탐) ===");
        s("for update", "select id from PRB_L1 for update");
        s("for update nowait", "select id from PRB_L1 for update nowait");
        s("for update skip locked", "select id from PRB_L1 for update skip locked");
        s("for update wait 3", "select id from PRB_L1 for update wait 3");
        s("for update of alias.col", "select a.id from PRB_L1 a for update of a.id");
        s("supportsOuterJoinForUpdate=true", "select a.id from PRB_L1 a left join PRB_L2 b on a.id=b.id for update");

        System.out.println();
        System.out.println("=== [E] IN 리스트 한도 (getInExpressionCountLimit=0) ===");
        for (int n : new int[]{1000, 10000, 65535, 100000}) inListProbe(n);
        System.out.println("  참고: 바인드 파라미터는 1000개 11ms / 10000개 약 16.7s / 32768개 이상 timeout");

        System.out.println();
        System.out.println("=== [F] DDL 타입 (columnType / registerColumnTypes) ===");
        for (String t : new String[]{"time", "time(6)", "timestamp(6) with time zone", "xmltype", "json",
                "geometry", "interval day to second", "number(19,0)", "binary_float", "binary_double"}) {
            System.out.printf("  %-58s %s%n", t, ddl("PRB_T", t) ? "OK" : "FAIL");
        }

        dropTables();
        conn.close();
    }

    interface Check { boolean ok(int n); }

    static int bisect(Check c) { return bisect(c, 1, 70000); }

    static int bisect(Check c, int lo, int hi) {
        if (!c.ok(lo)) return -1;
        int best = lo;
        while (lo <= hi) {
            int mid = lo + (hi - lo) / 2;
            if (c.ok(mid)) { best = mid; lo = mid + 1; }
            else hi = mid - 1;
        }
        return best;
    }

    static boolean ddl(String table, String type) {
        exec("drop table " + table);
        boolean ok = exec("create table " + table + " (c " + type + ")");
        exec("drop table " + table);
        return ok;
    }

    static boolean identOk(int n) {
        String name = "P" + "X".repeat(Math.max(0, n - 1));
        exec("drop table " + name);
        boolean ok = exec("create table " + name + " (c number)");
        exec("drop table " + name);
        return ok;
    }

    static void setupTables() {
        dropTables();
        exec("create table PRB_L1 (id number primary key, v varchar2(10))");
        exec("create table PRB_L2 (id number primary key)");
        exec("insert into PRB_L1 values (1)");
        exec("insert into PRB_L2 values (1)");
    }

    static void dropTables() {
        exec("drop table PRB_L1");
        exec("drop table PRB_L2");
    }

    static void inListProbe(int n) {
        StringBuilder sb = new StringBuilder("select 1 from dual where 1 in (1");
        for (int i = 1; i < n; i++) sb.append(',').append(i);
        sb.append(')');
        long t0 = System.currentTimeMillis();
        boolean ok = query(sb.toString()) != null;
        System.out.printf("  IN 리터럴 x%-8d %s (%d ms)%n", n, ok ? "OK" : "FAIL", System.currentTimeMillis() - t0);
    }

    static boolean exec(String sql) {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(20);
            st.execute(sql);
            return true;
        } catch (Exception e) {
            lastError = e.getMessage();
            return false;
        }
    }

    static String query(String sql) {
        try (Statement st = conn.createStatement()) {
            st.setQueryTimeout(20);
            try (ResultSet rs = st.executeQuery(sql)) {
                return rs.next() ? "rows>=1" : "rows=0";
            }
        } catch (Exception e) {
            lastError = e.getMessage();
            return null;
        }
    }

    static void s(String label, String sql) {
        boolean ok = exec(sql);
        System.out.printf("  %-58s %s%n", label, ok ? "OK" : "FAIL (" + brief(lastError) + ")");
    }

    static void q(String label, String sql) {
        String r = query(sql);
        System.out.printf("  %-58s %s%n", label, r != null ? "OK " + r : "FAIL (" + brief(lastError) + ")");
    }

    static String brief(String s) {
        if (s == null) return "?";
        s = s.replace("\n", " ");
        return s.length() > 66 ? s.substring(0, 66) : s;
    }
}
