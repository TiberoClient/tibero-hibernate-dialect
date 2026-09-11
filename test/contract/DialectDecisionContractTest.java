package contract;

import com.tmax.tibero.hibernate.dialect.TiberoDialect;
import org.hibernate.dialect.Dialect;
import com.tmax.tibero.hibernate.dialect.aggregate.TiberoAggregateSupport;
import org.hibernate.tool.schema.internal.StandardUserDefinedTypeExporter;
import org.hibernate.type.SqlTypes;
import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.Assert.*;

/**
 * Dialect 판단 고정 계약 (DB 불필요).
 *
 * 목적:
 * - Oracle gap 중 미지원/기본값 적합/미구현 판단을 다시 하지 않도록 고정
 * - Hibernate/기본값 스펙이 바뀌면 실패 → docs/dialect-decisions.md 와 함께 보수
 *
 * @see docs/dialect-decisions.md
 */
public class DialectDecisionContractTest {

    private final TiberoDialect dialect = new TiberoDialect();

    private static final Set<String> TIBERO_DECLARED = Arrays.stream(TiberoDialect.class.getDeclaredMethods())
            .filter(m -> !m.isSynthetic())
            .map(Method::getName)
            .collect(Collectors.toSet());

    // -------------------------------------------------------------------------
    // §1.1 미지원 — enum domain API를 Tibero에서 오버라이드하지 않음
    //   (Oracle 쪽도 23c 전용 경로이므로 Tibero 7 대상에서는 해당 없음)
    // -------------------------------------------------------------------------

    @Test
    public void unsupported_enumDomainApis_areNotOverriddenOnTiberoDialect() {
        for (String name : new String[]{
                "getCreateEnumTypeCommand",
                "getDropEnumTypeCommand",
                "getEnumTypeDeclaration"
        }) {
            assertFalse(
                    name + " 은 미지원으로 Dialect 기본값을 쓴다. "
                            + "Oracle과 맞추려 오버라이드하지 말 것. 변경 시 docs/dialect-decisions.md 갱신.",
                    TIBERO_DECLARED.contains(name));
        }
    }

    // -------------------------------------------------------------------------
    // §1.2 미구현 — array ORM 경로
    //   DB는 Oracle식 array UDT(varying array / table of)를 지원한다(§2 실측).
    //   "DB 불가"가 아니라 아직 만들지 않은 것이므로, 구현 시 이 테스트를 함께 옮긴다.
    // -------------------------------------------------------------------------

    @Test
    public void array_apis_areOverridden() {
        // 네이티브 배열 지원으로 세 훅이 오버라이드되어 있어야 한다.
        for (String name : new String[]{
                "getArrayTypeName",
                "getPreferredSqlTypeCodeForArray",
                "getUserDefinedTypeExporter"
        }) {
            assertTrue(
                    name + " 은 VARRAY 컬럼 지원의 필수 훅이다. 지우면 배열이 VARBINARY 로 돌아간다.",
                    TIBERO_DECLARED.contains(name));
        }
    }

    @Test
    public void array_dialectContracts_matchImplementation() {
        // ANSI array DDL(`c number array`)은 Tibero 가 받지 않는다 — 이건 여전히 false 다.
        // 대신 Oracle 식 VARRAY UDT 로 지원하므로 아래 두 값이 바뀌었다.
        assertFalse("ANSI array DDL 은 Tibero 가 거부한다", dialect.supportsStandardArrays());
        assertEquals("VARBINARY 로 되돌아가면 DB 에서 배열로 다룰 수 없다",
                SqlTypes.ARRAY, dialect.getPreferredSqlTypeCodeForArray());
        assertEquals("String[] -> StringArray", "IntegerArray",
                dialect.getArrayTypeName("Integer", "number", null));
    }

    @Test
    public void unsupported_enumDomainDefaults_areEmpty() {
        assertNull(dialect.getEnumTypeDeclaration("color", new String[]{"R", "G"}));
        assertEquals(0, dialect.getCreateEnumTypeCommand("color", new String[]{"R", "G"}).length);
        assertEquals(0, dialect.getDropEnumTypeCommand("color").length);
    }

    // -------------------------------------------------------------------------
    // §1.2 미구현 — @Struct / UDT ORM (오버라이드하지 않은 채 기본 fail-fast 유지)
    // -------------------------------------------------------------------------

    @Test
    public void struct_hooks_areOverridden() {
        // @Struct 지원으로 두 훅은 이제 오버라이드되어 있어야 한다.
        for (String name : new String[]{
                "getAggregateSupport",
                "getCreateUserDefinedTypeKindString"
        }) {
            assertTrue(
                    name + " 은 @Struct 지원의 필수 훅이다. 지우면 SessionFactory 기동이 깨진다.",
                    TIBERO_DECLARED.contains(name));
        }
        // exporter 는 array 지원으로 함께 교체되었다 — array_apis_areOverridden 이 지킨다.
        // @Struct 만 쓰던 시점에는 표준 exporter 로 충분했다(object UDT DDL 은 그쪽이 낸다).
    }

    @Test
    public void struct_dialectContracts_matchImplementation() {
        assertTrue("기본 AggregateSupportImpl 이면 @Struct 기동이 실패한다",
                dialect.getAggregateSupport() instanceof TiberoAggregateSupport);
        assertEquals("create type T as object(...) 이어야 함. 빈 문자열이면 Tibero 가 문법 오류",
                "object", dialect.getCreateUserDefinedTypeKindString());
        // TiberoUserDefinedTypeExporter 는 StandardUserDefinedTypeExporter 의 하위이므로
        // object UDT DDL 경로는 그대로다. array UDT 처리만 얹혀 있다.
        assertTrue("object UDT DDL 경로는 표준 구현을 그대로 쓴다",
                dialect.getUserDefinedTypeExporter() instanceof StandardUserDefinedTypeExporter);
    }

    // -------------------------------------------------------------------------
    // §1.3 기본값 적합 — Oracle 제한/분기를 따르지 않음
    // -------------------------------------------------------------------------

    @Test
    public void defaultFit_apis_areNotOverriddenOnTiberoDialect() {
        for (String name : new String[]{
                "getInExpressionCountLimit",
                "useInputStreamToInsertBlob",
                "getMinimumSupportedVersion"
        }) {
            assertFalse(
                    name + " 은 기본값 적합. Oracle 값을 그대로 넣지 말 것. "
                            + "변경 시 docs/dialect-decisions.md 갱신.",
                    TIBERO_DECLARED.contains(name));
        }
    }

    @Test
    public void defaultFit_values() {
        assertEquals(
                "IN 원소 수 한도 없음(0) 유지. Tibero 는 단일 IN 리스트 50,000개까지 오류 없이 실행된다. "
                        + "대량 IN 이 느린 것은 사실이나(파스가 N², 30,000개 138초) 이 값을 1000 으로 바꿔도 "
                        + "Hibernate 6.6 은 한 문장 안에서 or 로 이어붙일 뿐이라 3배밖에 못 줄인다. "
                        + "실효가 있는 쪽은 getParameterCountLimit 이며 그쪽은 500 으로 오버라이드했다. "
                        + "docs/dialect-decisions.md §1.3 각주 참고.",
                0, dialect.getInExpressionCountLimit());
        assertEquals(
                "배치 로딩은 500키씩 나눈다 — getInExpressionCountLimit 과 달리 이쪽은 실제로 문장을 쪼갠다",
                500, dialect.getParameterCountLimit());
        assertTrue("blob stream 기본 true 유지",
                dialect.useInputStreamToInsertBlob());
        assertEquals(7, dialect.getVersion().getMajor());
    }

    // -------------------------------------------------------------------------
    // 가드: 문서에 적힌 Oracle-only 11개가 빠지지 않았는지
    // -------------------------------------------------------------------------

    @Test
    public void documentedOracleOnlyEleven_areCoveredByNotOverriddenGuards() {
        // 두 차례에 걸쳐 구현되어 이 목록에서 빠진 것들:
        //   @Struct  — getAggregateSupport · getCreateUserDefinedTypeKindString
        //   배열      — getArrayTypeName · getPreferredSqlTypeCodeForArray · getUserDefinedTypeExporter
        // 각각 struct_hooks_areOverridden / array_apis_areOverridden 이 대신 지킨다.
        Set<String> documented = Set.of(
                "getCreateEnumTypeCommand",
                "getDropEnumTypeCommand",
                "getEnumTypeDeclaration",
                "getInExpressionCountLimit",
                "getMinimumSupportedVersion",
                "useInputStreamToInsertBlob"
        );
        for (String name : documented) {
            assertFalse("문서의 Oracle-only API는 TiberoDialect에 아직 없어야 함: " + name,
                    TIBERO_DECLARED.contains(name));
        }
        // Dialect 기본 타입과의 정합 스모크
        assertTrue(dialect instanceof Dialect);
    }
}
