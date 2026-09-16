# Dialect 판단 기록 (Hibernate 6.6 / Tibero 7.2.6 이상)

> ⚠️ **이 문서는 dialect 유지보수자용이며 배포 대상이 아닙니다.**
> 사용자가 알아야 할 내용은 [../README.md](../README.md) 와
> [../docs/user-notes.md](../docs/user-notes.md) 에 있습니다. 여기 적힌 "미지원 / 미구현" 분류는 구현 판단의 기록이지 제품 기능 목록이
> 아닙니다. `DialectDecisionContractTest` · `DialectDecisionCapabilityTest` 가 이 문서의
> 분류를 테스트로 고정하고 있으므로, 분류를 바꿀 때는 테스트도 함께 고쳐야 합니다.

이 문서는 **나중에 같은 판단을 반복하지 않기 위해** 남긴다.  
Oracle Dialect와의 diff 만 보고 “빠진 오버라이드”를 다시 추가하기 전에 **반드시 이 문서를 먼저** 본다.

관련 테스트:

- `DialectDecisionContractTest` — Dialect 선언값·미오버라이드 고정 (DB 불필요)
- `DialectDecisionCapabilityTest` — Tibero DB 실측으로 미지원/검증 완료 사실 고정

스펙(DB·Hibernate·드라이버) 이 바뀌어 테스트가 실패하면, 구현을 고치기 전에 **이 문서의 분류·재검토 조건을 갱신**한다.

용어:

| 용어 | 의미 |
|------|------|
| **미지원** | Tibero DB 가 해당 SQL/기능을 제공하지 않음 |
| **미구현** | DB·JDBC 는 가능하나 Tibero Dialect/ORM 경로를 아직 만들지 않음 |
| **기본값 적합** | Hibernate `Dialect` 기본값이 Tibero 에 맞아 오버라이드 불필요 |

기준: Hibernate 6.6.56.Final / Tibero 7.2.6 (FS02_PS06) / dialect 6.6.1
최종 갱신: 2026-09-16

---

## 1. Oracle 만 오버라이드한 API — 판단 결과

리플렉션 실측 기준(`Dialect` 의 재정의 가능 시그니처 327개, `static`·`final`·`private` 제외):
Oracle 이 재정의한 115개 중 Tibero 가 같이 재정의하지 **않는 것이 5개**다.
아래 표에 취소선이 그어진 항목은 그 뒤 구현돼 이 5개에서 빠졌다.

| | 6.6.0 | 현재 |
|---|---|---|
| Tibero 재정의 총수 | 61 | **114** |
| Oracle 과 겹치는 것 | 60 | **110** |
| 미대응 격차 | 55 | **5** |
| Tibero 만 재정의 | 1 | **4** |

### 1.1 미지원 (DB 불가) — Dialect 에 Oracle식 구현을 넣지 않음

| API | 근거 | 재검토 조건 |
|-----|------|-------------|
| `getCreateEnumTypeCommand` | `create domain … as enum` 문법 오류. Hibernate 쪽도 `OracleDialect`가 **Oracle 23c 이상에서만** 이 경로를 씀(`getEnumTypeDeclaration`이 `isSameOrAfter(23)` 분기) | DB 가 enum domain 을 지원하면 |
| `getDropEnumTypeCommand` | 위와 동일 | 위와 동일 |
| `getEnumTypeDeclaration` | domain enum 경로 없음. `OracleEnumJdbcType`/`OracleOrdinalEnumJdbcType`도 23c 전용 등록이라 Tibero 7.2.6 대상에서는 해당 없음 | 위와 동일 |

참고: JPA `@Enumerated(STRING/ORDINAL)` 는 **지원됨**. 위는 DB enum **타입/domain** 이야기다.

> **2026-09-02 재분류** — `getArrayTypeName` / `getPreferredSqlTypeCodeForArray` 는 이 절에 있었으나 §1.2(미구현) 로 옮겼다.
> 근거였던 `number array` / `int array[n]` 은 **ANSI 표준 array 문법**인데, Oracle Dialect 는 그 문법을 쓰지 않는다.
> Oracle 은 `getArrayTypeName()`이 UDT 이름을 돌려주고 `OracleUserDefinedTypeExporter`가 `create or replace type … as varying array(n) of …` / `as table of …` 를 낸다.
> 이 Oracle식 DDL 은 **Tibero 에서 정상 동작한다**(§2 참고). 따라서 "DB 불가"가 아니라 "ORM 경로 미구현"이다.

### 1.2 미구현 (DB·JDBC 는 됨) — 의도적 보류, “빠진 것”이 아님의 의미가 다름

| API | 근거 | 재검토 조건 |
|-----|------|-------------|
| ~~`getAggregateSupport`~~ | ✅ **구현됨** — `TiberoAggregateSupport`. STRUCT 계열(`@Struct`·`@Struct` 배열) 과 **JSON 집계**(`@JdbcTypeCode(SqlTypes.JSON)` 임베더블) 를 모두 다룬다 | XML 집계를 지원할 때 확장 |
| ~~`getCreateUserDefinedTypeKindString`~~ | ✅ **구현됨 (2026-09-07)** — `"object"`. 기본값 `""` 이면 `create type T as (...)` 가 나가 Tibero 가 거부 | — |
| ~~`getUserDefinedTypeExporter`~~ | ✅ **구현됨 (2026-09-07)** — `TiberoUserDefinedTypeExporter`. 기본 구현은 array UDT 에서 예외를 던진다. **Oracle 처럼 PL/SQL 헬퍼는 만들지 않는다** — 아래 참고 | — |
| ~~`getArrayTypeName`~~ | ✅ **구현됨 (2026-09-07)** — `String[]` → `StringArray` (Oracle 과 같은 규칙) | — |
| ~~`getPreferredSqlTypeCodeForArray`~~ | ✅ **구현됨 (2026-09-07)** — `VARBINARY` → `ARRAY`. **기존 배열 컬럼의 DDL 타입이 바뀌는 변경**이다 | — |

> **2026-09-07 — `@Struct` 는 구현되었다.** 위 표에서 취소선이 그어진 두 항목은 §1.2(미구현) 에서 빠졌다.
> 이전 판단은 "Spring 앱은 대부분 `@Embedded`로 충분하고 사용 빈도가 낮으니 미구현 유지"였는데,
> 그 판단의 근거 중 **구현 비용 추정이 크게 틀렸다** — Oracle 파일 크기(합계 1,210 LOC) 를 그대로
> 옮겨 적었으나 그 대부분은 JSON 집계와 array UDT 용이었고, STRUCT 에 실제로 필요한 것은
> 약 300 LOC 였다. 수요 판단은 그대로 유효하나 비용 근거는 무효다.
>
> `@Embedded` 는 **완전 대체가 아니다** — 필드가 컬럼으로 펼쳐지므로, 기존 스키마에 object 컬럼이
> 있거나 PL/SQL 이 그 타입을 받는 경우에는 쓸 수 없다. 자세한 내용은
> [구현 문서](https://outline.tibero.com/doc/tibero-hibernate-dialect-661-Bd2JF3zpdx) §5.2.1 참고.

> **2026-09-07 — 네이티브 배열 컬럼도 구현되었다.** `String[]` · `Integer[]` 같은 배열 필드가
> `VARBINARY` 이진 덩어리가 아니라 **VARRAY 컬럼**으로 매핑된다. `table()` 언네스트가 되고
> 카탈로그에도 배열로 보인다.
>
> ⚠️ **HQL `array_*` 함수는 의도적으로 등록하지 않았다.** Hibernate 의 Oracle 변종은 배열
> 타입마다 PL/SQL 헬퍼 18개를 만들어 호출하는데, 그 헬퍼를 Tibero 에 적용하자
> **DB 가 반복적으로 응답 불능**에 빠졌다 — 특히 `<타입>_concat` 은 **두 번째 호출부터
> 서버 워커가 멈추고 인스턴스 재기동으로만 풀렸다**. 애플리케이션에 노출할 수 없는 고장이라
> 헬퍼 생성과 함수 등록을 모두 뺐다. 자세한 내용은
> [구현 문서 §5.2 P3 하위 문서](https://outline.tibero.com/doc/tibero-hibernate-dialect-661-Bd2JF3zpdx) 참고.
>
> nested table(`as table of`)·`@Struct` 배열(STRUCT_ARRAY) 은 여전히 미구현이다.

### 1.3 기본값 적합 — Oracle 값을 따르지 않음

| API | Tibero 에서 쓰는 값 | 근거 | 재검토 조건 |
|-----|-------------------|------|-------------|
| `getInExpressionCountLimit` | `0` (무제한) | Tibero 에는 IN 원소 개수 한도가 **없다** — 바인드 50,000개 단일 IN 리스트까지 오류 없이 실행됨(ps06 실측). Oracle 의 1000/65535 는 `ORA-01795` 회피값이라 해당 없음. Hibernate 는 이 API 를 "성능 손잡이"가 아니라 "DB 가 거부하는 지점"으로 쓴다 (PostgreSQL·MySQL·H2 도 `0`) | ① Tibero 가 IN 원소 수에 **하드 한도**를 두면 ② Hibernate 가 이 값을 **문장 분할**에 쓰도록 바뀌면 — 6.6 은 한 문장 안에서 `or` 로 이어붙일 뿐이다. **대량 IN 이 느린 것은 이 API 로 고칠 문제가 아니다** (아래 각주) |
| `getParameterCountLimit` | **`500`** (기본값은 위 값과 연동되어 `0`) | 배치 로딩 파스 비용. `byMultipleIds`·`@BatchSize` 가 **실제로 문장을 나누는** 유일한 손잡이다. 3,000키 기준 1,411ms → 171ms (아래 각주) | 네트워크 지연이 큰 환경에서 배치 로딩이 느리면 값을 올려 재측정. Tibero 파스 성능이 개선되면 다시 `0` 검토 |
| `useInputStreamToInsertBlob` | `true` | Oracle Application Continuity 이슈용 분기. Tibero 에 해당 없음 | blob stream 바인딩 장애가 재현되면 |

> **실측 — `0` 은 "한도 없음"이지 "싸다"가 아니다.**
>
> ps06(Tibero 7.2.6) 에서 `where id in (?,…)` 의 바인드 수를 늘려가며 쟀다. **실패는 한 건도
> 없었다.** 대신 서버 파스 시간이 원소 수의 **제곱**으로 늘었다 — `t(ms) ≈ 1.55e-4 × N²`,
> 실측 4개 점에 오차 2.4% 이내.
>
> | 바인드 수 | 최초 실행(파스 포함) | 같은 문장 재실행 |
> |---|---|---|
> | 1,000 | 0.16 초 | 0.8 ms |
> | 10,000 | 15.6 초 | 5.4 ms |
> | 30,000 | **138.6 초** | 15.3 ms |
> | 50,000 | **387.6 초** | 20.2 ms |
>
> * **비용은 전부 서버 파스다.** 바인드 값을 N개 전부 다시 채워 재실행해도 2.7ms(N=5,000)이고,
>   `?` 를 리터럴로 바꿔도 같은 시간이 나온다 — 값 전송·바인딩 비용이 아니다.
> * **파스는 SQL 텍스트당 1회.** 같은 텍스트를 **새 커넥션**에서 실행해도 5.5ms — 서버측 공용 캐시.
> * **문제는 텍스트가 매번 바뀐다는 것.** Hibernate 는 리스트 길이만큼 `?` 를 찍으므로
>   5,000 → 5,001 → 4,999 처럼 **길이가 하나만 달라도 새 텍스트**가 되어 3.8초를 다시 문다.
>
> **`getInExpressionCountLimit` 을 1000 으로 바꾸는 것은 해법이 아니다.**
> `AbstractSqlAstTranslator#visitInListPredicate` 는 이 값을 문장 분할이 아니라 **한 문장 안에서
> `x in (…) or x in (…)` 로 이어 붙이는 데** 쓴다. 바인드 총수가 그대로라 개선폭이 10,000개 기준
> 15.4초 → 4.6초(3.3배) 에 그친다. 게다가 native query 경로는 분할조차 하지 않고
> `HHH000443`("will likely cause failures") 경고만 찍는데 **Tibero 는 실패하지 않으므로 오탐**이다.
>
> **실효가 있는 것은 `getParameterCountLimit` 이다.** `STANDARD_MULTI_KEY_LOAD_SIZING_STRATEGY`
> 가 이 값으로 `byMultipleIds`·`@BatchSize`·컬렉션 배치 페치를 **실제로 여러 문장으로 쪼갠다.**
> 청크 크기별 실측(`byMultipleIds`):
>
> | chunk | 10,000키 | 30,000키 |
> |---|---|---|
> | 100 | 76 ms | 101 ms |
> | **250** | **35 ms** | **65 ms** |
> | **500** ← 채택 | 57 ms | 78 ms |
> | 1000 | 167 ms | 178 ms |
> | 0 (미적용) | 15,490 ms | — |
>
> 최솟값은 250 이지만 **500** 을 골랐다. 이 측정은 **localhost** 라 왕복 비용이 사실상 0 이어서
> 작은 청크에 유리하게 치우쳐 있다. 네트워크 지연이 있는 실제 배포에서는 왕복이 늘수록 손해가
> 커지므로 왕복이 절반인 쪽이 안전하다.
>
> **남는 한계** — 이 값은 배치 로딩 경로에만 적용된다. **HQL 의 `in :list` 는 여전히 한 문장**이라
> 큰 리스트를 넘기면 파스 비용을 그대로 문다. 그쪽은 애플리케이션이 끊어 보내거나
> `hibernate.query.in_clause_parameter_padding=true`(기본 꺼짐) 로 텍스트 종류를 줄여야 한다.
> 미측정으로 남은 것: 큰 테이블에서 `or` 체인의 실행계획 품질.

---

## 2. Tibero 기본값·경로 검증 완료 (Oracle diff 와 무관해도 재프로브 금지)

아래는 **실제로 프로브/테스트로 확인한 항목**만 적는다. 목록에 없으면 미검증이다.

| 항목 | 기대 | 검증 방법(요약) |
|------|------|-----------------|
| `supportsStandardArrays()` | `false` | 계약 테스트 + array DDL 거부 |
| **ANSI** SQL array DDL | 거부 | `create table … (c number array)` / `(c int array[10])` 실패 |
| **Oracle식** array UDT DDL | **수용** | `create or replace type T as varying array(10) of number` / `as varray(10) of number` / `as table of number` 전부 성공 (ps06 9999) |
| Oracle식 array UDT 를 컬럼 타입으로 | **수용** | `create table … (v T)` 성공 + VARRAY 생성자 리터럴 insert 성공 |
| VARRAY 위 PL/SQL 함수 | **수용** | Oracle 이 생성하는 `T_cmp(a in T, b in T)` 형태 함수 컴파일 성공 |
| `TABLE()` 언네스트 | **수용** | `select column_value from t, table(t.v)` 성공 |
| JDBC array 왕복 | **수용** | `Connection.createArrayOf` → `com.tmax.tibero.jdbc.TbArray`, `setArray`/`getArray` 왕복 성공 |
| `user_coll_types` 등록 | **수용** | `coll_type = VARYING ARRAY`, `upper_bound = 10` 조회됨 |
| basic `int[]` 매핑 | VARBINARY 경로 왕복 OK | 엔티티 persist/load |
| enum domain DDL | 거부 | `create domain … enum` 실패 |
| `@Enumerated` STRING/ORDINAL | 왕복 OK | 엔티티 persist/load |
| `@Struct` + 현재 dialect | **정상 동작** (2026-09-07~) | `capability.StructMappingTest` 16건 — DDL·왕복·질의·갱신·중첩·`@Version` |
| object UDT DDL | DB 는 수용 | `create type … as object` |
| JDBC `Struct` | createStruct/insert/select OK | tbjdbc |
| 배열 컬럼(VARRAY) | 매핑·왕복 정상 | `capability.ArrayMappingTest` 9건 — DDL·상한·null·빈 배열·`table()` 언네스트 |
| 배열 PL/SQL 헬퍼(Oracle 생성분) | **사용 불가** | `<타입>_concat` 2회차 호출부터 서버 워커 정지(재기동으로만 복구), `<타입>_positions` 는 `sdo_ordinate_array` 부재로 INVALID |
| STRUCT 속성 타입 | `varchar2`·`nvarchar2`·`number`·`date`·`timestamp`·`raw` 만 가능 | `binary_double`/`binary_float` → `JDBC-590703`, LOB → `JDBC-90651`. **드라이버 한계** — 기동 시 fail-fast 로 거름 |
| STRUCT 성분 check 제약 | **불가** | `check (mix is null or …)` 가 `JDBC-8147`. `supportsComponentCheckConstraints()=false` 로 끔 |
| `@Struct` + `@ElementCollection` | **불가** | Hibernate 6.6 boot 단계 `AssertionFailure` — dialect 무관 |

---

## 2-1. 상류(Hibernate) 와 의도적으로 다르게 가는 항목

§1·§2 는 전부 "Oracle 대비" 축이다. 이 절은 그 축으로는 잡히지 않는 것 — **Hibernate 상류
구현을 그대로 따르면 틀리거나 부족해서, 우리가 먼저 다르게 간 자리**를 모은다. 상류가 나중에
같은 방향으로 바뀌면 이 절에서 지우고 §2 로 옮긴다.

| 항목 | 상류 동작 | Tibero 동작 | 근거 |
|------|-----------|-------------|------|
| `TiberoSqlAstTranslator.createLockingWrapper` — 바깥 정렬 | 정렬 사양을 **서브쿼리에만** 넣음 (`OracleSqlAstTranslator`, **7.4.8 까지 동일**) | 바깥 래퍼에도 복사 | 아래 참고 |

### locking wrapper 의 바깥 `order by`

페이징 + 비관적 락은 `… fetch first ? rows only for update` 가 `JDBC-8004` 라서 locking
wrapper 를 쓴다 — 페이징을 서브쿼리로 밀고 `for update` 를 바깥에 두는 방식이다. 그런데 상류
구현은 정렬 사양을 서브쿼리에만 넣어 **바깥 쿼리에 `order by` 가 남지 않는다.**

SQL 은 `order by` 가 없으면 행 순서를 보장하지 않는다. 어떤 행이 선택되는지는 맞지만 어떤
순서로 돌아오는지는 실행계획에 달린다. ps06 10 만 행 실측:

```
sc desc 상위 12건, 기본 계획            100000 99999 99998 …  (우연히 맞음)
같은 질의 + /*+ FULL USE_HASH */         99989  99990  99991 …  (뒤집힘)
바깥 order by 추가 후 + 같은 힌트        100000 99999 99998 …  (유지)
```

예외도 경고도 없이 순서만 틀리므로 발견이 늦다. 래퍼가 적용되는 조건이 *루트 엔티티 하나 ·
조인 없음 · DISTINCT/GROUP BY/집계 없음* 이라 **가장 단순한 질의에서만** 생긴다.

**상류와 다르게 가기로 한 이유.** locking wrapper 자체가 HHH-16433("`order by` + `for update`
를 쓸 수 있게 해 달라", fix 6.2.4) 으로 도입된 장치다. 그 `order by` 가 결과 순서를 보장하지
못하면 도입 목적과 어긋난다. 상류에 의도를 밝힌 주석·테스트·이슈가 없고 이 정렬 유실을 보고한
이슈도 없어 **누락으로 판단**했다. 수정은 가산적이라 선택되는 행 집합은 바뀌지 않는다.

**재검토 조건** — 상류가 같은 수정을 넣으면 이 항목을 지우고 복사본을 상류에 맞춘다.
`capability.ReviewFixRegressionTest.pagedLockedQuery_keepsOuterOrderBy` 가 렌더된 SQL 에
바깥 `order by` 가 있는지 고정한다.

---

## 3. 유지보수 규칙

1. Oracle Dialect 와 diff 를 보다가 “Tibero 에 없다”는 API 를 발견하면 **먼저 이 문서 §1 을 검색**한다.
2. **미지원 / 기본값 적합**에 있으면 구현하지 않는다. 테스트가 깨지면 DB/Hibernate 스펙 변경인지 확인하고 **문서와 테스트를 함께** 수정한다.
3. **미구현**에 있으면 “빠뜨린 버그”가 아니라 **로드맵 후보**다. 지원하려면 문서 분류를 바꾸고 구현·테스트를 추가한다.
4. §2 에 없는 기본값을 “검증됨”으로 적지 않는다. 새로 검증하면 §2 와 테스트를 함께 추가한다.

---

## 4. 변경 이력

| 날짜 | 내용 |
|------|------|
| 2026-08-12 | 최초 작성 (6.6.1 / Hibernate 6.6.55 기준 판단 고정) |
| 2026-09-02 | Oracle dialect 48개 파일 전수 검사 결과 반영. `getArrayTypeName` / `getPreferredSqlTypeCodeForArray` 를 **§1.1 미지원 → §1.2 미구현** 으로 재분류(근거가 ANSI 문법이었고 Oracle 실제 경로는 VARRAY UDT임). §2 에 Oracle식 array UDT·JDBC 왕복 검증 결과 추가. 기준 인스턴스를 fs02_ps06(9999) 으로 명시 |
| 2026-09-16 | 백지 세션 전체 리뷰의 코드 결함 6건 수정 반영. `getMinimumSupportedVersion` 을 **§1.3 기본값 적합 → 구현됨** 으로 옮김(무인자 생성자의 `make(7)` 은 info 생성자 경로에 적용되지 않아 근거가 무효였음). §2-1 신설 — locking wrapper 의 바깥 `order by` 를 상류와 다르게 감 |
