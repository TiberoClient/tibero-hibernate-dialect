# 설치와 설정

JAR 선택, dependency 추가, Spring 설정, 직접 빌드하는 방법을 담습니다.
빠른 설정과 기능 요약은 [../README.md](../README.md) 에 있습니다.

## 요구 사항

- **Java**: 11 이상 (6.6 dialect 기준)
- **Hibernate**: JAR 버전에 맞는 Hibernate 버전 (아래 버전별 설명 참고). 6.6.1 은 **6.6.56.Final** 로 검증
- **Tibero**: **7.2.6 이상** — 6.6.1 은 7.2.6 FS02_PS06 으로 검증
- **Tibero JDBC 드라이버**: Tibero 에서 제공하는 JDBC JAR (별도 설치 필요) — [Tibero JDBC 드라이버 다운로드](https://technet.tibero.com/ko/front/download/findDownloadList.do?cmProductCode=0301)

---

## 사용법

### 1. JAR 파일 준비

이 저장소의 **`lib/`** 폴더에 Hibernate 버전별로 빌드된 dialect JAR 이 있습니다. 사용 중인 Hibernate 버전에 맞는 JAR 을 선택하세요.

| Hibernate 버전 | lib 내 JAR 파일 |
|----------------|-----------------|
| Hibernate 6.6 | `tibero-hibernate-dialect-6.6.1.jar` (최신) / `tibero-hibernate-dialect-6.6.0.jar` |
| Hibernate 5.x | `tibero-hibernate-dialect-5.jar` |
| Hibernate 4.x | `hibernate-tibero-dialect-4.4.0.jar` |
| Hibernate 3.x | `hibernate-tibero-dialect_3.6.6.jar` |
| Hibernate 2.x | `hibernate-tibero-dialect-2.1.6.jar` |

필요한 JAR 을 다운로드하거나 클론한 뒤 `lib/`에서 복사해 프로젝트에 포함시키면 됩니다.

### 2. Spring 프로젝트에 dependency 추가

#### Gradle (로컬 JAR)

JAR 을 프로젝트 루트의 `lib` 폴더 등에 넣은 뒤:

```groovy
dependencies {
    // Hibernate (사용 중인 버전에 맞게)
    implementation 'org.hibernate.orm:hibernate-core:6.6.x'  // 6.6 사용 시

    // Tibero Hibernate Dialect (로컬 JAR)
    implementation files('lib/tibero-hibernate-dialect-6.6.1.jar')

    // Tibero JDBC 드라이버 (Tibero에서 제공하는 JAR 경로)
    implementation files('path/to/tibero7-jdbc.jar')
}
```

#### Maven (로컬 JAR)

JAR 을 `src/main/resources/lib` 또는 원하는 위치에 두고:

```xml
<dependency>
    <groupId>com.tmax.tibero</groupId>
    <artifactId>tibero-hibernate-dialect</artifactId>
    <version>6.6.1</version>
    <scope>system</scope>
    <systemPath>${project.basedir}/lib/tibero-hibernate-dialect-6.6.1.jar</systemPath>
</dependency>

<!-- Tibero JDBC 드라이버도 동일하게 system scope로 추가 -->
<dependency>
    <groupId>com.tmax.tibero</groupId>
    <artifactId>tibero-jdbc</artifactId>
    <version>7</version>
    <scope>system</scope>
    <systemPath>${project.basedir}/lib/tibero7-jdbc.jar</systemPath>
</dependency>
```

### 3. Spring 설정에서 Dialect 지정

#### application.properties (Spring Boot)

```properties
# JPA / Hibernate
spring.jpa.database-platform=com.tmax.tibero.hibernate.dialect.TiberoDialect
spring.jpa.hibernate.ddl-auto=validate

# DataSource
spring.datasource.url=jdbc:tibero:thin:@<호스트>:<포트>:<SID>
spring.datasource.username=<계정>
spring.datasource.password=<비밀번호>
spring.datasource.driver-class-name=com.tmax.tibero.jdbc.TbDriver
```

#### application.yml

```yaml
spring:
  jpa:
    database-platform: com.tmax.tibero.hibernate.dialect.TiberoDialect
    hibernate:
      ddl-auto: validate
  datasource:
    url: jdbc:tibero:thin:@<호스트>:<포트>:<SID>
    username: <계정>
    password: <비밀번호>
    driver-class-name: com.tmax.tibero.jdbc.TbDriver
```

#### Java 설정 (Hibernate 직접 사용 시)

```java
Configuration config = new Configuration();
config.setProperty("hibernate.dialect", "com.tmax.tibero.hibernate.dialect.TiberoDialect");
// ... 기타 설정
```

Spring Boot 를 쓰는 경우 `spring.jpa.database-platform`만 Tibero dialect 로 지정하면 됩니다.

---

## 버전별 추가 내용

### 6.6.1 (tibero-hibernate-dialect-6.6.1.jar) — 최신

- **대상**: Hibernate ORM **6.6.x**
- **Java**: 11 이상 (빌드 기준 17)
- **내용**: 6.6.0 기반 개선 + 결함 수정. 상세는 [CHANGELOG.md](CHANGELOG.md) 참고

  | 구분 | 내용 |
  |---|---|
  | 결함 수정 | JSON 컬럼 DDL 실패 · 페이징+락 문법 오류 · 프로시저 이름 파라미터 · month 연산 시각 유실 · `extract(epoch from DATE)` 실패 · HQL 파생 테이블 실패 · `NUMBER(p,0)` 역매핑 값 손상 · 페이징+락 정렬 유실 · `LimitHandler` 공유 가변 상태 · 최소 지원 버전 미검사 |
  | 신규 기능 | `@Struct`(object UDT) · 배열 컬럼(VARRAY) · **JSON 집계 임베더블** · dialect 자동 인식 · 예약어 자동 인용 · 대량 배치 로딩 분할 |
  | 타입 | NVARCHAR2 상한 32766 · `binary_float`/`binary_double` 매핑·바인딩 · `NUMBER(p,0)` 역매핑을 `Integer`/`Long` 두 단계로 |
  | SQL 생성 | `TiberoSqlAstTranslator` 20종 — MERGE 에뮬레이션, 재귀 CTE, 페이징+락 래핑, 파생 테이블 별칭, 정수 나눗셈 보정, LOB 비교 |

  ⚠️ **동작이 달라지는 것 네 가지** — IDENTITY DDL, month/quarter/year 연산, float/double DDL,
  네이티브 쿼리의 `NUMBER(p,0)` 결과 타입. [user-notes.md](user-notes.md) 를 확인하세요

### 6.6.0 (tibero-hibernate-dialect-6.6.0.jar)

- **대상**: Hibernate ORM **6.6.x**
- **Java**: 11 이상
- **내용**:
  - Hibernate 6.6 API 에 맞춘 Dialect 구현
  - Tibero 시퀀스/IDENTITY 지원, 페이징(LimitHandler), 락/예외 변환 등 6.6 스펙 반영
  - `TiberoDialect`, `TiberoSequenceSupport`, `TiberoIdentityColumnSupport`, `TiberoLimitHandler` 등 구성

### 5.x 이하 (레거시)

| Hibernate | JAR |
|---|---|
| 5.x | `tibero-hibernate-dialect-5.jar` |
| 4.x | `hibernate-tibero-dialect-4.4.0.jar` |
| 3.6.x | `hibernate-tibero-dialect_3.6.6.jar` |
| 2.x | `hibernate-tibero-dialect-2.1.6.jar` |

각 버전의 API 와 타입 체계에 맞춘 구현입니다. 신규 개발에는 6.6 을 쓰세요.

---

## 빌드 (선택 사항)

**Dialect 만 사용할 경우**에는 빌드할 필요 없이, 위에서 안내한 대로 `lib/` 폴더에 있는 해당 버전 JAR 을 복사해 프로젝트에 넣고 dependency 로 추가하면 됩니다.

아래 빌드 방법은 **소스 코드를 수정했거나, 저장소를 클론한 뒤 직접 JAR 을 만들고 싶을 때만** 참고하면 됩니다.

빌드에는 **JDK 17 이상**이 필요합니다 — Gradle 9.0.0 자체가 요구하는 조건입니다. 반면 만들어지는 JAR 은 **JDK 11 이상**에서 동작합니다(`options.release = 11`). 즉 빌드 환경과 실행 환경의 하한이 서로 다릅니다.

```bash
./gradlew build
```

빌드 결과물은 `build/libs/` 아래에 생성됩니다.

`build.gradle` 의 Tibero JDBC JAR 경로는 **플레이스홀더로 들어 있으므로 각자 채워야 합니다.**

```groovy
implementation files('Tibero_JDBC_JAR_절대경로')   // ← 사용 중인 드라이버 경로로 교체
```

### 테스트 실행

테스트 중 상당수는 살아 있는 Tibero 인스턴스가 필요합니다. 접속 정보는 커맨드라인으로 넘깁니다.

```bash
# DB 불필요 — 생성되는 SQL 문자열과 dialect 선언값만 검사
./gradlew test --tests 'contract.*' --tests 'render.*'

# 전체
./gradlew --init-script init-test.gradle test \
  -Dhibernate.connection.url=jdbc:tibero:thin:@<호스트>:<포트>:<SID> \
  -Dhibernate.connection.username=<계정> -Dhibernate.connection.password=<비밀번호>
```

⚠️ `-D` 를 빼먹으면 `test/resources/hibernate.properties` 의 값으로 접속합니다.

---

## 함께 볼 문서

| 문서 | 내용 |
|---|---|
| [../README.md](../README.md) | 빠른 설정과 기능 요약 |
| [CHANGELOG.md](CHANGELOG.md) | 릴리즈별 변경 사항 |
| [user-notes.md](user-notes.md) | 업그레이드 시 확인할 것과 대응 방법 |

---

## 라이선스

LGPL 2.1. 프로젝트 루트의 `lgpl.txt`를 참고하세요.
