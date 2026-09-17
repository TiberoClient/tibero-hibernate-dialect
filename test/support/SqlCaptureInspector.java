package support;

import org.hibernate.resource.jdbc.spi.StatementInspector;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * 생성된 SQL 을 모아 두었다가 테스트가 들여다보게 해 주는 훅.
 *
 * <p>⚠️ <b>수집 버퍼가 static 이라 테스트 병렬 실행을 켜면 깨진다.</b> Hibernate 가
 * {@code StatementInspector} 를 SessionFactory 마다 새로 만들기 때문에 인스턴스 필드로는
 * 테스트에서 꺼내 볼 수 없어 static 으로 두었다. 그 대가로 <b>모든 SessionFactory 의 SQL 이
 * 한 버퍼에 섞인다</b> — 지금은 Gradle 기본값인 직렬 실행이라 {@link #clear()} 로 경계를
 * 그을 수 있지만, {@code maxParallelForks} 를 올리는 순간 다른 테스트의 SQL 이 섞여 들어와
 * 단언이 무작위로 실패한다. 예외가 아니라 <b>가끔 틀리는 결과</b>로 나타나 원인을 찾기 어렵다.
 *
 * <p>병렬을 켜야 한다면 이 클래스를 먼저 인스턴스 단위로 바꿀 것.
 */
public class SqlCaptureInspector implements StatementInspector {
    private static final List<String> SQLS = Collections.synchronizedList(new ArrayList<>());

    public static void clear() {
        SQLS.clear();
    }

    public static List<String> getSqls() {
        return new ArrayList<>(SQLS);
    }

    @Override
    public String inspect(String sql) {
        SQLS.add(sql);
        return sql;
    }
}
