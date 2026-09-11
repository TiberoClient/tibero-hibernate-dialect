package com.tmax.tibero.hibernate.dialect;

import org.hibernate.boot.registry.selector.spi.DialectSelector;
import org.hibernate.dialect.Dialect;

/**
 * {@code hibernate.dialect=Tibero} 처럼 <b>짧은 이름</b>으로 지정할 수 있게 한다.
 *
 * <p>이게 없으면 FQN 을 전부 적어야 하고, 오타를 내면 이런 오류가 난다.
 *
 * <pre>
 * hibernate.dialect=Tibero
 *   → Unable to resolve name [Tibero] as strategy [org.hibernate.dialect.Dialect]
 * </pre>
 *
 * <p>{@link TiberoDialectResolver} 와는 역할이 다르다 — 저쪽은 <b>설정이 없을 때</b>
 * JDBC 메타데이터로 알아내는 것이고, 이쪽은 <b>설정을 짧게 적었을 때</b> 풀어주는 것이다.
 * 둘 다 {@code META-INF/services} 로 등록된다.
 *
 * <p>필수는 아니지만 등록 비용이 거의 없고, FQN 이 긴 이 프로젝트에서는
 * 설정 파일 가독성에 도움이 된다.
 */
public class TiberoDialectSelector implements DialectSelector {

    @Override
    public Class<? extends Dialect> resolve(String name) {
        return "Tibero".equalsIgnoreCase(name) ? TiberoDialect.class : null;
    }
}
