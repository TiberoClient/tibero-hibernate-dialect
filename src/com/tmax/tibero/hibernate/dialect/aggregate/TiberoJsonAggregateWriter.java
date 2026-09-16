package com.tmax.tibero.hibernate.dialect.aggregate;

import org.hibernate.dialect.aggregate.AggregateSupport.WriteExpressionRenderer;
import org.hibernate.metamodel.mapping.EmbeddableMappingType;
import org.hibernate.metamodel.mapping.SelectableMapping;
import org.hibernate.metamodel.mapping.SelectablePath;
import org.hibernate.sql.ast.spi.SqlAppender;
import org.hibernate.sql.ast.SqlAstTranslator;
import org.hibernate.dialect.aggregate.AggregateSupport.AggregateColumnWriteExpression;
import org.hibernate.sql.ast.SqlAstNodeRenderingMode;
import org.hibernate.type.descriptor.jdbc.AggregateJdbcType;
import org.hibernate.type.spi.TypeConfiguration;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * JSON 집계 컬럼의 <b>UPDATE 오른쪽 전체</b>를 만든다.
 */
final class TiberoJsonAggregateWriter {

    private TiberoJsonAggregateWriter() {
    }

    /** 트리의 한 마디 — 잎(스칼라 필드)이거나 가지(중첩 임베더블)다. */
    interface Node {
        void append(SqlAppender sb, SqlAstTranslator<?> translator, AggregateColumnWriteExpression expression);
    }

    /**
     * 최상위 — {@code json_mergepatch(바닥, 조각 returning 타입)} 을 만든다.
     *
     * <p>Hibernate 가 {@code WriteExpressionRenderer} 로 받아 SET 오른쪽에 꽂는다.
     */
    static final class Root extends Aggregate implements WriteExpressionRenderer {

        private final boolean nullable;
        private final String path;

        Root(SelectableMapping aggregateColumn, SelectableMapping[] columns,
             TypeConfiguration typeConfiguration) {
            super(aggregateColumn);
            this.nullable = aggregateColumn.isNullable();
            this.path = aggregateColumn.getSelectionExpression();
            initializeSubExpressions(columns, typeConfiguration);
        }

        @Override
        public void render(SqlAppender sqlAppender, SqlAstTranslator<?> translator,
                           AggregateColumnWriteExpression expression, String qualifier) {
            final String basePath = (qualifier == null || qualifier.isBlank())
                    ? path : qualifier + "." + path;

            sqlAppender.append("json_mergepatch(");
            if (nullable) {
                // Oracle 은 coalesce 를 쓰는데 Tibero 는 json 컬럼에 coalesce 를 못 쓴다(JDBC-11022).
                // 빈 객체도 json_object(returning …) 로는 못 만든다(JDBC-8004).
                sqlAppender.append("nvl(");
                sqlAppender.append(basePath);
                sqlAppender.append(",json_query('{}','$' returning ");
                sqlAppender.append(ddlTypeName);
                sqlAppender.append("))");
            }
            else {
                sqlAppender.append(basePath);
            }
            sqlAppender.append(',');
            append(sqlAppender, translator, expression);
            sqlAppender.append(" returning ");
            sqlAppender.append(ddlTypeName);
            sqlAppender.append(')');
        }
    }

    /**
     * 가지 — {@code json_object('a':…, 'b':… returning 타입)}.
     *
     * <p>중첩 임베더블이면 값 자리에 또 {@code json_object} 가 들어가고, 머지패치가
     * 재귀적으로 병합해 준다(실측 확인 — 안쪽의 건드리지 않은 필드가 보존된다).
     */
    static class Aggregate implements Node {

        private final LinkedHashMap<String, Node> children = new LinkedHashMap<>();
        final EmbeddableMappingType embeddableMappingType;
        final String ddlTypeName;

        Aggregate(SelectableMapping selectableMapping) {
            this.embeddableMappingType =
                    ((AggregateJdbcType) selectableMapping.getJdbcMapping().getJdbcType())
                            .getEmbeddableMappingType();
            this.ddlTypeName = determineJsonTypeName(selectableMapping);
        }

        /**
         * 바꿀 컬럼 목록을 <b>경로별로</b> 트리에 심는다.
         *
         * <p>{@code addr.inner.city} 같은 경로면 {@code addr} 밑에 {@code inner} 가지를 만들고
         * 그 밑에 {@code city} 잎을 단다. 그래야 {@code json_object('inner':json_object('city':?))}
         * 모양이 나온다.
         */
        final void initializeSubExpressions(SelectableMapping[] columns, TypeConfiguration typeConfiguration) {
            for (SelectableMapping column : columns) {
                final SelectablePath[] parts = column.getSelectablePath().getParts();
                Aggregate current = this;
                EmbeddableMappingType currentType = embeddableMappingType;

                // parts[0] 은 집계 컬럼 자신, 마지막은 잎. 그 사이가 중첩 가지다
                for (int i = 1; i < parts.length - 1; i++) {
                    final SelectableMapping nested = currentType.getJdbcValueSelectable(
                            currentType.getSelectableIndex(parts[i].getSelectableName()));
                    current = (Aggregate) current.children.computeIfAbsent(
                            parts[i].getSelectableName(), k -> new Aggregate(nested));
                    currentType = current.embeddableMappingType;
                }
                current.children.put(
                        parts[parts.length - 1].getSelectableName(),
                        new Basic(column, jsonWriteExpression(column, typeConfiguration)));
            }
        }

        @Override
        public void append(SqlAppender sb, SqlAstTranslator<?> translator,
                           AggregateColumnWriteExpression expression) {
            sb.append("json_object");
            char separator = '(';
            for (Map.Entry<String, Node> entry : children.entrySet()) {
                sb.append(separator);
                if (entry.getValue() instanceof Aggregate) {
                    sb.append('\'');
                    sb.append(entry.getKey());
                    sb.append("':");
                }
                entry.getValue().append(sb, translator, expression);
                separator = ',';
            }
            sb.append(" returning ");
            sb.append(ddlTypeName);
            sb.append(')');
        }
    }

    /** 잎 — {@code 'city':&lt;값 식&gt;}. */
    private static final class Basic implements Node {

        private final SelectableMapping selectableMapping;
        private final String prefix;
        private final String suffix;

        Basic(SelectableMapping selectableMapping, String writeExpression) {
            this.selectableMapping = selectableMapping;
            if ("?".equals(writeExpression)) {
                this.prefix = "";
                this.suffix = "";
            }
            else {
                final String[] parts = writeExpression.split("\\?", -1);
                if (parts.length != 2) {
                    throw new IllegalArgumentException(
                            "write expression must contain exactly one '?': " + writeExpression);
                }
                this.prefix = parts[0];
                this.suffix = parts[1];
            }
        }

        @Override
        public void append(SqlAppender sb, SqlAstTranslator<?> translator,
                           AggregateColumnWriteExpression expression) {
            sb.append('\'');
            sb.append(selectableMapping.getSelectableName());
            sb.append("':");
            sb.append(prefix);
            // NO_UNTYPED — 이 식이 어떤 표현식 안에 들어갈지 모르므로 타입 추론이 필요한
            // 자리는 명시적으로 cast 시킨다. Oracle 도 같은 이유로 같은 모드를 쓴다
            translator.render(expression.getValueExpression(selectableMapping),
                    SqlAstNodeRenderingMode.NO_UNTYPED);
            sb.append(suffix);
        }
    }

    // ------------------------------------------------------------------

    /**
     * 값 하나를 JSON 에 넣을 때 감쌀 식.
     *
     * <p>대부분은 값을 그대로 쓰지만 세 부류는 감싸야 한다.
     *
     * <table>
     *   <tr><th>타입</th><th>감싸는 이유</th></tr>
     *   <tr><td>CLOB</td><td>{@code to_clob} — Oracle 과 동일</td></tr>
     *   <tr><td>BOOLEAN(number)</td><td>{@code decode} 로 JSON 의 {@code true}/{@code false} 로 —
     *       Oracle 과 동일</td></tr>
     *   <tr><td><b>DATE · TIME · TIMESTAMP 계열</b></td>
     *       <td><b>Tibero 고유</b> — {@code to_char} 로 ISO-8601 을 강제한다</td></tr>
     * </table>
     *
     * <p>temporal 래핑이 이 구현의 핵심 차이다. Tibero 의 {@code json_object} 는 날짜를
     * {@code "2024/03/05"} 로 직렬화하는데 읽기 쪽({@code to_date(…,'YYYY-MM-DD')})과 Java 쪽
     * ({@code JsonHelper}) 은 ISO 를 기대한다. 감싸지 않으면 <b>HQL 부분 갱신 한 번으로 그 행의
     * 엔티티가 로드 불가</b>가 된다 — 쓸 때는 조용하고 읽을 때 터지는 유형이다.
     *
     * <p>읽기 쪽 형식 문자열과 <b>반드시 짝이 맞아야 한다</b>
     * ({@code TiberoAggregateSupport.aggregateComponentCustomReadExpression}).
     * 한쪽만 바꾸면 왕복이 깨진다.
     */
    private static String jsonWriteExpression(SelectableMapping column, TypeConfiguration typeConfiguration) {
        final String writeExpression = column.getWriteExpression();
        final int sqlTypeCode = column.getJdbcMapping().getJdbcType().getDefaultSqlTypeCode();
        if (sqlTypeCode == org.hibernate.type.SqlTypes.ARRAY) {
            return arrayWriteExpression(column, writeExpression, typeConfiguration);
        }
        return scalarWriteExpression(sqlTypeCode, column, writeExpression, typeConfiguration);
    }

    /**
     * 배열이 아닌 값 하나를 감싸는 식.
     *
     * <p>{@code writeExpression} 은 보통 {@code ?} 하나지만 {@code @ColumnTransformer} 를
     * 쓰면 {@code encrypt(?)} 처럼 올 수도 있어 그대로 끼워 넣는다.
     */
    private static String scalarWriteExpression(int sqlTypeCode, SelectableMapping column,
                                                String writeExpression, TypeConfiguration typeConfiguration) {
        switch (sqlTypeCode) {
            case org.hibernate.type.SqlTypes.CLOB:
                return "to_clob(" + writeExpression + ")";
            case org.hibernate.type.SqlTypes.DATE:
                return "to_char(" + writeExpression + ",'YYYY-MM-DD')";
            case org.hibernate.type.SqlTypes.TIME:
                return "to_char(" + writeExpression + ",'hh24:mi:ss')";
            case org.hibernate.type.SqlTypes.TIMESTAMP:
                return "to_char(" + writeExpression + ",'YYYY-MM-DD\"T\"hh24:mi:ss.FF9')";
            case org.hibernate.type.SqlTypes.TIMESTAMP_WITH_TIMEZONE:
            case org.hibernate.type.SqlTypes.TIMESTAMP_UTC:
                return "to_char(" + writeExpression + ",'YYYY-MM-DD\"T\"hh24:mi:ss.FF9TZH:TZM')";
            case org.hibernate.type.SqlTypes.BOOLEAN:
                final String sqlTypeName = org.hibernate.sql.ast.spi.AbstractSqlAstTranslator
                        .getSqlTypeName(column, typeConfiguration);
                if (sqlTypeName.toLowerCase(java.util.Locale.ROOT).trim().startsWith("number")) {
                    return "decode(" + writeExpression + ",1,'true',0,'false',null)";
                }
                return writeExpression;
            default:
                return writeExpression;
        }
    }

    /**
     * 배열 필드를 JSON 배열로 만드는 식 — <b>Oracle 에는 없는 보강</b>.
     */
    private static String arrayWriteExpression(SelectableMapping column, String writeExpression,
                                               TypeConfiguration typeConfiguration) {
        final org.hibernate.type.descriptor.jdbc.JdbcType jdbcType =
                column.getJdbcMapping().getJdbcType();
        String element = "t.column_value";
        if (jdbcType instanceof org.hibernate.type.descriptor.jdbc.ArrayJdbcType) {
            final int elementTypeCode = ((org.hibernate.type.descriptor.jdbc.ArrayJdbcType) jdbcType)
                    .getElementJdbcType().getDefaultSqlTypeCode();
            element = elementTypeCode == org.hibernate.type.SqlTypes.BOOLEAN
                    // 원소가 boolean 이면 varray 안에는 1/0 이 들어 있다 — Tibero 에 boolean
                    // 컬럼 타입이 없어서다. Java 쪽은 JSON 참/거짓으로 쓰므로 맞춰 준다
                    ? "decode(t.column_value,1,'true',0,'false',null)"
                    : scalarWriteExpression(elementTypeCode, column, element, typeConfiguration);
        }
        return "(select json_arrayagg(" + element + ") from table(" + writeExpression + ") t)";
    }

    /**
     * {@code returning} 뒤에 붙일 타입 이름.
     *
     * <p>{@code @Column(columnDefinition=…)} 으로 명시했으면 그것을, 아니면 {@code json} 을 쓴다.
     * Tibero 는 네이티브 {@code json} 타입이 있어 Oracle 19c 처럼 {@code blob} 으로 내려갈
     * 필요가 없다 — {@code TiberoDialect} 가 {@code DdlTypeImpl(JSON, "json")} 을 등록한다.
     *
     * <p>{@code columnDefinition} 을 그대로 쓰는 덕에 {@code blob} 으로 선언한 컬럼도
     * {@code json_query('{}','$' returning blob)} 이 되어 함께 동작한다.
     */
    static String determineJsonTypeName(SelectableMapping aggregateColumn) {
        final String columnDefinition = aggregateColumn.getColumnDefinition();
        return columnDefinition == null ? "json" : columnDefinition;
    }
}
