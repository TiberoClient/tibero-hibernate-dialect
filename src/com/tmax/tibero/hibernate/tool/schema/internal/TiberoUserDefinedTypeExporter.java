package com.tmax.tibero.hibernate.tool.schema.internal;

import org.hibernate.boot.Metadata;
import org.hibernate.boot.model.naming.Identifier;
import org.hibernate.boot.model.relational.QualifiedName;
import org.hibernate.boot.model.relational.QualifiedNameParser;
import org.hibernate.boot.model.relational.SqlStringGenerationContext;
import org.hibernate.dialect.Dialect;
import org.hibernate.mapping.UserDefinedArrayType;
import org.hibernate.tool.schema.internal.StandardUserDefinedTypeExporter;

import static org.hibernate.type.SqlTypes.TABLE;

/**
 * 사용자 정의 타입(UDT)의 DDL 을 만든다 — object 타입과 array 타입 둘 다.
 */
public class TiberoUserDefinedTypeExporter extends StandardUserDefinedTypeExporter {

    public TiberoUserDefinedTypeExporter(Dialect dialect) {
        super(dialect);
    }

    /**
     * 배열 UDT 선언.
     *
     * <pre>
     * varying array   create or replace type StringArray as varying array(10) of varchar2(255 char)
     * nested table    create or replace type StringArray as table of varchar2(255 char)
     * </pre>
     *
     * <p>{@code arraySqlTypeCode} 가 비어 있거나 {@code TABLE} 이면 nested table 로 낸다 —
     * Hibernate 가 그렇게 구분한다. 현재 {@code TiberoArrayJdbcType} 은 varying array 만
     * 만들지만, nested table 경로를 막아둘 이유가 없어 상위 규약대로 처리한다.
     */
    @Override
    public String[] getSqlCreateStrings(
            UserDefinedArrayType userDefinedType, Metadata metadata, SqlStringGenerationContext context) {
        final String typeName = context.format(qualifiedName(userDefinedType));
        final Integer arraySqlTypeCode = userDefinedType.getArraySqlTypeCode();
        final String elementType = userDefinedType.getElementTypeName();
        if (arraySqlTypeCode == null || arraySqlTypeCode == TABLE) {
            return new String[]{"create or replace type " + typeName + " as table of " + elementType};
        }
        return new String[]{
                "create or replace type " + typeName
                        + " as varying array(" + userDefinedType.getArrayLength() + ") of " + elementType};
    }

    /**
     * 배열 UDT 삭제.
     *
     * <p>{@code force} 를 붙인다 — 그 타입을 컬럼으로 쓰는 테이블이 먼저 지워지지 않았을 때
     * 의존성 때문에 실패하는 것을 막는다. Oracle 도 같다.
     */
    @Override
    public String[] getSqlDropStrings(
            UserDefinedArrayType userDefinedType, Metadata metadata, SqlStringGenerationContext context) {
        final String ifExists = dialect.supportsIfExistsBeforeTypeName() ? "if exists " : "";
        return new String[]{
                "drop type " + ifExists + context.format(qualifiedName(userDefinedType)) + " force"};
    }

    private static QualifiedName qualifiedName(UserDefinedArrayType udt) {
        return new QualifiedNameParser.NameParts(
                Identifier.toIdentifier(udt.getCatalog(), udt.isCatalogQuoted()),
                Identifier.toIdentifier(udt.getSchema(), udt.isSchemaQuoted()),
                udt.getNameIdentifier());
    }
}
