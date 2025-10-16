from sqlalchemy import create_engine, MetaData, Table, select, insert
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError


def get_table(conn_str: str, schema_name: str, table_name: str):
    """
    주어진 DB 연결 문자열, 스키마명, 테이블명을 이용해 Table 객체를 반환
    """
    engine = create_engine(conn_str)
    metadata = MetaData(schema=schema_name)
    metadata.reflect(bind=engine, only=[table_name])
    table = Table(table_name, metadata, autoload_with=engine)
    return table


def compare_table_structures(table1, table2):
    """
    두 테이블의 컬럼 구조를 비교
    """
    cols1 = {c.name: str(c.type) for c in table1.columns}
    cols2 = {c.name: str(c.type) for c in table2.columns}

    only_in_1 = set(cols1.keys()) - set(cols2.keys())
    only_in_2 = set(cols2.keys()) - set(cols1.keys())
    diff_types = {
        k: (cols1[k], cols2[k])
        for k in set(cols1.keys()) & set(cols2.keys())
        if cols1[k] != cols2[k]
    }

    return {"only_in_1": only_in_1, "only_in_2": only_in_2, "diff_types": diff_types}


def transfer_data(conn_str_src, schema_src, table_src,
                  conn_str_dst, schema_dst, table_dst,
                  where_clause=None):
    """
    src → dst 데이터 이관
    """
    engine_src = create_engine(conn_str_src)
    engine_dst = create_engine(conn_str_dst)

    src_table = get_table(conn_str_src, schema_src, table_src)
    dst_table = get_table(conn_str_dst, schema_dst, table_dst)

    # --- 구조 비교 ---
    diff = compare_table_structures(src_table, dst_table)
    if diff["only_in_1"] or diff["only_in_2"] or diff["diff_types"]:
        print("⚠️ 테이블 구조가 완전히 일치하지 않습니다.")
        print(diff)
        return

    # --- 데이터 선택 ---
    with engine_src.connect() as conn_src:
        stmt = select(src_table)
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        result = conn_src.execute(stmt)
        rows = [dict(row._mapping) for row in result]

    # --- 데이터 삽입 ---
    if not rows:
        print("📭 전송할 데이터가 없습니다.")
        return

    with engine_dst.begin() as conn_dst:
        try:
            conn_dst.execute(insert(dst_table), rows)
            print(f"✅ {len(rows)}개 행을 {table_dst}에 삽입했습니다.")
        except SQLAlchemyError as e:
            print("❌ 데이터 삽입 중 오류 발생:", e)


# ==============================
# 예시 사용법
# ==============================

if __name__ == "__main__":
    conn_src = "postgresql+psycopg2://user1:pass1@192.168.1.10/db1"
    conn_dst = "postgresql+psycopg2://user2:pass2@192.168.1.20/db2"

    transfer_data(
        conn_str_src=conn_src,
        schema_src="public",
        table_src="customers",
        conn_str_dst=conn_dst,
        schema_dst="public",
        table_dst="customers_backup",
        where_clause=None  # 예: text("id > 100")
    )





    from sqlalchemy import create_engine, select, insert, func
from sqlalchemy.exc import SQLAlchemyError

def transfer_data(conn_str_src, schema_src, table_src,
                  conn_str_dst, schema_dst, table_dst,
                  where_clause=None, chunk_size=1000):
    engine_src = create_engine(conn_str_src)
    engine_dst = create_engine(conn_str_dst)

    src_table = get_table(conn_str_src, schema_src, table_src)
    dst_table = get_table(conn_str_dst, schema_dst, table_dst)

    # --- row count 계산 ---
    with engine_src.connect() as conn_src:
        stmt_count = select(func.count()).select_from(src_table)
        if where_clause is not None:
            stmt_count = stmt_count.where(where_clause)
        count = conn_src.execute(stmt_count).scalar()
        print(f"🔍 이관 대상 행 수: {count}")

    # --- chunk 단위 select/insert ---
    if count == 0:
        print("📭 전송할 데이터가 없습니다.")
        return

    transferred = 0
    with engine_src.connect() as conn_src, engine_dst.begin() as conn_dst:
        stmt = select(src_table)
        if where_clause is not None:
            stmt = stmt.where(where_clause)
        result = conn_src.execution_options(stream_results=True).execute(stmt)

        rows = []
        for row in result:
            rows.append(dict(row._mapping))
            if len(rows) >= chunk_size:
                try:
                    conn_dst.execute(insert(dst_table), rows)
                    transferred += len(rows)
                    print(f"✅ {transferred}/{count} rows migrated")
                except SQLAlchemyError as e:
                    print("❌ 데이터 삽입 중 오류 발생:", e)
                    raise
                rows = []
        if rows:  # 남은 row
            try:
                conn_dst.execute(insert(dst_table), rows)
                transferred += len(rows)
                print(f"✅ {transferred}/{count} rows migrated")
            except SQLAlchemyError as e:
                print("❌ 데이터 삽입 중 오류 발생:", e)

    print("🎉 이관 완료!")


    from sqlalchemy import create_engine, MetaData, Table, insert, select
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import SQLAlchemyError

def clone_table_and_transfer_data(src_conn_str, dst_conn_str, schema, table_name, chunk_size=10000):    
    # 1. 엔진 및 메타데이터
    engine_src = create_engine(src_conn_str)
    engine_dst = create_engine(dst_conn_str)

    metadata_src = MetaData(schema=schema)
    metadata_src.reflect(bind=engine_src, only=[table_name])
    src_table = Table(table_name, metadata_src, autoload_with=engine_src)

    # 2. dst에 동일 테이블 생성
    metadata_dst = MetaData(schema=schema)
    dst_table = Table(table_name, metadata_dst)

    # dst DB에 테이블 없으면 새로 생성
    if not engine_dst.dialect.has_table(engine_dst.connect(), table_name, schema=schema):
        ddl = str(CreateTable(src_table).compile(engine_dst))
        print("🔨 Creating table in dst:\n", ddl)
        with engine_dst.begin() as conn:
            conn.execute(CreateTable(src_table))
        print(f"✅ {table_name} 테이블 생성 완료")

    # dst_table 객체 재반영
    metadata_dst.reflect(bind=engine_dst, only=[table_name])
    dst_table = Table(table_name, metadata_dst, autoload_with=engine_dst)

    # 3. 데이터 이관
    with engine_src.connect() as conn_src:
        count = conn_src.execute(select(src_table.count())).scalar()
        print(f"🚚 전체 이관 대상 행: {count}")

        stmt = select(src_table)
        result = conn_src.execution_options(stream_results=True).execute(stmt)

        rows, transferred = [], 0
        with engine_dst.begin() as conn_dst:
            for row in result:
                rows.append(dict(row._mapping))
                if len(rows) >= chunk_size:
                    conn_dst.execute(insert(dst_table), rows)
                    transferred += len(rows)
                    print(f"🔄 {transferred}/{count} rows migrated")
                    rows = []
            if rows:
                conn_dst.execute(insert(dst_table), rows)
                transferred += len(rows)
                print(f"🔄 {transferred}/{count} rows migrated")
        print(f"🎉 이관 완료! 총 {transferred} rows.")
