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
