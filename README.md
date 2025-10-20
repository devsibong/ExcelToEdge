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



from sqlalchemy import create_engine, MetaData, Table, insert, select, func
from sqlalchemy.schema import CreateTable
from sqlalchemy.exc import SQLAlchemyError

def clone_table_and_transfer_data(
    src_conn_str, src_schema, table_name, 
    dst_conn_str, dst_schema,
    chunk_size=10000
):    
    # 1. 엔진 및 메타데이터
    engine_src = create_engine(src_conn_str)
    engine_dst = create_engine(dst_conn_str)

    metadata_src = MetaData(schema=src_schema)
    metadata_src.reflect(bind=engine_src, only=[table_name])
    src_table = Table(table_name, metadata_src, autoload_with=engine_src)

    # 2. dst에 동일 테이블 생성
    # 테이블 정의를 복제하되, schema만 dst_schema로 바꿈
    table_ddl = CreateTable(src_table).compile(engine_dst)
    ddl_sql = str(table_ddl).replace(f'"{src_schema}".', f'"{dst_schema}".')
    print("🔨 Creating table in dst (with different schema):\n", ddl_sql)
    with engine_dst.begin() as conn:
        conn.execute(ddl_sql)
    print(f"✅ {dst_schema}.{table_name} 테이블 생성 완료")

    # dst_table 객체 재반영
    metadata_dst = MetaData(schema=dst_schema)
    metadata_dst.reflect(bind=engine_dst, only=[table_name])
    dst_table = Table(table_name, metadata_dst, autoload_with=engine_dst)

    # 3. 데이터 이관
    with engine_src.connect() as conn_src:
        count = conn_src.execute(select(func.count()).select_from(src_table)).scalar()
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


langgraph로 만들어진 코드들이 있다. 일부를 보여줄테니 내가 궁금한것을 설명해라.
initial_state: IntentAnalysisState = {
"user_question": "test"
"persona_state":{
"curplanid": "222P"
...
}
...
}
planner_builder = build_planner()
planner_graph = planner_builder.compile()
config = RunnableConfig(recursion_limit=10, configurable={"thread_id":str(uuid.uuid4)))})
planner_graph.invoke(initial_state, config=config)

이게 테스트 코드로 들어와있다.

나는 어떤것을 원하냐면, api요청이 들어왔을 때 이 정보를 쪼개서 그래프를 invoke하고, 그 결과를 다시 편집해 response에 담아야 한다. IntentAnalysisState는 typedict이다.

장고 기반으로 view를 정의해놓았고, 여기 연결화기 위해서 advisor(request_dict)라는 함수를 정의하여 return값을 true, result, none, none 이렇게 보낼 것이다.
이 함수 안에 request_dict를 분해하여 초기 IntentAnalysisState객체를 만들고, 이를 활용해 graph를 build하여 invoke하고, 그 결과를 객체에 담아 이를 분석해 response를 만드는 것. 이것은 정상적인 프로세스라고 볼수있는지? 만약 그렇다면 예시 advisor(request_dict)를 작성해봐라. 어떻게 하냐면

그래프는 무조건 요청마다 build/compile한다.
주요 분기점은 다음과 같다. request_dict에 threadid값이 있다면, 해당 값을 사용한다. 만약에 없다면, thread_id를 새로 생성한다. request_dict의 Input.Type값이 chat이라면, input.value값을 user_question변수에 넣는다
이 정보들로 intentAnalysisState 객체를 생성하고, 그래프 빌드 및 인보크하여, 결과를 json파일로 저장한다. 나머지는 이 저장된 파일을 해석 후 결정하겠다.

요구하신 흐름은 일반적으로 맞습니다. 요청을 받아 초기 IntentAnalysisState를 구성 → 그래프 build/compile → invoke → 결과를 JSON으로 저장 → 상위 레이어는 저장 파일을 해석해 응답 구성. 단, 요청마다 build/compile은 비용이 크므로 트래픽이 많다면 캐시/싱글턴으로 최적화하는 게 보통이지만, 지금은 “요청마다 build/compile” 조건대로 예시를 드립니다.

예시 advisor(request_dict)
- 규칙
  - threadid가 있으면 사용, 없으면 새로 생성
  - Input.Type == "chat"이면 Input.value를 user_question에 매핑
  - IntentAnalysisState는 여러분 프로젝트의 TypedDict 정의를 사용
  - 그래프는 요청마다 build/compile
  - 결과는 JSON 파일로 저장
  - 반환형: (True, result_dict, None, None) 또는 실패 시 (False, None, err_msg, None)

코드 예시 (필요한 import/경로는 프로젝트 구조에 맞게 수정)
```python
import os
import json
import uuid
from datetime import datetime
from typing import Any, Dict, Tuple, Optional

from langchain_core.runnables import RunnableConfig

# 여러분 프로젝트의 타입/빌더 import로 교체하세요.
# from your_project.langgraph.types import IntentAnalysisState
# from your_project.langgraph.builder import build_planner

# 예: settings 기반 경로 사용을 원하면 주석 해제
# from django.conf import settings

def _ensure_dir(path: str) -> None:
    if not os.path.exists(path):
        os.makedirs(path, exist_ok=True)

def _to_jsonable(obj: Any) -> Any:
    # LangGraph state에 비직렬화 객체가 섞일 수 있어 방어적으로 처리
    try:
        json.dumps(obj)
        return obj
    except TypeError:
        if isinstance(obj, dict):
            return {k: _to_jsonable(v) for k, v in obj.items()}
        if isinstance(obj, (list, tuple)):
            return [_to_jsonable(v) for v in obj]
        if hasattr(obj, "model_dump"):
            return obj.model_dump()  # pydantic v2
        if hasattr(obj, "dict"):
            return obj.dict()  # pydantic v1
        if hasattr(obj, "__dict__"):
            return {k: _to_jsonable(v) for k, v in obj.__dict__.items()}
        return str(obj)

def advisor(request_dict: Dict[str, Any]) -> Tuple[bool, Optional[Dict[str, Any]], Optional[str], Optional[str]]:
    """
    요청을 분해해 IntentAnalysisState 초기화 → LangGraph build/compile → invoke → 결과 JSON 저장.
    성공 시 (True, result_dict, None, None) 반환.
    실패 시 (False, None, error_message, None) 반환.
    """
    try:
        # 1) thread_id 결정
        thread_id = request_dict.get("threadid")
        if not thread_id:
            thread_id = str(uuid.uuid4())

        # 2) user_question 추출 (Input.Type == "chat"일 때만)
        user_question = ""
        input_obj = request_dict.get("Input") or request_dict.get("input") or {}
        if isinstance(input_obj, dict) and str(input_obj.get("Type") or input_obj.get("type")).lower() == "chat":
            user_question = input_obj.get("value") or ""

        # 3) persona_state 등 나머지 상태 구성
        #    - 프로젝트의 TypedDict 필수 필드에 맞게 기본값/전달값을 채워주세요.
        #    - 예: persona_state는 요청에 있으면 사용, 없으면 기본값.
        persona_state = request_dict.get("persona_state") or {
            "curplanid": request_dict.get("curplanid", None)
        }

        # 추가로 필요한 상태 필드가 있다면 여기서 채웁니다.
        # 예시 placeholder:
        # other_state = request_dict.get("other_state", {})

        # 초기 IntentAnalysisState 구성 (여러분 프로젝트의 TypedDict 구조를 따르세요)
        initial_state: Dict[str, Any] = {
            "user_question": user_question,
            "persona_state": persona_state,
            # "other_state": other_state,
            # 필요 필드 더 추가
        }

        # 4) 그래프 build/compile (요청마다)
        planner_builder = build_planner()
        planner_graph = planner_builder.compile()

        # 5) config 구성 및 invoke
        config = RunnableConfig(
            recursion_limit=10,
            configurable={"thread_id": thread_id}
        )
        graph_result = planner_graph.invoke(initial_state, config=config)

        # 6) JSON 파일로 저장
        # output_base = getattr(settings, "LANGGRAPH_OUTPUT_DIR", os.path.join(settings.BASE_DIR, "langgraph_results"))
        output_base = os.path.join(os.getcwd(), "langgraph_results")  # 필요 시 settings로 교체
        _ensure_dir(output_base)

        timestamp = datetime.utcnow().strftime("%Y%m%dT%H%M%S%fZ")
        filename = f"{timestamp}_{thread_id}.json"
        output_path = os.path.join(output_base, filename)

        payload = {
            "thread_id": thread_id,
            "initial_state": _to_jsonable(initial_state),
            "graph_result": _to_jsonable(graph_result),
            "meta": {
                "recursion_limit": 10,
                "saved_at": timestamp,
            },
        }

        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)

        # 7) 상위에서 파일을 해석한다고 하셨으므로 경로/메타만 리턴
        result = {
            "thread_id": thread_id,
            "result_file": output_path,
        }
        return True, result, None, None

    except Exception as e:
        # 실패 시 에러 메시지 전달
        return False, None, f"advisor failed: {e}", None
```

설명 요약
- 위 함수는 요청을 해석해 IntentAnalysisState를 만들고, 빌더를 매 요청마다 build/compile 후 invoke합니다.
- threadid는 요청의 threadid가 있으면 재사용, 없으면 새로 생성합니다.
- Input.Type이 chat이면 Input.value를 user_question에 매핑합니다.
- invoke 결과와 초기 상태를 함께 JSON 파일로 저장합니다. 상위에서는 해당 파일을 읽어 해석하시면 됩니다.
- TypedDict 필수 필드는 프로젝트 정의에 맞게 initial_state 부분을 보완하세요.


아래는 result(dict[str, Any])에서 “두 번째 레벨에 있는 'test'”를 찾아 그 하위의 'E3' 값을 변수에 담아 출력하는 예시입니다.

예시 1) 첫 번째로 발견된 'test'의 'E3'만 가져오기
```python
# result = planner_graph.invoke(...)
# result는 dict[str, Any]

test_dict = None
e3_value = None

for _, second_level in result.items():
    if isinstance(second_level, dict) and 'test' in second_level:
        test_dict = second_level['test']
        if isinstance(test_dict, dict) and 'E3' in test_dict:
            e3_value = test_dict['E3']
        break  # 첫 매치만 사용하려면 break

print(e3_value)
```

예시 2) 모든 두 번째 레벨의 'test'에서 'E3'를 모아서 리스트로 가져오기
```python
# result = planner_graph.invoke(...)

e3_values = [
    lvl2['test']['E3']
    for lvl2 in result.values()
    if isinstance(lvl2, dict)
    and isinstance(lvl2.get('test'), dict)
    and 'E3' in lvl2['test']
]

print(e3_values)  # 여러 개면 리스트로 출력
```

필요 시 KeyError를 피하기 위해 isinstance와 in 체크를 유지하면 안전하게 접근할 수 있습니다.



def to_table_html(data):
    """
    주어진 리스트 형태의 데이터를 HTML 테이블 코드로 변환하는 함수.
    
    Args:
    - data (list of dict): 테이블 행 데이터를 포함하는 딕셔너리들의 리스트.
    
    Returns:
    - str: HTML 형식의 테이블 코드를 반환.
    """
    
    if not data:
        return "<table></table>"

    # 리스트의 첫 번째 딕셔너리에서 키를 얻어 테이블 헤더 생성
    headers = data[0].keys()
    header_html = "<tr>" + "".join(f"<th>{key}</th>" for key in headers) + "</tr>"

    # 각 딕셔너리에서 값을 추출하여 테이블의 데이터 행 생성
    rows_html = ""
    for row in data:
        row_html = "<tr>" + "".join(f"<td>{row[col]}</td>" for col in headers) + "</tr>"
        rows_html += row_html

    # 최종 HTML 테이블 코드 조립
    table_html = f"<table border='1'>{header_html}{rows_html}</table>"
    
    return table_html

# 예시 데이터
data = [
    {"id": "myuser1", "name": "choi"},
    {"id": "myuser2", "name": "ch"},
    # ... 추가 데이터
]

# 함수 실행 및 결과 출력
html_snippet = to_table_html(data)
print(html_snippet)
